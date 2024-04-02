
import time
import json
from datetime import datetime
from suntime import Sun
from dateutil import tz
import pytz
from gpiozero import TimeOfDay, OutputDevice
import paho.mqtt.client as mqtt
from typing_extensions import Literal
from queue import Queue
import glob
import threading
import schedule
import logging


#################################################################################################
### setup logging
#################################################################################################

logging.basicConfig(
    level=logging.DEBUG,
#    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

##################################################################################################
### create a seperate thread to read the DS18B20 temp sensors
##################################################################################################

class DS18B20(threading.Thread):

    default_base_dir = "/sys/bus/w1/devices/"

    def __init__(self, base_dir=None):
        super().__init__()
        self._base_dir = base_dir if base_dir else self.default_base_dir
        self.daemon = True
        self.discover()

    def discover(self):
        device_folder = glob.glob(self._base_dir + "28*")
        self._num_devices = len(device_folder)
        self._device_file: list[str] = []
        for i in range(self._num_devices):
            self._device_file.append(device_folder[i] + "/w1_slave")

        self._values: list[float | None] = [None] * self._num_devices
        self._times: list[float] = [0.0] * self._num_devices

    def run(self):
        """Thread entrypoint: read sensors in a loop.

        Calling DS18B20.start() will cause this method to run in
        a separate thread.
        """

        while True:
            for dev in range(self._num_devices):
                self._read_temp(dev)

            # Adjust this value as you see fit, noting that you will never
            # read actual sensor values more often than 750ms * self._num_devices.
            time.sleep(1)

    def _read_temp(self, index):
        for i in range(3):
            with open(self._device_file[index], "r") as f:
                data = f.read()

            if "YES" not in data:
                time.sleep(0.1)
                continue

            disacard, sep, reading = data.partition(" t=")
            temp = float(reading) / 1000.0
            self._values[index] = temp
            self._times[index] = time.time()
            break
        else:
            logging.debug(f"failed to read device {index}")

    def tempC(self, index=0):
        return self._values[index]

    def device_count(self):
        """Return the number of discovered devices"""
        return self._num_devices


d = DS18B20()
d.start()

#####################################################################################
###  Create a schedule to calculate sundown / sunrise once a day
#####################################################################################


def calcsun():
    latitude = 45.08608
    longitude = -79.552073
    tz_muskoka = pytz.timezone('America/Toronto')
    sun = Sun(latitude, longitude)
    global today_sr
    today_sr = sun.get_sunrise_time().astimezone(tz_muskoka).strftime("%H:%M")
    global today_ss
    today_ss = sun.get_sunset_time().astimezone(tz_muskoka).strftime("%H:%M")
    logging.debug("Running calcsun")
    logging.debug("new sunrise time: %s", today_sr)
    logging.debug("new sunset time: %s", today_ss)


### schedule to calcualte new sunrise/sunset every night
schedule.every().day.at("00:30").do(calcsun)


#####################################################################################
###  Initialization Routines
#####################################################################################

# initialize GPIO pins
bub1Pin = 5
bub2Pin = 6
dangerPin = 26

# initialize variables
state = 0         # statemachine, 0=Off, 1=idle, 2=nightly, 3=constant
logging.debug("state initiated = 0")
master = 0        # variable for master power
auto_bubble = 0   # auto bubbler operation
dl_flag = 0       # flag for danger lights

# initialize queue for MQTT message arrival
q=Queue()

# initialize GPIO pins on pi
bubbler_1 = OutputDevice(bub1Pin, active_high=True, initial_value=False)
bubbler_2 = OutputDevice(bub2Pin, active_high=True, initial_value=False)
danger = OutputDevice(dangerPin, active_high=False, initial_value=False)

# initialize time values for fixed time actions
# need to fix time.time vs datetime.time conflict
#timevalue1 = TimeOfDay(time(15,58), time(15,59), utc=False)


#  run sunrise/sunset calcs at startup
calcsun()

# startup MQTT message subscriber
def on_message(client, userdata, message):
    q.put(message)


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,"queenMos")
client.username_pw_set("ha-user", "ha-pass")
#broker_address="10.4.24.11"
broker_address="debian12vm.emerald-gopher.ts.net"
client.connect(broker_address)
client.subscribe([("cust1/cmd/bubbler_main",1),("cust1/cmd/statemachine",1),("cust1/cmd/auto_bubble",1),("cust1/cmd/bubbler_1",1),("cust1/cmd/bubbler_2",1),("cust1/cmd/danger_lights",1)])
client.on_message = on_message
client.loop_start()
logging.debug("MQTT subscribe loop started")

# initialize MQTT status variables with broker
client.publish("cust1/state/bubbler_main","OFF",1,True)
client.publish("cust1/state/statemachine","OFF",1,True)  #options: OFF, IDLE, NIGHTLY, CONSTANT
client.publish("cust1/state/auto_bubble","OFF",1,True)
client.publish("cust1/state/bubbler_1","OFF",1,True)
client.publish("cust1/state/bubbler_2","OFF",1,True)
client.publish("cust1/state/danger_lights","OFF",1,True)

################################################################################
###  Functions to turn bubblers and danger ligths on/off
###   - only allow 1 bubbler to run at a time
################################################################################

def bubbler_1_off():
    bubbler_1.off()
    client.publish("cust1/state/bubbler_1","OFF")

def bubbler_1_on():
    if bubbler_2.value == 0:
        bubbler_1.on()
        client.publish("cust1/state/bubbler_1","ON")

def bubbler_2_off():
    bubbler_2.off()
    client.publish("cust1/state/bubbler_2","OFF")

def bubbler_2_on():
    if bubbler_1.value == 0:
        bubbler_2.on()
        client.publish("cust1/state/bubbler_2","ON")

def danger_lights_off():
    danger.off()
    client.publish("cust1/state/danger_lights","OFF")

def danger_lights_on():
    danger.on()
    client.publish("cust1/state/danger_lights","ON")

######################################################################################
###  Routine to run in seperate thread to retrieve & publish temp values once a minute
######################################################################################

# wait for temp sensor read thread to startup
time.sleep(1)

def publish_temp():
    while True:
        global air_temp
        air_temp = d.tempC(0)
        box_temp = 21.2
        water_temp = 5.5
        send_temp = {
                'airtemp': air_temp,
                'watertemp': water_temp,
                'boxtemp': box_temp
        }
        client.publish("cust1/state/temperatures", payload=json.dumps(send_temp), qos=1)


#print out temp arry
#        for i in range(d.device_count()):
#            print(f'dev {i}: {d.tempC(i)}')

        time.sleep(60)

t = threading.Thread(target=publish_temp)
t.start()

#############################################################################################
###  Define class Alternator for state 3, CONSTANT bubble
#############################################################################################

class Alternator(threading.Thread):
    def __init__(self,timeout):
        self.delay_mins = timeout
#        self.functions = [cycle_1, cycle_2]
        threading.Thread.__init__(self)
        self.event = threading.Event()

    def run(self):
        while not self.event.is_set():
            bubbler_1_on()
            time.sleep(3)
            bubbler_2_off()
            time.sleep(self.delay_mins*60)
            bubbler_1_off()
            time.sleep(3)
            bubbler_2_on()
            time.sleep(self.delay_mins*60)

    def stop(self):
        self.event.set()

#################################################################################
### Main Loop
#################################################################################


while True:

    schedule.run_pending()


# check MQTT queue for new cmd messages and act upon them

    while not q.empty():
        msg = q.get()
        if msg is None:
            continue
        topic = str(msg.topic)
        payload = str(msg.payload.decode("utf-8"))

        if topic == "cust1/cmd/bubbler_main":
            if payload == "ON":
                client.publish("cust1/state/bubbler_main","ON", qos=1, retain=True)
                master = 1
            else:
                client.publish("cust1/state/bubbler_main","OFF", qos=1, retain=True)
                master = 0

        if topic == "cust1/cmd/auto_bubble":
            if payload == "ON":
                if master == 1:  ### only turn on auto_bubble if master power is on
                    client.publish("cust1/state/auto_bubble","ON", qos=1, retain=True)
                    auto_bubble = 1
            else:
                client.publish("cust1/state/auto_bubble","OFF", qos=1, retain=True)
                auto_bubble = 0

        if topic == "cust1/cmd/bubbler_1":
            if payload == "ON":
                if master == 1:
                    bubbler_1_on()
            else:
                bubbler_1_off()

        if topic == "cust1/cmd/bubbler_2":
            if payload == "ON":
                if master == 1:
                    bubbler_2_on()
            else:
                bubbler_2_off()

        if topic == "cust1/cmd/danger_lights":
            if payload == "ON":
                if master == 1:
                    danger_lights_on()
            else:
                danger_lights_off()


#    time.sleep(1)


################################################################################
###  Operate Danger Lights from Dusk to Dawn unless state = 0
################################################################################

    now = datetime.now()
    today_now = now.strftime("%H:%M")

    if today_now >= today_ss or today_now < today_sr:
        if dl_flag == 0:
            danger_lights_on()
            logging.debug("danger lights on at: %s", today_now)
            dl_flag = 1
    else:
        if dl_flag == 1:
            danger_lights_off()
            logging.debug("danger lights off at: %s", today_now)
            dl_flag = 0

################################################################################
### State: OFF  [state = 0]
################################################################################
    if state == 0:

### exit: bubbler_main turns on
        if master == 1:
            state = 1
            logging.debug("entering state 1 from state 0")
            client.publish("cust1/state/bubbler_main","ON", qos=1, retain=True)
            client.publish("cust1/state/statemachine","IDLE", qos=1, retain=True)


################################################################################
### State: IDLE  [state = 1]
################################################################################

    if state == 1:


### exit: bubbler_main turns off, go to state 0, OFF
        if master == 0:
            state = 0
            logging.debug("entering state 0 from state 1")
            client.publish("cust1/state/bubbler_main","OFF", qos=1, retain=True)
            client.publish("cust1/state/statemachine","OFF", qos=1, retain=True)
            auto_bubble = 0
            client.publish("cust1/state/auto_bubble","OFF", qos=1, retain=True)
            danger_lights_off()

### exit: auto_bubble on and air temp below <1 degree C go to state 2, NIGHLTY
        if auto_bubble == 1:
            if air_temp < 19:
                state = 2
                logging.debug("entering state 2 from state 1")
                client.publish("cust1/state/statemachine","NIGHTLY", qos=1, retain=True)
                schedule.every().day.at("03:00").do(bubbler_1_on).tag("nigtly")
                schedule.every().day.at("04:55").do(bubbler_1_off).tag("nightly")
                schedule.every().day.at("05:00").do(bubbler_2_on).tag("nightly")
                schedule.every().day.at("06:55").do(bubbler_2_off).tag("nightly")


#################################################################################
### State: NIGHTLY [state = 2]
#################################################################################
    if state == 2:


### exit: temp drops below 16.8, go to state 3, CONSTANT
        if air_temp < 14:  #18
            state = 3
            logging.debug("entering state 3 from state 2")
            client.publish("cust1/state/statemachine","CONSTANT", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")

            # start 60 mins alternating timer
            a = Alternator(15)
            a.start()

### exit: auto_bubble turned off, go to state 1, IDLE
        if auto_bubble == 0:
            state = 1
            logging.debug("entering state 1 from state 2")
            client.publish("cust1/state/statemachine","IDLE", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")

### exit: if temp goes above 1, go to state 1, IDLE
        if air_temp > 19.5:
            state = 1
            logging.debug("entering state 1 from state 2")
            client.publish("cust1/state/statemachine","IDLE", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")

### exit: bubbler_main turns off, go to state 0, OFF
        if master == 0:
            state = 0
            logging.debug("entering state 0 from state 2")
            client.publish("cust1/state/bubbler_main","OFF", qos=1, retain=True)
            client.publish("cust1/state/statemachine","OFF", qos=1, retain=True)
            auto_bubble = 0
            client.publish("cust1/state/auto_bubble","OFF", qos=1, retain=True)
            danger_lights_off()
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")

#################################################################################
### State: CONSTANT [state = 3]
#################################################################################
    if state == 3:


### exit: if temp >-6, go to state 2 NIGHTLY
        if air_temp > 18.5:
            state = 2
            logging.debug("entering state 2 from state 3")
            client.publish("cust1/state/statemachine","NIGHTLY", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            a.stop()

### exit: auto_bubble turned off, go to state 1, IDLE
        if auto_bubble == 0:
            state = 1
            logging.debug("entering state 1 from state 3")
            client.publish("cust1/state/statemachine","IDLE", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            a.stop()

### exit: bubbler_main turns off, go to state 0, OFF
        if master == 0:
            state = 0
            logging.debug("entering state 0 from state 3")
            client.publish("cust1/state/bubbler_main","OFF", qos=1, retain=True)
            client.publish("cust1/state/statemachine","OFF", qos=1, retain=True)
            auto_bubble = 0
            client.publish("cust1/state/auto_bubble","OFF", qos=1, retain=True)
            danger_lights_off()
            bubbler_1_off()
            bubbler_2_off()
            a.stop()


##################################################################################
##################################################################################
##################################################################################
