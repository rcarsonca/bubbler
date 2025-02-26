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



#####################################################################################
###  Initialization Routines
#####################################################################################

# text substitutions
cust = "cust1"
#cust = "heller"
#cust = "miller"

# temps (deg C) for state transistions
temp_to_nightly = 0
temp_from_nightly = 1
temp_to_constant = -8
temp_from_constant = -6

# initialize GPIO pins
bub1Pin = 5
bub2Pin = 6
bub3Pin = 22
dangerPin = 26

# initialize variables
#logging.debug("state initiated = 0")
state = 0
dl_flag = 0             # flag for danger lights
state2_first_run = 0    # flag for code to run first time in state 2

# initialize queue for MQTT message arrival
q=Queue()

# initialize queue for sharing air temp variable across threads
tempq=Queue()


# initialize GPIO pins on pi
bubbler_1 = OutputDevice(bub1Pin, active_high=True, initial_value=False)
bubbler_2 = OutputDevice(bub2Pin, active_high=True, initial_value=False)
bubbler_3 = OutputDevice(bub3Pin, active_high=True, initial_value=False)
danger = OutputDevice(dangerPin, active_high=True, initial_value=False)


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
###
### from https://stackoverflow.com/questions/72771186/read-multiple-ds18b20-temperature-sensors-faster-using-raspberry-pi
###
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
#        print(self._num_devices)
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
            time.sleep(2)

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
        try:
            return self._values[index]
        except:
            logging.debug("check temp sensor connections")

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
    now = datetime.now()
    global today_sr
    today_sr = sun.get_sunrise_time(now).astimezone(tz_muskoka).strftime("%H:%M")
    global today_ss
    today_ss = sun.get_sunset_time(now).astimezone(tz_muskoka).strftime("%H:%M")
    logging.debug("Running calcsun")
    logging.debug("new sunrise time: %s", today_sr)
    logging.debug("new sunset time: %s", today_ss)


### schedule to calcualte new sunrise/sunset every night
schedule.every().day.at("00:30").do(calcsun)


######################################################################################
#  run sunrise/sunset calcs at startup
######################################################################################

calcsun()

######################################################################################
###  startup MQTT message subscriber
######################################################################################

def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe([(f"{cust}/cmd/bubbler_main",1),(f"{cust}/cmd/statemachine",1),(f"{cust}/cmd/auto_bubble",1),(f"{cust}/cmd/bubbler_1",1),(f"{cust}/cmd/bubbler_2",1),(f"{cust}/cmd/danger_lights",1)])


def on_message(client, userdata, message):
    q.put(message)

#def on_log(client, userdata, paho_log_level, messages):
#    print("paho log: ",message)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,"cust1")
client.username_pw_set("ha-user", "ha-pass")
broker_address="debianvm-nuc.emerald-gopher.ts.net"
client.connect_async(broker_address)   #asyn connection in case internet not avail.
client.on_connect = on_connect
client.on_message = on_message
client.enable_logger # enable logging
#client.on_log = on_log
client.loop_start()


#################################################################################
###  load values of power on/off and autobubble  from persistent savedata.json file
#################################################################################

with open('/home/randy/bubbler/savedata.json', 'r') as f:
    data = json.load(f)
    master = int(data["mainkey"])
    auto_bubble = int(data["autokey"])

logging.debug("loading from initial load of savedata file")
logging.debug("main power = %s", master)
logging.debug("auto_bubble = %s", auto_bubble)


################################################################################
###  Function to update savedata.json file every time value changes
################################################################################

def savedata():

    logging.debug("running savedata function:")

    jsonData = {"mainkey": master, "statekey": state, "autokey": auto_bubble, "b1key": bubbler_1.value, "b2key": bubbler_2.value, "b3key": bubbler_3.value, "dangerkey": danger.value}

    with open('/home/randy/bubbler/savedata.json', 'w') as f:
        json.dump(jsonData, f)


################################################################################
###  Functions to turn bubblers and danger lights on/off
###   - only allow 1 bubbler to run at a time
################################################################################

def bubbler_1_off():
    bubbler_1.off()
    client.publish(f"{cust}/state/bubbler_1","OFF",1,True)
    savedata()

def bubbler_1_on():
    if bubbler_2.value == 0:
        bubbler_1.on()
        client.publish(f"{cust}/state/bubbler_1","ON",1,True)
        savedata()

def bubbler_2_off():
    bubbler_2.off()
    client.publish(f"{cust}/state/bubbler_2","OFF",1,True)
    savedata()

def bubbler_2_on():
    if bubbler_1.value == 0:
        bubbler_2.on()
        client.publish(f"{cust}/state/bubbler_2","ON",1,True)
        savedata()

def danger_lights_off():
    danger.off()
    client.publish(f"{cust}/state/danger_lights","OFF",1,True)
    savedata()

def danger_lights_on():
    danger.on()
    client.publish(f"{cust}/state/danger_lights","ON",1,True)
    savedata()
######################################################################################
###  Routine to run in seperate thread to retrieve & publish temp values every 5 sec
######################################################################################

# first run, wait for temp sensor read thread to startup
time.sleep(5)

def publish_temp():#
#    global air_temp
    while True:
        air_temp = d.tempC(2)
        tempq.put(air_temp)
#        logging.debug("putting air_temp on queue: %s", air_temp)
        box_temp = d.tempC(0)
        water_temp = d.tempC(1)
        send_temp = {
                'airtemp': air_temp,
                'watertemp': water_temp,
                'boxtemp': box_temp
        }
#        logging.debug(" *** publishing temperature data via MQTT ***")
        client.publish(f"{cust}/state/temperatures", payload=json.dumps(send_temp),qos=1,retain=True)
# publish availability hearbeat
        client.publish(f"{cust}/state/availability", "online",qos=1,retain=False)

        time.sleep(10)

t = threading.Thread(target=publish_temp)
t.start()

#############################################################################################
###  Define class Alternator for state 3, CONSTANT bubble
#############################################################################################

class Alternator(threading.Thread):
    def __init__(self,timeout):
        super().__init__()  # Initializes the thread properly
        self.delay_mins = timeout
        threading.Thread.__init__(self)
        self.event = threading.Event()

    def run(self):
        while not self.event.is_set():
            bubbler_2_off()
            self._safe_sleep(3)

            bubbler_1_on()
            logging.debug("alternator B1 ON B2 OFF")
            self._safe_sleep(self.delay_mins*60)

            bubbler_1_off()
            self._safe_sleep(3)

            bubbler_2_on()
            logging.debug("alternator B1 OFF B2 ON")
            self._safe_sleep(self.delay_mins*60)

    def _safe_sleep(self, duration):
        """Sleep in small intervals to allow early exit when stop() is called."""
        interval = 0.5  # check every 0.5 seconds
        elapsed = 0
        while elapsed < duration:
            if self.event.is_set():
                return # exit early if stop() is called
            time.sleep(min(interval, duration - elapsed))
            elapsed += interval

    def stop(self):
        """Signal the thread to stop and wait for it to finish"""
        self.event.set()
        self.join(timeout=5)  # Wait up to 5 seconds for the thread to stop
        if self.is_alive():
            logging.warning("Alternator thread did not stop in time!")

#################################################################################
### Main Loop
#################################################################################


while True:

    schedule.run_pending()

#short sleep to avoid high CPU
    time.sleep(0.1)

#check temp queue for updates
    while not tempq.empty():
        air_temp_loop = tempq.get()
#        logging.debug("getting air_temp_loop from queue: %s",air_temp_loop)

# check MQTT queue for new cmd messages and act upon them

    while not q.empty():
        logging.debug("MQTT message queue not empty")
        msg = q.get()
        if msg is None:
            continue
        topic = str(msg.topic)
        payload = str(msg.payload.decode("utf-8"))
        logging.debug("new MQTT message decoded")
        logging.debug(topic)
        logging.debug(payload)
        if topic == f"{cust}/cmd/bubbler_main":
            if payload == "ON":
                client.publish(f"{cust}/state/bubbler_main","ON", qos=1, retain=True)
                master = 1
            else:
                client.publish(f"{cust}/state/bubbler_main","OFF",1,True)
                master = 0
                bubbler_1_off()
                bubbler_2_off()
                danger_lights_off()

        if topic == f"{cust}/cmd/auto_bubble":
            if payload == "ON":
                if master == 1:  ### only turn on auto_bubble if master power is on
                    client.publish(f"{cust}/state/auto_bubble","ON", qos=1, retain=True)
                    auto_bubble = 1
                    bubbler_1_off()
                    bubbler_2_off()
                    danger_lights_off()
            else:
                client.publish(f"{cust}/state/auto_bubble","OFF", qos=1, retain=True)
                auto_bubble = 0

        if topic == f"{cust}/cmd/bubbler_1":
#            if auto_bubble == 0:  ### only turn on/off if auto bubble not enabled
                if payload == "ON":
                    if master == 1:
                        bubbler_1_on()
                else:
                    bubbler_1_off()

        if topic == f"{cust}/cmd/bubbler_2":
#            if auto_bubble == 0:  ### only turn on/off if auto bubble not enabled
                if payload == "ON":
                    if master == 1:
                        bubbler_2_on()
                else:
                    bubbler_2_off()

        if topic == f"{cust}/cmd/danger_lights":
#            if auto_bubble == 0:  ### only turn on/off if auto bubble not enabled
                if payload == "ON":
                    if master == 1:
                        danger_lights_on()
                else:
                    danger_lights_off()


        savedata()


################################################################################
###  Operate Danger Lights from Dusk to Dawn unless state = 0
################################################################################

    now = datetime.now()
    today_now = now.strftime("%H:%M")

    if today_now >= today_ss or today_now < today_sr:
        if dl_flag == 0:
            if master == 1:
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
#        logging.debug("in state 0 off, airtemp: %s", air_temp_loop)
### exit: bubbler_main turns on
        if master == 1:
            state = 1
            logging.debug("entering state 1 from state 0")
            client.publish(f"{cust}/state/bubbler_main","ON", qos=1, retain=True)
            client.publish(f"{cust}/state/statemachine","Idle", qos=1, retain=True)

################################################################################
### State: IDLE  [state = 1]
################################################################################

    if state == 1:
#        logging.debug("in state 1 idle, airtemp: %s", air_temp_loop)

### exit: bubbler_main turns off, go to state 0, OFF
        if master == 0:
            state = 0
            logging.debug("entering state 0 from state 1")
            client.publish(f"{cust}/state/bubbler_main","OFF", qos=1, retain=True)
            client.publish(f"{cust}/state/statemachine","Off", qos=1, retain=True)
            auto_bubble = 0
            client.publish(f"{cust}/state/auto_bubble","OFF", qos=1, retain=True)

### exit: auto_bubble on and air temp below <0 degree C go to state 2, NIGHLTY
        if auto_bubble == 1:
            if air_temp_loop < temp_to_nightly:
                state = 2
                state2_first_run = 0
                logging.debug("entering state 2 from state 1")

#################################################################################
### State: NIGHTLY [state = 2]
#################################################################################
    if state == 2:

### run this the first time entering nightly state
        if state2_first_run == 0:
            client.publish(f"{cust}/state/statemachine","Nightly", qos=1, retain=True)
            schedule.every().day.at("03:00").do(bubbler_1_on).tag("nightly")
            schedule.every().day.at("04:55").do(bubbler_1_off).tag("nightly")
            schedule.every().day.at("05:00").do(bubbler_2_on).tag("nightly")
            schedule.every().day.at("06:55").do(bubbler_2_off).tag("nightly")
            logging.debug("setting nightly schedule, 3am & 5am runs")
            state2_first_run = 1

### exit: temp drops below -8, go to state 3, CONSTANT
        if air_temp_loop < temp_to_constant:
            state = 3
            logging.debug("entering state 3 from state 2")
            client.publish(f"{cust}/state/statemachine","Constant", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")
            logging.debug("clearing nightly schedule")

            # start () mins alternating timer
            a = Alternator(60)
            a.start()

### exit: auto_bubble turned off, go to state 1, IDLE
        if auto_bubble == 0:
            state = 1
            logging.debug("entering state 1 from state 2")
            client.publish(f"{cust}/state/statemachine","Idle", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")
            logging.debug("clearing nightly schedule")

### exit: if temp goes above 1, go to state 1, IDLE
        if air_temp_loop > temp_from_nighly:
            state = 1
            logging.debug("entering state 1 from state 2")
            client.publish(f"{cust}/state/statemachine","Idle", qos=1, retain=True)
            bubbler_1_off()
            bubbler_2_off()
            schedule.clear("nightly")
            logging.debug("clearing nightly schedule")

### exit: bubbler_main turns off, go to state 0, OFF
        if master == 0:
            state = 0
            logging.debug("entering state 0 from state 2")
            client.publish(f"{cust}/state/bubbler_main","OFF", qos=1, retain=True)
            client.publish(f"{cust}/state/statemachine","Off", qos=1, retain=True)
            auto_bubble = 0
            client.publish(f"{cust}/state/auto_bubble","OFF", qos=1, retain=True)
            schedule.clear("nightly")
            logging.debug("clearing nightly schedule")

#################################################################################
### State: CONSTANT [state = 3]
#################################################################################
    if state == 3:


### exit: if temp >-6, go to state 2 NIGHTLY
        if air_temp_loop > temp_from_constant:
            state = 2
            state2_first_run = 0
            logging.debug("entering state 2 from state 3")
            client.publish(f"{cust}/state/statemachine","Nightly", qos=1, retain=True)
            a.stop()
            bubbler_1_off()
            bubbler_2_off()

### exit: auto_bubble turned off, go to state 1, IDLE
        if auto_bubble == 0:
            state = 1
            logging.debug("entering state 1 from state 3")
            client.publish(f"{cust}/state/statemachine","Idle", qos=1, retain=True)
            a.stop()
            bubbler_1_off()
            bubbler_2_off()

### exit: bubbler_main turns off, go to state 0, OFF
        if master == 0:
            state = 0
            logging.debug("entering state 0 from state 3")
            client.publish(f"{cust}/state/bubbler_main","OFF", qos=1, retain=True)
            client.publish(f"{cust}/state/statemachine","Off", qos=1, retain=True)
            auto_bubble = 0
            client.publish(f"{cust}/state/auto_bubble","OFF", qos=1, retain=True)
            a.stop
            bubbler_1_off()
            bubbler_2_off()

##################################################################################
##################################################################################
##################################################################################
