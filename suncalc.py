import time
from datetime import datetime
from datetime import date
import pytz
from suntime import Sun
import schedule


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
    ct = now.strftime("%H:%M")
    cd = date.today()
    print("Running calcsun at ", cd, ct)
    print("new sunrise time:", today_sr)
    print("new sunset time:", today_ss)


### schedule to calcualte new sunrise/sunset every night
schedule.every().hour.do(calcsun)

calcsun()

while True:
    schedule.run_pending()
    time.sleep(1)
