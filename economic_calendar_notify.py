from bs4 import BeautifulSoup
from urllib2 import urlopen
import re
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
import time as timee
import threading
from satori.rtm.client import make_client
import satori.rtm.auth as auth

logging.basicConfig()

scheduler = BackgroundScheduler()
scheduler.start()

url = 'http://ecal.forexpros.com/gen_eco_cal.php?max_rows=200&time=daily&timezone_ID=14&dst=off&print_date_col=Yes&print_actual_col=Yes&print_previous_col=Yes&print_forecast_col=Yes&force_lang=NONE&print_event_details=Yes&print_currency_col=Yes&print_date_col=Yes&print_actual_col=Yes&print_previous_col=Yes&print_forecast_col=Yes'
html = urlopen(url)

soup = BeautifulSoup(html,"html.parser")
ts = timee.time()
epoch = datetime.utcfromtimestamp(0)
timepatt = re.compile("\d{2}:\d{2}")
scheduled={}
timers={}
sendQueue=[]

channel = "economic-calendar"
role = "economic-calendar"
secret = "b90eAda916b64852aD8aca0A5a9986ED"
endpoint = "wss://open-data.api.satori.com"
appkey = "79e43f3774eeDC7ad31bA504A0230dFa"

def unix_time(dt):
    return (dt - epoch).total_seconds()
def send(event):
    sendQueue.append(event)

def schedule(event,key):
    if(timepatt.match(event['time'])==None):
        return None
    stime = datetime.strptime(event['date']+' 2017 '+event['time'], '%b. %d %Y %H:%M')
    stime=datetime.fromtimestamp(unix_time(stime))
    print(stime)
    timers[key]=scheduler.add_job(send, 'date', run_date=stime, misfire_grace_time=5, args=[event])
    
rows = soup.find_all('tr', {'class': re.compile('ec_bg[12]_tr')})
date = rows[0].find('td',{'class':'ec_td_date'}).text

def scrape():
    print('scrape')
    for tr in rows:
        time = tr.find('td', {'class': 'ec_td_time'}).text
        name = tr.find('td', {'class': 'ec_td_event'}).text.replace(u'\xa0', '')
        currency = tr.find('td', {'class': 'ec_td_currency'}).text
        actual = tr.find('td', {'class': 'ec_td_actual'}).text.replace(u'\xa0', '')
        forecast = tr.find('td', {'class': 'ec_td_forecast'}).text.replace(u'\xa0', '')
        previous = tr.find('td', {'class': 'ec_td_previous'}).text.replace(u'\xa0', '')
        event={'date':date,'time':time,'event_name':name,'currency':currency,'actual':actual,'forecast':forecast,'previous':previous}
        
        key = currency+name
        
        if(key in scheduled):
            if(event!=scheduled[key]):
                # Existing event that changed
                timers[key].remove()
                scheduled[key]=event
                schedule(event,key)
        else:
            # New event, schedule it
            scheduled[key]=event
            schedule(event,key)
scrape()
scheduler.add_job(scrape, 'interval', minutes=1, misfire_grace_time=5 )

with make_client(endpoint=endpoint, appkey=appkey) as client:
        auth_finished_event = threading.Event()
        auth_delegate = auth.RoleSecretAuthDelegate(role, secret)
        def auth_callback(auth_result):
            if type(auth_result) == auth.Done:
                print('Auth success')
                auth_finished_event.set()
            else:
                print('Auth failure: {0}'.format(auth_result))
                sys.exit(1)

        client.authenticate(auth_delegate, auth_callback)

        def publish_callback(ack):
            print('Publish ack:', ack)
            pass

        while True:
            for i in sendQueue:
                client.publish(
                    channel, message=i, callback=publish_callback)
                timee.sleep(0.001)
            sendQueue = []

scheduler.print_jobs()
#print json.dumps(data, indent=4, sort_keys=True)

