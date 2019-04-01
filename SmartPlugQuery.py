#!/home/zeroc/M9Venv/venv/bin/python3
import time
import requests
import uuid
import json
from google.cloud import pubsub_v1
import os
import datetime

#Authentication on GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/zeroc/M9-Assignment-c923be01a063.json"
#Authentication for Kasa / Tp-Link Cloud
USERNAME = 'am182@hdm-stuttgart.de'
PASSWORD = 'BwEiLOp6XWemHNh0TEWV'
#PubSub Project/Topic
project_id = "m9-assignment"
topic_name = "prdsmartsocketdata1"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

 #Get Data from Kasa Cloud where Smart Sockets are registered
def get_device_data(USERNAME, PASSWORD):
    # First step is to get a token by authenticating with your username (email) and password
    payload = {
        "method": "login",
        "params":
            {
                "appType": "Kasa_Android",
                "cloudUserName": USERNAME,
                "cloudPassword": PASSWORD,
                "terminalUUID": str(uuid.uuid4())
            }
    }
    response = requests.post("https://wap.tplinkcloud.com/", json=payload)
    obj = json.loads(response.content)
    token = obj["result"]["token"]

    # Find the device we want to change
    payload = {"method": "getDeviceList"}
    response = requests.post("https://wap.tplinkcloud.com?token={0}".format(token), json=payload)


    # The JSON returned contains a list of devices. You could filter by name etc, but here we'll just use the first
    obj = json.loads(response.content)
    devices = obj["result"]["deviceList"]
    usageDataList=list()
    for device in devices:
        #device = obj["result"]["deviceList"][1]
        #print(obj["result"]["deviceList"])
        # The device object contains a 'regional' address for control commands
        app_server_url = device["appServerUrl"]
        # Also grab the device ID
        device_id = device["deviceId"]


        device_command = {"emeter":{"get_realtime":{}}}

        # ...which is escaped and passed within the JSON payload which we post to the API
        payload = {
            "method": "passthrough",
            "params": {
                "deviceId": device_id,
                "requestData": json.dumps(device_command)  # Request data needs to be escaped, it's a string!
            }
        }
        # use the app server URL, not the root one we authenticated with
        response = requests.post("{0}?token={1}".format(app_server_url, token), json=payload)
        #returnstring="Device: " + str(device) + " ; Data:"+ str(response.content)
        SmartDict=dict(device)

        # get only emeter resultsdata and bring it to google understandable Json format
        result=json.loads(response.content)
        try:
            realdata=result["result"]["responseData"]
            jsondata = json.loads(realdata)
            jsondata = jsondata["emeter"]["get_realtime"]
        except:
            print("deviceOffline")
            jsondata={}
       
        #adjust data of Socket with older HW/FW Version
        if 'power' in jsondata.keys():
            jsondata['power_mw'] = jsondata.pop("power")
            jsondata['current_ma'] = jsondata.pop("current")
            jsondata['voltage_mv'] = jsondata.pop("voltage")
            jsondata['total_wh'] = jsondata.pop("total")
            jsondata['power_mw'] = int(jsondata['power_mw']*1000)
            jsondata['current_ma'] = int(jsondata['current_ma']*1000)
            jsondata['voltage_mv'] = int(jsondata['voltage_mv']*1000)
            jsondata['total_wh'] = int(jsondata['total_wh']*1000)

                
        SmartDict.update(jsondata)
        


        #add timestamp
        ts=time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        SmartDict["timestamp"]= str(st)
        
        usageDataList.append(SmartDict)

        iter1 = range(1, 1000)
        for i in iter1:
            sampledata = dict(SmartDict)
            sampledata["deviceId"]=sampledata["alias"]+""+str(i)
            usageDataList.append(sampledata)       

    #print(device)
    #print(response.content)
    return usageDataList

while True:  
        smartData=get_device_data(USERNAME, PASSWORD)

        for datapoint in smartData:
            data=str(datapoint).replace("\\", "")
            data=str(data).replace("\'", "\"")
            data=str(data).replace("True","true")
            data=str(data).replace("False","false")
            data=str(data).replace(":{",": {")
            data=str(data).replace("Null","null")
            #data=str(data).replace("current","current_ma")
            #data=str(data).replace("voltage","voltage_mv")
            #data=str(data).replace("power ","power_mw")
            #data=str(data).replace("total","total_wh")

            data = str.encode(str(data),"utf-8")#.encode('utf-8')
            future = publisher.publish(topic_path, data=data)
                #print('Published {} of message ID {}.'.format(data, future.result()))
        time.sleep(10)
