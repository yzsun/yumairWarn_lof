# -*- coding: utf-8 -*-
'''
create time: 2016-12-23 16:00:00
'''
from __future__ import division
import numpy as np
import pandas as pd
import pymysql
import requests
import json
from datetime import datetime
import time
from lof import CompreWarn

def get_Grid(gridCon):
    gridDf = pd.read_sql('select distinct id,\
                           left_down_latitude, left_down_longitude, \
                           right_up_latitude,right_down_longitude \
                           from grid where delete_flag=0',gridCon,index_col='id')
    return gridDf

def get_device(gridDf,gridIndex):
    '''Input: gridIndex，即网格id 如 “1500”
       return: deviceCode2Name{'deviceCode':['YA00100000000001',...,'YA00100000000165']
                               'deviceName':['1#微站',...,'n#微站']} '''
    deviceUrl = 'http://192.168.0.166:5558/services/device/getDeviceCount'
    paras = {"queryType":'YUMAIR',
             "longitudeMin":gridDf['left_down_longitude'][gridIndex],
             "longitudeMax":gridDf['right_down_longitude'][gridIndex],
             "latitudeMin":gridDf['left_down_latitude'][gridIndex],
             "latitudeMax":gridDf['right_up_latitude'][gridIndex]}
    try:
        tmp = requests.get(deviceUrl,params=paras)
        tmp = json.loads(tmp.text)
        deviceCode2Name = {};deviceCode2Site = {};deviceCode2ID = {}
        for device in tmp:
            if device['openStatus']==0:
                deviceCode2Name[device['deviceCode']]= device['deviceName']
                deviceCode2Site[device['deviceCode']]= [device['longitude'],device['latitude']]
                deviceCode2ID[device['deviceCode']]= device['id']
        return [deviceCode2Name,deviceCode2Name,deviceCode2ID]
    except ValueError:
        return False

    
def get_data(deviceList,fetchTime): 
    dataUrl = "http://192.168.0.177:10886/v1.1/yumair/statistic/raw"
    data = json.dumps({"dids":','.join(deviceList),
         "dvids":",".join(['6','7','8','9','10','11','12']),#,pm2_5,pm10,co,no2,so2,o3,aqi',
         "period":'day',
         "startTime":fetchTime,
         "endTime":fetchTime})
    headers={"APIKey":"42d6c32f992541eea8b4e835cb9b6b77"}
    dataTmp = requests.post(dataUrl,data,headers=headers)
    dataTmp = json.loads(dataTmp.text)
    data=[]
    for i in range(len(dataTmp['datas'])):
        data.append(float(dataTmp['datas'][i]['data'][datetime.strptime(fetchTime,"%Y-%m-%dT%H:%M:%S").hour][fetchTime]))
    data = np.reshape(data,(-1,7))
    return data


def mainWarn(gridCon,fetchTime,yellow=2.5,red=4,k=2):
    gridDf = get_Grid(gridCon)
    for gridIndex in gridDf.index:
        print '#### gridIndex ####',gridIndex
        deviceInfo = get_device(gridDf,gridIndex)
        if not deviceInfo:
            continue
        deviceCode2Name,deviceCode2Site,deviceCode2ID = deviceInfo
        deviceNum = len(deviceCode2Name)
        dataArr = get_data(deviceCode2Name.keys(),fetchTime)
        
        dataDf = pd.DataFrame(dataArr,index=deviceCode2Name.keys(),columns=['6','7','8','9','10','11','12'])
        
        Warn = CompreWarn(arr=dataDf.copy().values,deviceIx=dataDf.index,yellow=yellow,red=red,k=2)
        
        # device code
        deviceYellowCode = Warn.deviceYellowCode
        deviceRedCode = Warn.deviceRedCode
        deviceBlueCode = Warn.deviceBlueCode
        # device num
        deviceYellowNum = len(deviceYellowCode)
        deviceRedNum = len(deviceRedCode)
        deviceBlueNum = len(deviceBlueCode)
        deviceNum = deviceRedNum + deviceYellowNum + deviceBlueNum
        # device name,long,lat,ID
        deviceRedName,deviceRedLong,deviceRedLat,deviceRedID = [],[],[],[]
        deviceYellowName,deviceYellowLong,deviceYellowLat,deviceYellowID = [],[],[],[]
        deviceBlueName,deviceBlueLong,deviceBlueLat,deviceBlueID=[],[],[],[]
        if len(deviceYellowCode) !=0:
            for device in deviceYellowCode:
                deviceYellowName = deviceCode2Name[device]
                deviceYellowLong = deviceCode2Site[device][0]
                deviceYellowLat =  deviceCode2Site[device][1]
                deviceYellowID = deviceCode2ID[device] # device id
        if len(deviceRedCode) !=0:
            for device in deviceRedCode:
                deviceRedName.append(deviceCode2Name[device])
                deviceRedLong.append(deviceCode2Site[device][0])
                deviceRedLat.append(deviceCode2Site[device][1])
                deviceRedID.append(deviceCode2ID[device]) # device id
        if len(deviceBlueCode) !=0:
            for device in deviceBlueCode:
                deviceBlueName = deviceCode2Name[device]
                deviceBlueLong = deviceCode2Site[device][0]
                deviceBlueLat = deviceCode2Site[device][1]
                deviceBlueID = deviceCode2ID[device] # device id

        ################  save to MySql   ###################
        saveDf = pd.DataFrame(np.zeros((deviceNum,7)),index=range(deviceNum),
                              columns=['grid_id','device_name','time','level',\
                              'longitude','latitude','device_id'])
        #gird id
        saveDf['grid_id'] = [gridIndex]*deviceNum
        #current time
        saveDf['time'] = [fetchTime]*deviceNum
        #device name
        deviceName = deviceRedName+deviceYellowName+deviceBlueName
        saveDf['device_name'] = deviceName
        #device warn level
        deviceLevel = ['红色预警']*deviceRedNum+['黄色预警']*deviceYellowNum+['蓝色正常']*deviceBlueNum
        saveDf['level']=deviceLevel
        # device site
        deviceLong = deviceRedLong + deviceYellowLong + deviceBlueLong
        deviceLat =  deviceRedLat + deviceYellowLat + deviceBlueLat 
        saveDf['longitude'] = deviceLong
        saveDf['latitude'] = deviceLat
        # device id
        deviceID = deviceRedID + deviceYellowID + deviceBlueID
        saveDf['device_id'] = deviceID
        # device lof
        saveDf['key'] = Warn.deviceLof
        pd.io.sql.to_sql(saveDf,"warn",gridCon, flavor='mysql', if_exists='append',index=False)


#if __name__ =='__main__':
#    init = True
#    while True:
#        if init==True: #首次运行
#            timeCur = datetime.now()
#            if timeCur.minute == 33 and timeCur.second == 00:  #首次运行，在00：20:00时刻
#                init = False
#                fetchTime = timeCur.replace(minute=0,second=0).isoformat()[:19]
#                gridCon = pymysql.connect(host='test.machtalk.net',port=13307,user='root',\
#                           passwd='yunho201311',db='yumairgrid',charset='utf8')
#                mainWarn(gridCon,fetchTime)
#        else:
#            time.sleep(3600)
#            timeCur = datetime.now()
#            fetchTime = timeCur.replace(minute=0,second=0).isoformat()[:19]
#            mainWarn(gridCon,fetchTime)
if __name__ =='__main__':
    init = True
    while True:
        if init==True: #首次运行
            init = False
            timeCur = datetime.now()
            fetchTime = timeCur.replace(minute=0,second=0).isoformat()[:19]
            gridCon = pymysql.connect(host='test.machtalk.net',port=13307,user='root',\
                      passwd='yunho201311',db='yumairgrid',charset='utf8')
            mainWarn(gridCon,fetchTime)
        else:
            time.sleep(3600)
            timeCur = datetime.now()
            fetchTime = timeCur.replace(minute=0,second=0).isoformat()[:19]
            mainWarn(gridCon,fetchTime)