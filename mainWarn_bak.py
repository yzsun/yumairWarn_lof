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
    '''return:gridDf
    DataFrame(   grid_id   left_down_latitude left_down_longitude right_up_latitude right_down_longitude 
                 '1500'   35.314778           116.12817             35.894778       117.12817
                 '121'    35.374778           117.12817             35.431478       116.19837
                 ...........................................................................
                 '143'    35.764778           116.4817              35.234778       115.12817)'''
    gridDf = pd.read_sql('select distinct id,\
                           left_down_latitude, left_down_longitude, \
                           right_up_latitude,right_down_longitude \
                           from grid',gridCon,index_col='id')
    return gridDf

def get_device(gridDf,gridIndex):
    '''Input: gridIndex，即网格id 如 “1500”
       return: deviceCode2Name{'deviceCode':['YA00100000000001',...,'YA00100000000165']
                               'deviceName':['1#微站',...,'n#微站']} '''
    deviceUrl = 'http://192.168.0.69:5558/services/device/getDeviceCount'
    paras = {"queryType":'YUMAIR',
             "longitudeMin":gridDf['left_down_longitude'][gridIndex],
             "longitudeMax":gridDf['right_down_longitude'][gridIndex],
             "latitudeMin":gridDf['left_down_latitude'][gridIndex],
             "latitudeMax":gridDf['right_up_latitude'][gridIndex]}
    tmp = requests.get(deviceUrl,params=paras)
    tmp = json.loads(tmp.text)
    deviceCode2Name = {}
#    print('tmp',tmp)
    for device in tmp:
        deviceCode2Name[device['deviceCode']]= device['deviceName']
    return deviceCode2Name

def get_data(deviceList,fetchTime): 
    dataUrl = "http://192.168.0.177:10886/v1.1/yumair/statistic/raw"
    data = json.dumps({"dids":','.join(deviceList),
         "dvids":",".join(['6','7','8','9','10','11','12']),#,pm2_5,pm10,co,no2,so2,o3,aqi',
         "type":'average',
         "period":'real',
         "startTime":fetchTime,
         "endTime":fetchTime})
    headers={"APIKey":"42d6c32f992541eea8b4e835cb9b6b77"}
    dataTmp = requests.post(dataUrl,data,headers=headers)
    dataTmp = json.loads(dataTmp.text)
    data=[]
    for i in range(len(dataTmp['datas'])):
        data.append(float(dataTmp['datas'][i]['data'][0][fetchTime]))
    data = np.reshape(data,(-1,7))
    return data

def mainWarn(gridCon,fetchTime,yellow=2.5,red=4,k=2):
    gridDf = get_Grid(gridCon)
    for grid in gridDf.index:
        deviceCode2Name = get_device(gridDf,grid)
        deviceNum = len(deviceCode2Name)
        dataArr = get_data(deviceCode2Name.keys(),fetchTime)
        dataDf = pd.DataFrame(dataArr,index=deviceCode2Name.keys(),columns=['6','7','8','9','10','11','12'])
        
        Warn = CompreWarn(arr=dataDf.copy().values,deviceIx=dataDf.index,yellow=yellow,red=red,k=2)
        deviceYellowCode = Warn.deviceYellow
        deviceRedCode = Warn.deviceRed 

        deviceYellowName = [deviceCode2Name[device] for device in deviceYellowCode]
        deviceRedName = [deviceCode2Name[device] for device in deviceRedCode]
        deviceBlueName = deviceCode2Name.values()
        for deviceWarn in deviceRedName+deviceYellowName:
            deviceBlueName.remove(deviceWarn)
        
        deviceRedNum = len(deviceRedName)
        deviceYellowNum = len(deviceYellowName)
        deviceBlueNum = len(deviceBlueName)
        
        # save to MySql
        saveDf = pd.DataFrame(np.zeros((deviceNum,4)),index=range(deviceNum),
                              columns=['grid_id','device_name','time','level'])
        saveDf['grid_id'] = [grid]*deviceNum
        saveDf['time'] = [fetchTime]*deviceNum
        deviceName = deviceRedName+deviceYellowName+deviceBlueName
        saveDf['device_name'] = deviceName
        deviceLevel = ['红色预警']*deviceRedNum+['黄色预警']*deviceYellowNum+['蓝色正常']*deviceBlueNum
        saveDf['level']=deviceLevel
        pd.io.sql.to_sql(saveDf,"warn",gridCon, flavor='mysql', if_exists='append',index=False)

if __name__ =='__main__':
    init = True
    while True:
        if init==True: #首次运行
            timeCur = datetime.now()
            if timeCur.minute == 20 and timeCur.second == 00:  #首次运行，在00：20:00时刻
                init = False
                fetchTime = timeCur.replace(minute=0,second=0).isoformat()[:19]
                gridCon = pymysql.connect(host='test.machtalk.net',port=13307,user='root',\
                           passwd='yunho201311',db='yumairgrid',charset='utf8')
                mainWarn(gridCon,fetchTime)
        else:
            time.sleep(3600)
            timeCur = datetime.now()
            fetchTime = timeCur.replace(minute=0,second=0).isoformat()[:19]
            mainWarn(gridCon,fetchTime)
    
    
 
#    dataArr[0,1]=dataArr[0,1]+1000
#    dataArr[0,3]=dataArr[0,3]+10
#    dataArr[0,4]=dataArr[0,1]+1000
#    dataArr[0,5]=dataArr[0,1]+1000
#    dataArr[4,1]=dataArr[0,1]+1000
#    dataArr[4,5]=dataArr[0,1]+1000
#    dataArr[4,2]=dataArr[0,1]+1000
#    dataArr[4,3]=dataArr[0,1]+1000
#    

 
    
    
        
# ## figure ##
#    items=['6','7','8','9','10','11','12']
##    dataDf.index=range(12)
#    plt.figure()
#    axSet = [(0,0),(0,1),(1,0),(1,1),(2,0),(2,1),(3,0),(3,1)]
#    ## item ##
#    for item,axIndex in zip(items,axSet):
#        plt.subplot2grid((4,2),axIndex)
#        itemData = dataDf[item]   #aqi
#        mean = np.mean(itemData)
#        # 趋势线
#        plt.plot(range(12),itemData)      
#        #散点
#        for singleData,xDevice,i in zip(itemData,dataDf.index,range(12)):
#            if xDevice in deviceYellow:
#                m = 'o'
#                c = 'y'
#                s = 10
#            elif xDevice in deviceRed:
#                m = 'o'
#                c = 'r'
#                s = 10                
#            else:
#                m = '*'
#                c = 'b' 
#                s = 3                
#            plt.plot(i,singleData,marker = m,color=c,markersize=s)
#        plt.plot([mean]*(len(itemData)),'g--',label='mean')
#        plt.title(item)
##        plt.xlim(7,31)  
#    ##  lof  ##
#    plt.subplot2grid((4,2),(3,1)) 
#    # 趋势线
##    for i,device in zip(range(12),deviceList):
#    plt.plot(Warn.deviceWarn['lof'].values)
#    #散点
#    for singleLof,xDevice,i in zip(Warn.deviceWarn['lof'].values,dataDf.index,range(12)):
#        if xDevice in deviceYellow:
#            m = 'o'
#            c = 'y'
#            s = 10
#        elif xDevice in deviceRed:
#            m = 'o'
#            c = 'r'
#            s = 10                
#        else:
#            m = '*'
#            c = 'b' 
#            s = 3                
#        plt.plot(i,singleLof,marker = m,color=c,markersize=s)
#    # alert line    
#    plt.plot([yellow]*(len(itemData)),'y--',label='yellow alert')
#    plt.plot([red]*(len(itemData)),'r--',label='red alert')
##    plt.xlim(7,31)
##    plt.legend()
##    plt.savefig('./'+timeIndex[:-6]+'_alert.png')     
#    plt.show()
        
        
        
        
        
        
        
        
        
        









