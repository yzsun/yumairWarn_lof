#!/usr/bin/local/env python
from __future__ import division
import pandas as pd
import numpy as np
import math

class CompreWarn():
    def __init__(self,arr,deviceIx,yellow,red,k):
        pots = pd.DataFrame(arr,index=deviceIx)
        self.yellow = yellow;self.red = red;self.k = k
        self.potsDf = pots;self.potsArr = self.potsDf.values
        deviceNum = len(self.potsDf)
        items_max = np.max(self.potsArr,0);items_min = np.min(self.potsArr,0)
        self.potsArr = self.normalization(self.potsArr,items_max,items_min) #normalization
        self.deviceWarn = pd.DataFrame(index=self.potsDf.index,columns=['lof','dist','negNum'])
        deviceAver = np.average(self.potsArr,0)
        for pot,device in zip(self.potsArr,self.potsDf.index):
            lof_value = self.local_outlier_factor(self.k,pot,self.potsArr)
            self.deviceWarn['lof'][device] = lof_value
            self.deviceWarn['dist'][device] = self.distEuclidean(pot,deviceAver)
            self.deviceWarn['negNum'][device] = sum(map(lambda x:int(x),(pot-deviceAver)<0)) #Negative
        # distance to average based warning
        self.deviceWarnSort = self.deviceWarn.sort_values(by='dist')
        self.deviceYellow = self.deviceWarnSort[int(deviceNum*0.0):]
        self.deviceRed = self.deviceWarnSort[int(deviceNum*0.0):]
        # lof based warning
        self.deviceYellow = self.deviceYellow[np.logical_and(self.deviceYellow['lof']<self.red,self.deviceYellow['lof']>self.yellow)]
        self.deviceRed = self.deviceRed[self.deviceRed['lof']>self.red]
        # negative to average based warning
        self.deviceYellow = list(self.deviceYellow.index[self.deviceYellow['negNum']<4])
        self.deviceRed = list(self.deviceRed.index[self.deviceRed['negNum']<4])
        
    #normalization
    def normalization(self,arr,arr_max,arr_min):
        for col in range(arr.shape[1]): 
            arr[:,col] = (arr[:,col]-arr_min[col])/(arr_max[col]-arr_min[col])
        return arr
        
    # Euclidean distance
    def distEuclidean(self,pot1,pot2):
        return math.sqrt(sum((pot1-pot2)**2)/len(pot1))
        
    # k_distance, neighbour within k_distance
    def k_dist(self,k,pot,set):
        dist = {};neighbour=[]
        for row in set:
            if not all(row==pot):
                distValue = self.distEuclidean(pot,row)
                if distValue in dist:
                    dist[distValue].append(list(row))
                else:
                    dist[distValue]=[list(row)]
        distOrder = sorted(dist.items()) 
        k_distance = distOrder[k-1][0]
        for neigh in distOrder[:k]:
            neighbour.extend(neigh[1]) 
        return k_distance,np.array(neighbour)
        
    # reachable distance (from neigh to pot)
    def reach_dist(self,k,pot,neigh,set):
        k_distance,_ = self.k_dist(k,neigh,set)
        return max(k_distance,self.distEuclidean(pot,neigh))
        
    # local reachable density(lrd)
    def local_reach_density(self,k,pot,set):
        k_distance,neighbour = self.k_dist(k,pot,set)
        reach_dist_list = []   
        for neigh in neighbour:
            reach_dist_list.append(self.reach_dist(k,pot,neigh,set))
        return len(reach_dist_list)/sum(reach_dist_list)
        
    # lof
    def local_outlier_factor(self,k,pot,set):
        _,neighbour = self.k_dist(k,pot,set)
        lrd_pot = self.local_reach_density(k,pot,set)
        lrd_neigh=[]
        for neigh in neighbour:
            lrd_neigh.append(self.local_reach_density(k,neigh,set))
        lof = sum(lrd_neigh)/len(neighbour)/lrd_pot
        return lof

