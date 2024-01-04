#
# This script uses an interface with Osisoft's PI to pull voltage data
# from a list of PQ monitor sites that were reporting zero available data.
# The data is processed determine if, after reconfiguration, are still 
# reporting zero available data.
#
# Written by Cody Morrison
# V1 - 4/1/23


##--- module importation---##
from PIconnect.PIConsts import RetrievalMode
import numpy as np
import pandas as pd
import PIconnect as PI

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

##------------PI setup------------##

print('\nDefault Time: '+PI.PIConfig.DEFAULT_TIMEZONE)
PI.PIConfig.DEFAULT_TIMEZONE = 'Australia/Brisbane'
print('\nConfig to UTC+10 Time: '+PI.PIConfig.DEFAULT_TIMEZONE)

print('\nServer List: '+str(list(PI.PIServer.servers.keys())))
print('\nDatabase List: '+str(list(PI.PIAFDatabase.servers.keys())))
print('\n-----------------------------------------------------\n')

##------------functions------------##

def indataCheck(k,n):
    # This function checks if a filepath can be formed using the csv's data file.
    # If no filepath can be formed for a particular PQ monitor site, it is skipped
    # and the next site is tested.

    ##---checking that sub, region, and location are given in the csv list so a filepath can be formed---##
    testStr = np.genfromtxt('NOVmonitors.csv',dtype=str,delimiter=',',usecols=(5),skip_header=1)
    
    while testStr[k]=='No Data' or testStr[k]=='XXXX':

        n += 1
        print('Cannot form filepath for k = {f}'.format(f=str(k)))
        print('This site has been skipped. {h} have failed to form a filepath. k = {g} will now be run:'.format(h=str(n),g=str(k+1)))
        print('\n---------------------------next monitor---------------------------\n')
        
        k += 1
        
    if testStr[k]!='No Data' or testStr[k]!='XXXX':

        print('Filepath can be formed for k = {f}'.format(f=str(k)))

        return k, n


def dataGrab(fPath1,fPath2):
    # This function uses PIConnect (an interface with Osisoft's PI) to pull voltage
    # and from PQ monitors. This function outputs a matrix stating if live data is retrievable.

    ##---pulling data from Osisoft PI with formed filepath---##
    with PI.PIAFDatabase(database="PQ Monitors") as database:

        dataMatrix = ['Monitor in col A',"Monitor in Bonora's comments",2000,2000]
        #dataMatrix = np.array(['a','b'],dtype=str)#.reshape(1,2)
        #print(dataMatrix)

        
        ##---timeline to pull data---##
        intT = '10m'
        
        startT1 = '2024-01-02 00:00:00'
        endT1 = '2024-01-04 00:00:00'

        try:
            element1 = database.descendant(fPath1)
            attvalues1 = iter(element1.attributes.values())

            attList1 = [next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),
                        next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1),next(attvalues1)]

            ## QZ 1
            ##---search and assign Voltage and Current data to matrix---##
            for att in range(len(attList1)):
                #print(attList[att].name)

                if attList1[att].name == 'VOLT_A':
                    VdataAa1 = attList1[att].interpolated_values(startT1,endT1,intT)


            k = 0
            
            ##print(VdataAa1)
            
            for i in range(len(VdataAa1)-2):
                i+=1
                if (type(VdataAa1.iloc[i]) is np.float64 or type(VdataAa1.iloc[i]) is float) and (VdataAa1[i] != VdataAa1[i-1] or VdataAa1[i]-VdataAa1[i-1] != VdataAa1[i]-VdataAa1[i-2]):

                #if (type(VdataAa.iloc[i]) is np.float64 or type(VdataAa.iloc[i]) is float) and (round(VdataAa[i],1) != round(VdataAa[i-1],1) or round(VdataAa[i]-VdataAa[i-1]) != round(VdataAa[i]-VdataAa[i-2])):
                    
                    k += 1

            print(len(VdataAa1))
            print('K=')
            print(k)
            if k > 100:
                print('fPath1 Provides live data')
                dataMatrix[0]='Provides live data'
                dataMatrix[2]=k
                #dataMatrix.replace('a','Provides live data',1)
            if k < 100:
                print('fPath1 No live data')
                dataMatrix[0]='No live data'
                dataMatrix[2]=k
               # dataMatrix.replace('a','No live data',1)


            element2 = database.descendant(fPath2)
            attvalues2 = iter(element2.attributes.values())

            attList2 = [next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2)]
            
            print('filepath1 was fine')

            ## QZ 2
            ##---search and assign Voltage and Current data to matrix---##
            for att in range(len(attList2)):
                #print(attList[att].name)

                if attList2[att].name == 'VOLT_A':
                    VdataAa2 = attList2[att].interpolated_values(startT1,endT1,intT)
            u = 0
            
            ##print(VdataAa2)
            
            for i in range(len(VdataAa2)-2):
                i+=1
                if (type(VdataAa2.iloc[i]) is np.float64 or type(VdataAa2.iloc[i]) is float) and (VdataAa2[i] != VdataAa2[i-1] or VdataAa2[i]-VdataAa2[i-1] != VdataAa2[i]-VdataAa2[i-2]):

                #if (type(VdataAa.iloc[i]) is np.float64 or type(VdataAa.iloc[i]) is float) and (round(VdataAa[i],1) != round(VdataAa[i-1],1) or round(VdataAa[i]-VdataAa[i-1]) != round(VdataAa[i]-VdataAa[i-2])):
                    
                    u += 1

            print(len(VdataAa2))
            print('u=')
            print(u)
            if u > 100:
                dataMatrix[1]='Provides live data'
                dataMatrix[3]=u
                #dataMatrix.replace('b','Provides live data',1)
                print('fPath2 Provides live data')
            if u < 100:
                print('fPath2 No live data')
                dataMatrix[1]='No live data'
                dataMatrix[3]=u
                #dataMatrix.replace('b','No live data',1)
    
        except:
            
            element2 = database.descendant(fPath2)
            attvalues2 = iter(element2.attributes.values())

            attList2 = [next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),
                        next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2),next(attvalues2)]
            print('fPath1 failed')

            ## QZ 2
            ##---search and assign Voltage and Current data to matrix---##
            for att in range(len(attList2)):
                #print(attList[att].name)

                if attList2[att].name == 'VOLT_A':
                    VdataAa2 = attList2[att].interpolated_values(startT1,endT1,intT)


            u = 0
            
            ##print(VdataAa2)
            d
            for i in range(len(VdataAa2)-2):
                i+=1
                if (type(VdataAa2.iloc[i]) is np.float64 or type(VdataAa2.iloc[i]) is float) and (VdataAa2[i] != VdataAa2[i-1] or VdataAa2[i]-VdataAa2[i-1] != VdataAa2[i]-VdataAa2[i-2]):

                #if (type(VdataAa.iloc[i]) is np.float64 or type(VdataAa.iloc[i]) is float) and (round(VdataAa[i],1) != round(VdataAa[i-1],1) or round(VdataAa[i]-VdataAa[i-1]) != round(VdataAa[i]-VdataAa[i-2])):
                    
                    u += 1
                    
            print(len(VdataAa2))
            print('u=')
            print(u)
            if u > 100:
                print('fPath2 Provides live data')
                dataMatrix[0]='fPath1 unavailable'
                dataMatrix[1]='Provides live data'
                dataMatrix[2]='Failed'
                dataMatrix[3]=u
                #dataMatrix.replace('a','fPath1 unavailable',1)
                #dataMatrix.replace('b','Provides live data',1)
                #dataMatrix = ['fPath1 unavailable','Provides live data']
            if u < 100:
                print('fPath2 No live data')
                dataMatrix[0]='fPath1 unavailable'
                dataMatrix[1]='No live data'
                dataMatrix[2]='Failed'
                dataMatrix[3]=u
                #dataMatrix.replace('a','fPath1 unavailable',1)
                #dataMatrix.replace('b','No live data',1)
        
        print(dataMatrix)
        return dataMatrix

def feederGrab(txrN,monN1,monN2,parPath):
    # This function searches the AF database and pulls the feeder name for the monitor site.
    # As only the transformer number/name and substation name is given, the feeder
    # name must be searched for and returned to form a full filepath for PI data.

    print(txrN,monN1,monN2,parPath)
    
    ##---searching database for transformer to get feeder, as feeder isn't given in csv---##
    with PI.PIAFDatabase(database="PQ Monitors") as database:
        
        feederSRCH = database.descendant(parPath)
        for fdr in feederSRCH.children.values():

            fdrStr = str(fdr)
            fdrStr1 = fdrStr.replace('PIAFElement(','')
            fdrStr2 = fdrStr1.replace(')','')
            #print(fdrStr2)

            ##---transformer being searched---##            
            txrSRCH = database.descendant(parPath+'\\'+fdrStr2)
            for txr in txrSRCH.children.values():
                
                txrStr = str(txr)
                txrStr1 = txrStr.replace('PIAFElement(','')
                txrStr2 = txrStr1.replace(')','')
                #print(txrStr2)

                ##---building full filepath for PI to read---##
                if txrStr2==txrN:

                    fPath1 = parPath+'\\'+fdrStr2+'\\'+txrStr2+'\\'+monN1
                    fPath2 = parPath+'\\'+fdrStr2+'\\'+txrStr2+'\\'+monN2

                    print('Found monitor filepath for monitor {a}: {b}'.format(a=monN1,b=fPath1)) 
                    print('Found monitor filepath for monitor {a}: {b}'.format(a=monN2,b=fPath2)) 
                    
                    return fPath1,fPath2
                

def pathGrab(k):
    # This function forms a partial filepath and passes it to the above function
    # (feederGrab) so that a full filepath can be passed to PIConnect (dataGrab) function.
    # This function uses the monitor csv file to concatenate a filepath.

    ##---pulling sub, region and txr name from the csv file---##
    siteDF23 = np.genfromtxt('NOVmonitors.csv',dtype=str,delimiter=',',usecols=(0,2,3,4,5,11),skip_header=1)

    eqlName = "EQL"
    regName = siteDF23[k][4]
    locName = siteDF23[k][3]
    subName = siteDF23[k][2]
    txrName = siteDF23[k][1]
    monName1 = siteDF23[k][5]
    monName2 = siteDF23[k][0]

    arr1 = [regName,locName,subName,txrName,monName1,monName2]
    print(arr1)

    ##---forming/concatenating the filepath---##
    oPath = "\\".join([eqlName,regName,locName,subName])
    parPath = r'{}'.format(oPath)
    
    print('\nFound partial filepath from CSV: '+parPath+'\n')
    
    return txrName,parPath,monName1,monName2


##-----------------main script-----------------##
# View individual functions/methods for description

##---intialising counters for main script---##

i = 0
start = 0
end = 205

output1 = np.array(['Monitor in col A',"Monitor in Bonora's comments",2000,2000])

##---main script prints for number of sites given---##
for start in range(end):
    
    i, failed = indataCheck(i,failed)

    txrName, parPath, monName1,monName2 = pathGrab(i)

    if parPath == "EQL\\UNKOWN REGION\\UNKNOWN AREA\\XXXX":
        output = np.array(['Failed filepath','Failed filepath','Failed','Failed']) 
    else:
        filePath1,filePath2 = feederGrab(txrName,monName1,monName2,parPath)

        output = dataGrab(filePath1,filePath2)
    
        output = np.array(output)
    
    output1 = np.vstack([output1,output])
        #print('testoutput, main')
        #print(output1)
    
    print('\n---------------------------next monitor---------------------------\n')
    
    i+=1


##---printing summary of program---##
print('List of Unbalance Percentiles')

outputDF = pd.DataFrame(output1)
print('output dataframe')
print(outputDF)

outputDF.to_csv('outputDF2.csv',',')

