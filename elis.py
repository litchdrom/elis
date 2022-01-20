import asyncio
import logging
import os
import redis
import re
import time
import json
import pyodbc
from pprint import pprint
import websockets
import requests

from panoramisk import Manager, Message
from datetime import datetime, timezone

requesturl="https://starside.it-service.io/backend_api.php?action=save_call"
#r = redis.Redis(host='localhost', port=6379, db=0, charset="utf-8")
##r = redis.Redis(host='localhost', port=6379, db=0, charset="utf-8", decode_responses=True)

pool = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
r = redis.Redis(connection_pool=pool,charset="utf-8")


i=1
manager = Manager(
    host=os.getenv('AMI_HOST', '95.161.104.2'),
    port=os.getenv('AMI_PORT', 5038),
    username=os.getenv('AMI_USERNAME', 'starside'),
    secret=os.getenv('AMI_SECRET', 'r7qyHX34JGxSogdbmnNMtPTl0w'),
    ping_delay=10,  # Delay after start
    ping_interval=10,  # Periodically ping AMI (dead or alive)
    reconnect_timeout=2,  # Timeout reconnect if connection lost
)

specnumber=['122','112','911']
ivrvars=['DIGITS']
trunks=['go']
mainpar=['Linkedid','Route','Originate','CallerIDNum','CallerIDName','Destination','CallThrough','Main_start','Main_end','Main_Status','Up_duration','Main_duration','Bill_duration','Main_Hold','Rating']

#Main_duration 155.1012420654297
#Bill_duration 155.09805703163147
#Main_end 1642416787.62275

normagents=['Queue','MemberName','Interface',
        'StateInterface','Membership','Penalty',
        'CallsTaken','LastCall','LastPause',
        'InCall','Status','Paused',
        'PausedReason','Ringinuse','Wrapuptime']

#async def wsservrun(websocket):
##    print(msg)
#    while True:
#        redstring=r.hgetall('Queuemon')
#        await websocket.send("Hello world!")
 #       for key, value in redstring.items():
 #           greeting = {key,value}
 #           await websocket.send(greeting)
#        await asyncio.sleep(3)






async def qureload_onstart(mngr: Manager):
    #print('Show queue')
    #action = {'Action': 'Command','Command':'queue reload'}
    action = {'Action': 'Command','Command':'queue show'}
    temp = await mngr.send_action(action,as_list=False)
#    print(temp)

async def dev_state(mngr: Manager):
#    print('Show queue')
    #action = {'Action': 'Command','Command':'queue reload'}
    while True:
        r.delete('Devmon', 0, -1)
        action = {'Action':'DeviceStateList'}
        temp = await mngr.send_action(action,as_list=False)
        await asyncio.sleep(3)


def activechan(linkeidid,action):
    #print(linkeidid,action)
    if action == 'add':   
        redadd=r.rpush("activechan_id",linkeidid)
        radansv='add_ok'
    if action == 'del':
     #   redindex=r.lpos("activechan_id",linkeidid)
     #   print("redindex is ",redindex)
     #   redindex=r.delete("activechan_id",redindex,redindex)
        redindex=r.lrem("activechan_id",0,linkeidid)
        radansv='del_ok'



loop = asyncio.new_event_loop()

def route_par(source, dest):
    print(source, dest, len(source), len(dest))
    if dest in specnumber:
        route ="Dialout"
    elif len(source) <= 4 and len(dest) <= 4 and not dest=="s":
        route="Local"
    elif len(source) >= 10 and len(dest) >= 10:
        route="Dialin"
    #elif len(source) <= 4 and len(dest) = 3:
    else: 
        route ="Dialout"
    #/print(route)
    return route

def splitchan(channel):
    splitchan=channel.split('-')
    splitchan=splitchan[0].split('/')
    splitchan=splitchan[1]
    return(splitchan)

def splitlocal(channel):
    splitchan=channel.split('@')
#    splitchan=splitchan[0].split('/')
    splitchan=splitchan[0]
    return(splitchan)

def getsid(chahnnum):
    cnxn = pyodbc.connect('DSN=asterisk_rt')
    cursor = cnxn.cursor()
    sql=("select crm_id from starside.agents where is_deleted=0 and endpoint_id= ?")
    params=chahnnum
    cursor.execute(sql, params)
    row = cursor.fetchone()
    cnxn.close()
    if row:
        starsideid=row[0]
    #    print(row[0])
    else: 
        starsideid='None'
    return starsideid

#async def wsserverr():
#    async with websockets.serve(wsservrun,'0.0.0.0', 8765):
#        await asyncio.Future()
#    chan={}
async def serializer(dictionary,state):
    asyncio.sleep(0.1)
#    b=b
    ser=r.hgetall(dictionary)
#    ser=dictionary
    main={}
    sourcepr={}
    mainpr={}
    iterators={}
    destpr={}
    channels={}
    chan={}
    maininfo={}
    iterator={}
    qupr={}
    quchan={}
    aqupr={}
    qntd=0
    cnt=str(0)
    
    rec={'CallRec':ser['CallRec']}

    for key,value in r.hscan_iter(dictionary, match='Qu_*'):
        print(key,value)
    print('++++++++++++++++++','www','++++++++++++++++++')

    for key,value in ser.items():
        if  re.search(r'Source', key) and not  re.search(r'hold', key):
            sourcepr[key]=value
#            print(sourcepr)
        if not re.search(r'Chann', key) and not re.search(r'Qu', key) and not key == 'CallRecord' and not re.search(r'agent', key):
            mainpr[key]=value
 #           print (mainpr)
        if  re.search(r'Iterator', key):
            iterators[key]=value
  #          print(iterators)
        if  re.search(r'DstChannel', key) and not re.search(r'Qu', key) and not re.search(r'_agent_', key) and not  re.search(r'hold', key):
            destpr[key]=value

        if  re.search(r'Qu_', key) and not re.search(r'DstChannel_', key)  and not re.search(r'DstChannel_', key) and not re.search(r'Qu_\d+:\d+:.+:.+', key):
            qupr[key]=value

        if  re.search(r'Qu_\d+:\d+:.+:.+', key):
            quchan[key]=value

        if  re.search(r'Iterator', key) and not re.search(r'DstChannel_', key):
            iterator[key]=value

    for key,value in  qupr.items():
        if  re.search(r'^Qu_\d+$', key):
#            qupr[key]=value
            qnt=key.split('_')[1]
            if not 'Qu_'+str(qnt)+'_status' in qupr:
                qntd=qnt
                #print('active qu'+qntd)
#            else:
 #               qntd=str(0)
#            )
    

    for key,value in  qupr.items():
        if not qntd == 0:
            if re.search(r'Qu_'+qntd, key):
                print(key,value)


#    print(qupr)
 #           aqupr[key]=value
#            else:
#        print(key,value)

#        if not re.search(r'Qu_'+qntd, key):
#            print(key,value)
#            chan[cnt][key]=value

       # else:
        #    destpr={}

   # ci=iterators['Iterator']
   #         print(destpr)

    for key,value in  destpr.items():
#        cnt=str(0)
  #      print('******************************************')
  #      print("fchan",key,value)
  #      print('******************************************')
        #if  re.search(r'DstChannel_'+ci, key) and not re.search(r'Hangup'+ci, value):
        if  re.search(r'DstChannel_\d+$', key):
            cnt=key.split('_')[1]
            #print(key,value)
            if state == 'active':
                if destpr['DstChannel_'+str(cnt)+'_state'] == 'Hangup':
                    cnt=str(0)
            else:
                cnt=cnt
            channum=splitchan(value)
##            if channum in trunks:
            channum=channum+"_"+cnt
            schan="DstChannel_"+cnt

            chan={channum:{}}
            #chan={cnt:{}}
            for key,value in  destpr.items():
                if re.search(fr'{schan}', key):
       # if re.search(schan, key):
#            cnt=key.split('_')[1]
#            channum=channum+"_"+cnt
#                    print('prejson',key,value,'++',schan)
                    chan[channum][key]=value
#            if 

                    channels.update(chan)
#            print(chan)
    for key,value in  mainpr.items():
        if key in mainpar:
            maininfo[key]=value
        if re.search(r'IVR_digits', key):
            maininfo[key]=value

            

#        chan[cnt]=chan   
#        print(chan)
        
#        if  re.search(r'DstChannel_', key) and not re.search(r'Hangup', value):
#        print(key,value,cnt,chan)
#    print(chan)
#            print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
#            chan=dict(chan+ci)
#            chan[key]=value
#    channels.update(chan)



    
        

#            pass
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print(maininfo,sourcepr,channels,qupr,quchan,iterators,rec)

#    print(prejson,queuedata)
#    print(sourcepr,mainpr,iterators,destpr)
#    print(prejson,callers)
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    if state == 'stop':
        call={}
        calllist={}
        mainparm=[maininfo,sourcepr,channels,qupr,quchan,iterators,rec]
        mainparmn=['maininfo','source','channels','queues','queuechannels','iterators','rec']
        print(maininfo['Linkedid'])
        for i, x in enumerate(mainparm):
            call={mainparmn[i]:x}
 #           print(call)
            calllist.update(call)

        print(calllist)
        findict={maininfo['Linkedid']:calllist}
#        print(findict)
        params=findict
        response = requests.post(requesturl, json=params,timeout=6)
        print(response)
#    return

#        print(mainparmn[i],x)
#        call[maininfo['Linkedid']]={mainparmn[i]:x}
        
    #    main.
    



                #if len(key.split('-')) > 0:
                #    print(len(key.split('-')))

#            chann={
#            
#            }


    #print(prejson)
 #   return(dictionary)
    

async def calltoweb():
    while True:
        #print('uu')
        active=r.lrange('activechan_id', 0, -1)
        if not  active:
            r.delete('activechan_web', 0, -1)
            #print('No calls')
        else: 
        #    print(active,len(active))
            for i,x in enumerate(active):
#                if not x == r.hgetall
                redict=x
                #redict=r.hgetall(x)
                try:
                    await serializer(redict,'active')
                except Exception as e:
                    print("type error: " + str(e))




                
             #   print('Call #',i,ser)

#        lrange activechan_id 0 -1


        await asyncio.sleep(2)

    
#uu=calltoweb('uu')
#    return radansv
        

#   #     r.zadd("activechan_id",linkeidid)
#    #    r.rpush("activechan_id",linkeidid)
async def queuesdata(mngr: Manager):
    while True:
        #print('uu')
        r.delete('Queuemon', 0, -1)
        r.delete('Agentmon', 0, -1)
        action = {'Action': 'QueueStatus'}
        temp = await mngr.send_action(action,as_list=False)
        await asyncio.sleep(2)
        



def on_connect(mngr: Manager):
    logging.info(
        'Connected to %s:%s AMI socket successfully' %
        (mngr.config['host'], mngr.config['port'])
    )


def on_login(mngr: Manager):
    logging.info(
        'Connected user:%s to AMI %s:%s successfully' %
        (mngr.config['username'], mngr.config['host'], mngr.config['port'])
    )


def on_disconnect(mngr: Manager, exc: Exception):
    logging.info(
        'Disconnect user:%s from AMI %s:%s' %
        (mngr.config['username'], mngr.config['host'], mngr.config['port'])
    )
    logging.debug(str(exc))


async def on_startup(mngr: Manager):
    await asyncio.sleep(0.1)
    logging.info('Something action...')
    r.delete("activechan_id",0,-1)
    #startbackend = await calltoweb()
#    startbackend = await  calltoweb()
#    startqueuedata = await queuesdata(mngr)
    await asyncio.gather(calltoweb(),  queuesdata(mngr), qureload_onstart(mngr) ,dev_state(mngr))
    #start_server = websockets.serve(wsserverr,'localhost', 8765)
#    loop.run_until_complete(calltoweb('uu'))
#        temp = await mngr.send_action(action,as_list=False)


async def on_shutdown(mngr: Manager):
    await asyncio.sleep(0.1)
    logging.info(
        'Shutdown AMI connection on %s:%s' % (mngr.config['host'], mngr.config['port'])
    )
#    r.delete("activechan_id",0,-1)

#def queue_status(mngr: Manager):
#    yield from mngr.send_action(
#                {'Action': 'Ping'})
#    pprint('Does it work')
#    pprint(queues_details)



@manager.register_event('*')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    if msg.Event == 'FullyBooted':
        print(msg)

#@manager.register_event('QueueCallerJoin')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    print('Caller join')
#    print(msg)

#    print(msg)

#@manager.register_event('QueueCallerAbandon')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    print('Caller abandon')
#    print(msg)
#    print(msg)

#    str1=str(msg).split( )
#    print(str1[3],str1[13])
    

#@manager.register_event('BridgeEnter')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    print(msg)

#@manager.register_event('BridgeEnter')  # Register all events
#async def ami_callback(event, manager):
#    msg = dict(manager)
#    print(msg['CallerIDNum'],msg['ConnectedLineNum'], msg['Exten'])
#    print(msg)

#@manager.register_event('Newchannel')  # Register all events
#async def ami_callback(event, manager):
#    msg = dict(manager)
##    print('------------------------------------------Channel started---------------------------------------------------------------------------------------------')
#    print(msg)



@manager.register_event('Newchannel')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    skipmark=0
#    print(msg['Channel'],msg['Exten'],msg['Uniqueid'],msg['Linkedid'],msg['ChannelStateDesc'])
    #if not msg['Exten'] == 's':
#    if  re.search(r'originate', msg['Channel']):
#        print("originate  channel suppressed")
#    else:
#        pass
    #print(r.hexists(msg['Linkedid'],"SourceChannel"))
    #print(r.hexists(msg['Linkedid'],"Iterator"))
 #   print(r.hexists(msg['Linkedid'],"Channel"))

    if not r.hexists(msg['Linkedid'],"SourceChannel") and not r.hexists(msg['Linkedid'],"Iterator"):
    ##if not r.hexists(msg['Linkedid'],"DstChannel") and not r.hexists(msg['Linkedid'],"Iterator") or re.search(r'originate', msg['Channel']):
        if re.search(r'originate', msg['Channel']):
         #   print('Originate suppresed src')
          #  print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXiXXZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
            r.hset(msg['Linkedid'], "Originate", "1")
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=2

        if re.search(r'trsf', msg['Channel']):
            #print('Transfer orphan  suppresed src')
            r.hset(msg['Linkedid'], "Orphan", "1")
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=1

        if  re.search(r'forwards', msg['Channel']):
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
           # print('Forward suppresed src')
            skipmark=1
  #          r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
  #          action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
#            itera = int(r.hget(msg['Linkedid'],"Iterator"))
       #     action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
       #     temp = await mngr.send_action(action,as_list=False)
       #     temp=dict(temp)
       #     print(temp)
        if  msg['Exten'] == '*8' and msg['Context'] == 'internal':
            r.hset(msg['Linkedid'], "Orphan", "1")
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=1


        if  msg['Exten'] == 's' and msg['Context'] == 'internal':
#        #if  msg['Exten'] == 's' and msg['Context'] == 'internal' and r.hexists(msg['Linkedid'],"Iterator"):
            if not r.hexists(msg['Linkedid'],"Originate"):
#            if r.hexists(msg['Linkedid'],"Iterator"):
                #print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                #print('Transfer orphan  suppresed src', msg['Linkedid'], r.hget(msg['Linkedid'],"Iterator"),">",0)
                r.hset(msg['Linkedid'], "Orphan", "1")
                #print(msg)
                skipmark=3
               # print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            else:
            #    print('ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ')
                r.hset(msg['Linkedid'], "Orphan", "1")
                #print(msg)
                #print('ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ','Orphan',msg['Channel'],'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ')
  #                  pass
#        else:
        if skipmark==0:


#            print('ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',msg['Channel'],'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ')
#            print(temp['ConnectedLineNum'])
            rdefine=route_par(msg['CallerIDNum'], msg['Exten'])
            #print(rdefine)
            r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
            r.hset(msg['Linkedid'], "Route", rdefine)
            r.hset(msg['Linkedid'], "SourceChannel", msg['Channel'])
            #srcclone=msg['Channel']
            r.hset(msg['Linkedid'], "SourceChannel_state", msg['ChannelStateDesc'])
            if  len(msg['CallerIDNum']) == 10:
                r.hset(msg['Linkedid'], "CallerIDNum", "7"+msg['CallerIDNum'])
            else:
                r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "CallerIDName", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "Destination", msg['Exten'])
            r.hset(msg['Linkedid'], "Iterator", 0)
            r.hset(msg['Linkedid'], "QuIterator", int(0))
            r.hset(msg['Linkedid'], "IVRIterator", int(0))
            r.hset(msg['Linkedid'], "TRSFIterator", int(0))
            r.hset(msg['Linkedid'], "FWDIterator", int(0))
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            r.hset(msg['Linkedid'], "Main_Status", 'NOANSWER')
            r.hset(msg['Linkedid'], "Main_Hold", 0)
            r.hset(msg['Linkedid'], "Up_duration", 0)
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)
            
            #print('WOOT!:221')
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['Linkedid'], "SourceChannel_starid", starsideid)

        if skipmark==3:

            rdefine=route_par(msg['CallerIDNum'], msg['Exten'])
            #print(rdefine)
            r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
            r.hset(msg['Linkedid'], "Route", rdefine)
            r.hset(msg['Linkedid'], "SourceChannel", msg['Channel'])
            #srcclone=msg['Channel']
            r.hset(msg['Linkedid'], "SourceChannel_state", msg['ChannelStateDesc'])
            if  len(msg['CallerIDNum']) == 10:
                r.hset(msg['Linkedid'], "CallerIDNum", "7"+msg['CallerIDNum'])
            else:
                r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "CallerIDName", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "Destination", msg['Exten'])
            r.hset(msg['Linkedid'], "Iterator", 0)
            r.hset(msg['Linkedid'], "QuIterator", int(0))
            r.hset(msg['Linkedid'], "IVRIterator", int(0))
            r.hset(msg['Linkedid'], "TRSFIterator", int(0))
            r.hset(msg['Linkedid'], "FWDIterator", int(0))
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            r.hset(msg['Linkedid'], "Main_Status", 'NOANSWER')
            r.hset(msg['Linkedid'], "Main_Hold", 0)
            r.hset(msg['Linkedid'], "Up_duration", 0)
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['Linkedid'], "SourceChannel_starid", starsideid)
            
            #print('WOOT!:221')


#################################################################################            
#            if len(channame) <= 4 and not channame in trunks:
#                starsideid=getsid(channame)
#                print('SourceChannel_Starid',starsideid)
#                print('WOOT! :225')
#                r.hset(msg['Linkedid'], "SourceChannel_Starid", starsideid)
#                print('WOOT!:227')

        if not r.hexists(msg['Linkedid'],"Orphan"):
            addtored=activechan(msg['Linkedid'],'add')
                #print(addtored,"--------354---------",r.hexists(msg['Linkedid'],"Orphan"))
                #print(msg['Linkedid'],"Orphan")
    #else:
    if r.hexists(msg['Linkedid'],"SourceChannel") and r.hexists(msg['Linkedid'],"Iterator"):
        skipmark=0
    #            pass


    #        temp=dict(temp)
    #        action = {'Action': 'Status', 'Channel' :  msg['Channel'], 'AllVariables' : 'true'}
       # action = {'Action': 'Ping'}
     #       print(r.hget(msg['Linkedid'],"Iterator"))
      #      temp = await mngr.send_action(action,as_list=False)
      #      temp=dict(temp)
      #      print(temp)
    #elif:
    #    iter = int(r.hget(msg['Linkedid'],"Iterator"))
    #    if re.search(r'originate', msg['Channel']):
    #        print("channel suppressed")
    
        if re.search(r'originate', msg['Channel']):
            #print('Originate suppresed false')
 #       print(msg)
#            itera = int(r.hget(msg['Linkedid'],"Iterator"))
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)
            temp=dict(temp)
            skipmark=1
        #print(temp)
        if re.search(r'trsf', msg['Channel']):
     #   print("channel suppressed")
     #   print("150")
            #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Skip this f channel'+msg['Channel']+' from active !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            skipmark=1

        if re.search(r'forwards', msg['Channel']):
      #  print("channel suppressed")
      #  print("150")
            pass
            skipmark=1

        if re.search(r'phone_forwards', msg['Channel']):
            skipmark=1
            #print("channel suppressed")

        if r.hget(msg['Linkedid'],"SourceChannel") == msg['Channel']:
            skipmark=1

#        else:
    
#    elif msg['Channel'] == srcclone:
#        pass
#        print('fwd channel '+r.hget(msg['Linkedid'],"Iterator"))
  #          print(msg)
#            r.hset(msg['Linkedid'], "transfer_chan_"+str(iter),msg['Exten'])
#            r.hset(msg['Linkedid'], "transfer_chan_"+str(iter)+"_state",msg['ChannelStateDesc'])
         #   iter = iter+1
#        else:
#            if re.search(r'originate', msg['Channel']):
                #print('suppresed')
#        else:
#        print(msg['Channel'].split('-'))
        if skipmark==0:
            spchan=msg['Channel'].split('-')
            spchan=spchan[0]
#        print(spchan)
 #       if 
            itera = int(r.hget(msg['Linkedid'],"Iterator"))
            itera = itera+1
#        print(itera)
            #print('second chan!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!',msg['Channel'],'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            r.hset(msg['Linkedid'], "Iterator", itera)
            #print("DstChannel_"+str(i))
            r.hset(msg['Linkedid'], "DstChannel_"+str(itera), msg['Channel'])
            r.hset(msg['Linkedid'], "DstChannel_"+str(itera)+"_state", msg['ChannelStateDesc'])
#            r.hset(msg['Linkedid'],"DstChannel_"+str(itera)+"_dstate", msg['ChannelState'])
            r.hset(msg['Linkedid'], "DstChannel_"+str(itera)+"_Uid", msg['Uniqueid'])
            r.hset(msg['Linkedid'], "DstChannel_"+str(itera)+"_start", msg['Timestamp'])
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['Linkedid'], "DstChannel_"+str(itera)+"_starid", starsideid)
#        print(msg)
################################################################################################   
#        if len(channame) <= 4 and not channame in trunks:
#            starsideid=getsid(channame)
#            print(starsideid,'286')
#            r.hset(msg['Linkedid'], "DstChannel_"+str(itera)+"_Starid", starsideid)
#        print(msg)
#        if len()
#        action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
#        temp = await mngr.send_action(action,as_list=False)
#        temp=dict(temp)
#        print(temp)

        
#                print('second channel')
#                print("DstChannel_"+str(i))
#                r.hset(msg['Linkedid'], "DstChannel_"+str(i), msg['Channel'])

#            rdict=r.hgetall(msg['Linkedid'])
          #  if key = ''
#                print (key)
            #if not r.hexists(msg['Linkedid'],"DstChannel_"+str(i)):
            #print(i)
            #print("DstChannel_"+str(i))
            #r.hset(msg['Linkedid'], "DstChannel_"+str(i), msg['Channel'])
      #  if not r.hexists(msg['Linkedid'],"DstChannel"):
       #     print('Exists')
       #     r.hset(msg['Linkedid'], "DstChannel", msg['Channel'])
       # else:
      #      pass
   # i=2

    #if r.hexists(msg['Linkedid'],"DstChannel_"+str(i)):
    #    i=i+1
    #    r.hset(msg['Linkedid'], "DstChannel_"+str(i), msg['Channel'])
   # else:
   #     r.hexists(msg['Linkedid'],"DstChannel_"+str(i))


@manager.register_event('Hold')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value in redstring.items():
#        print(value,msg['Channel'])
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
 #           print('DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD')
 #           print('Newstate matched'+key,msg['ChannelStateDesc'])
            r.hset(msg['Linkedid'],key+"_state", 'Hold')
            if value == msg['Channel']  and not re.search(r'_agent_', key):
                r.hset(msg['Linkedid'],key+"_hold", msg['Timestamp'])
##        if value == msg['Channel'] and re.search(r':channel', key):
##            agenhold=key.split(':')
##            print(agenhold)
##            agenhold.insert(3, 'hold')
##            print(':'.join(str(x) for x in agenhold))
##            aghold=':'.join(str(x) for x in agenhold)
##            r.hset(msg['Linkedid'],aghold,msg['Timestamp'])
#            for x in agenhold:
 #               strag+= ':' + x
  #          agenhold[3]='hold'
#            agenthold=str(agenthold)
   #         print('agenthold',agenhold,'agenthold')
  #          print=key)
 #   print(r.hgetall(msg['Linkedid']))

@manager.register_event('Unhold')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print('999999999999999999999999999999999999999',msg,'99999999999999999999999999999999999')
    delchan=0
    redstring=r.hgetall(msg['Linkedid'])
    holdtm=r.hget(msg['Linkedid'],"Main_Hold")
    for key,value in redstring.items():
#        print(value,msg['Channel'])
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
  #          print('DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD')
  #          print('Newstate matched'+key,msg['ChannelStateDesc'])
            delchan=key
            r.hset(msg['Linkedid'],key+"_state", msg['ChannelStateDesc'])
            if value == msg['Channel']  and not re.search(r'_agent_', key):
                r.hset(msg['Linkedid'],key+"_unhold", msg['Timestamp'])
            if not re.search(r'_hold', key):
                r.hset(msg['Linkedid'],key+"_hold", msg['Timestamp'])
    #redstring=r.hgetall(msg['Linkedid'])
    #for key,value in redstring.items():

#    redstring=r.hgetall(msg['Linkedid'])
#    for key,value in redstring.items():
#        if re.search(r'_hold', key):
        if  re.search(r'_hold', key):
            #print(key,value,float(holdtm)+float(msg['Timestamp'])-float(value))
            r.hset(msg['Linkedid'],"Main_Hold", float(holdtm)+float(msg['Timestamp'])-float(value))
            r.hdel(msg['Linkedid'],key)

    redstring=r.hgetall(msg['Linkedid'])
    for key,value in redstring.items():
        if  re.search(r'_unhold', key) and re.search(delchan, key):
            r.hdel(msg['Linkedid'],key)

            #print(key,value,'???????????????????????????????????????????????????????')
#        print(value,msg['Channel'])

#            r.hdel(msg['Linkedid'],key+'_unhold')
#        if re.search(r'_unhold', key):
 #           r.hdel(msg['Linkedid'],key)
#        if not re.search(r'Main_hold', key)  and re.search(r'_hold', key):
#            print(value)
#            r.hset(msg['Linkedid'],"Main_hold", float(msg['Timestamp'])-float(value))

    #print(r.hgetall(msg['Linkedid']))
##        if value == msg['Channel'] and re.search(r':channel', key):
##            agenhold=key.split(':')
##            print(agenhold)
##            agenhold.insert(3, 'hold')
##            print(':'.join(str(x) for x in agenhold))
##            aghold=':'.join(str(x) for x in agenhold)
##            lasthold=redstring[aghold]
##            r.hset(msg['Linkedid'],aghold,float(lasthold)-msg['Timestamp'])


@manager.register_event('Status')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
  #  print(msg['ConnectedLineNum'])
    if msg['Exten'] == "s":
  #      print("set dest "+msg['ConnectedLineNum'])
        speervariables=msg['Variable']
        varindex = [i for i, x in enumerate(speervariables) if re.search(r'DIALEDPEER', x)]
        if varindex:
            dialedpeer=speervariables[varindex[0]].split('=')[1]
            if dialedpeer:
                r.hset(msg['Linkedid'],"Destination",msg['ConnectedLineNum'])
                r.hset(msg['Linkedid'],"CallerIDNum",msg['ConnectedLineNum'])
   #             print(msg['Variable'])
                svariables=msg['Variable']
                varindex = [i for i, x in enumerate(svariables) if re.search(r'TRUECID', x)]
                if varindex:
                    truedest=svariables[varindex[0]].split('=')[1]
                    r.hset(msg['Linkedid'],"Destination",truedest)
        else:
            svariables=msg['Variable']
            returned = [i for i, x in enumerate(speervariables) if re.search(r'TRUELINKED', x)]
            if returned:


    #            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    #            print(returned)
                truedest=svariables[returned[0]].split('=')[1]
                redstring=r.hgetall(truedest)
                r.hset(msg['Linkedid'],"Destination",redstring["Destination"])
                
                itera = int(r.hget(truedest,"Iterator"))
                itera = itera+1
  #              print(itera)
                #print('second chan')
                r.hset(truedest, "Iterator", itera)
            #print("DstChannel_"+str(i))
                r.hset(truedest, "DstChannel_"+str(itera), msg['Channel'])
                r.hset(truedest, "DstChannel_"+str(itera)+"_state", msg['ChannelStateDesc'])
#                r.hset(truedest, "DstChannel_"+str(itera)+"_dstate", msg['ChannelState'])
                r.hset(truedest, "DstChannel_"+str(itera)+"_Uid", msg['Uniqueid'])
                channame=splitchan(msg['Channel'])
                starsideid=getsid(channame)
                r.hset(truedest, "DstChannel_"+str(itera)+"_starid", starsideid)
                if float(msg['Linkedid']) > float(truedest):
                #if not msg['Linkedid'] < truedest:
                #if  msg['Linkedid'] < truedest:
                   # print('??????????????????????????????????????????????????????????????????????')
                   # print(msg['Linkedid'],'+',truedest)
                   # print('??????????????????????????????????????????????????????????????????????')
                    r.hset(msg['Linkedid'], "Parent_ID_"+str(itera), truedest)
                    r.hset(truedest, "Orphan_ID_"+str(itera), msg['Linkedid'])
  #                  print('Orphan detected')

#        print(msg)

   #             print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

#            if re.search(r'originate', msg['Channel']):
 #               print('-----------------------------------------------------------------------------------------------CallThrough------------------------------------------------------------------')
#                r.hset(msg['Linkedid'], "CallThrough", msg['CallerIDNum'])
#                action = {'Action': 'Getvar', 'EXTERNAL' :}
           # varindex = [i for i, x in enumerate(svariables) if re.search(r'AGENTEXTERNAL', x)]
           # if varindex:
           #     truedest=svariables[varindex[0]].split('=')[1]
           #     r.hset(msg['Linkedid'],"CallThrough",truedest)

  #      print(varindex[0])
  #      print(svariables[varindex[0]])
  #      print(truedest)

       #     else:

#    print(msg)
#    str1=str(msg).split( )
#    print(str1)
#    print(str1[3],str1[13])
@manager.register_event('AttendedTransfer')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print("******************************************************************")
    #print(msg)
#    print("******************************************************************")
#    iter = int(r.hget(msg['OrigTransfererLinkedid'],"Iterator"))
#   r.hset(msg['OrigTransfererLinkedid'], "transfer_chan_"+str(iter),msg['OrigTransfererExten'])
#    print(msg['CallerIDNum'],msg['ConnectedLineNum'], msg['Exten'])
    redstring=r.hgetall(msg['TransfereeLinkedid'])
 #   titer=r.hget(msg['TransfereeLinkedid'], "TranferI")
    for key,value in redstring.items():
        if value == msg['OrigTransfererChannel']:
            for key,value in redstring.items():
                if re.search(r'_hold', key):
                    holdtm=redstring["Main_Hold"]
                    #print('Transferer hold '+key)
                    #print(key,value,float(holdtm)+float(msg['Timestamp'])-float(value))
                    r.hset(msg['TransfereeLinkedid'],"Main_Hold", float(holdtm)+float(msg['Timestamp'])-float(value))

            #r.hset(msg['TransfereeLinkedid'],key+"_trsf_result_"+str(titer), msg['DestType'])
#    if "TRANSFERED" in msg.keys():
@manager.register_event('Pickup')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
 #   print("Pickup")
    #print(msg)
 #   print("Pickup")
    r.hset(msg['Linkedid'],"Orphan", "1")
    r.hset(msg['Linkedid'],"Pickup","Pickup")
    r.hset(msg['Linkedid'],"PickupChannel",msg['TargetChannel'])
    r.hset(msg['Linkedid'],"PickupLinkedid",msg['TargetLinkedid'])
    r.hset(msg['Linkedid'],"PickupConnectedCli",msg['TargetConnectedLineNum'])
    r.hset(msg['TargetLinkedid'],"Pickup","Pickup")

    truedest=r.hget(msg['TargetLinkedid'],"Linkedid")
    #itera = int(r.hget(msg['TargetLinkedid'],"Iterator"))
    #r.hset(msg['Linkedid'], "Parent_ID_"+str(itera), truedest)
    redstring=r.hgetall(msg['TargetLinkedid'])
    itera = int(redstring["Iterator"])
    for key,value in redstring.items():
        if value == msg['TargetChannel'] and not re.search(r'_agent_', key) and not re.search(r'_pickup', key)  and not  re.search(r'Qu_\d+:\d+:\d+:channel', key) and not  re.search(r'Qu_\d+:\d+:None:channel', key):
            #print("==========================================================",value,key,truedest,"========================================================")

            r.hset(msg['TargetLinkedid'],key+"_pickup", msg['Channel'])
            r.hset(msg['TargetLinkedid'],key+"_PickupLinkeidid",msg['Linkedid'])

            itera = itera+1
#            itera = itera
            r.hset(msg['TargetLinkedid'], "Orpan_ID_"+str(itera), truedest)
            #r.hset(msg['TargetLinkedid'], "Parent_ID_"+str(itera), truedest)
            r.hset(msg['Linkedid'], "Parent_ID_"+str(itera), msg['TargetLinkedid'])
            r.hset(msg['TargetLinkedid'], "Iterator", itera)
            r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera), msg['Channel'])
            r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+"_state", msg['ChannelStateDesc'])
#            r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+"_dstate", msg['ChannelState'])
            r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+"_Uid", msg['Uniqueid'])
           # r.hset(msg['Linkedid'], "DstChannel_"+str(itera)+"_start", msg['Timestamp'])
            r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+"_start", msg['Timestamp'])
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+"_starid", starsideid)
        if value == msg['TargetChannel'] and re.search(r'Qu_\d+:\d+:\d+|None:channel+$', key):
            #print("==========================================================",value,key," release========================================================")
            agefow=key.split(':')
            agefow.insert(3, 'release')
            agefow.pop(4)
            agefow=':'.join(str(x) for x in agefow)
            #print("==========================================================",value,key,agefow," release========================================================")
            r.hset(msg['TargetLinkedid'],agefow,msg['Timestamp'])

            
            channame=splitchan(msg['Channel'])
            channum=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            qit=str(redstring['QuIterator'])
            qit=(str(qit))
            qname='Qu_'+qit
            #print(msg['TargetLinkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])
            r.hset(msg['TargetLinkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])
           # r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+":start", msg['Timestamp'])


   # print(starsideid,channame)

#            agefow=key.split(':')
#            agefow.insert(3, 'release')
#            agefow.pop(3)
#            agefow=':'.join(str(x) for x in agefow)
#            r.hset(msg['TargetLinkedid'], msg['Timestamp'])



#        if value  == msg['TargetChannel'] and 
####            if msg['ChannelStateDesc'] == "Up":
####                r.hset(msg['TargetLinkedid'], "DstChannel_"+str(itera)+"_Ans_Time",msg['Timestamp'])
                                      #print("DstChannel_"+str(i))

#@manager.register_event('NewConnectedLine')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    msg = dict(msg)
#    print(msg)


@manager.register_event('OriginateResponse')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
#    print(msg['Exten'])
    #print(r.hget(msg['Uniqueid'],"Destination"))
    if r.hget(msg['Uniqueid'],"Destination") == "s":
        #print("set dest")
        r.hset(msg['Uniqueid'],"Destination",msg['Exten'])
    #iter = int(r.hget(msg['TransfererLinkedid'],"Iterator"))
            #    print(msg['CallerIDNum'],msg['ConnectedLineNum'], msg['Exten'])
    #print(msg)

@manager.register_event('Newstate')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
 #   print("------------------------------------------------------000000000000000000000000000000000000000000000000000000000000000--------------------------------------------------")
 #   print(msg)
 #   print(msg['Channel'])
    redstring=r.hgetall(msg['Linkedid'])
    channame=splitchan(msg['Channel'])
 #   print('000000000000000000000000000000000000000000000000000000'+channame)
  
    
    if 'Up_time' not in redstring and channame in trunks:
        r.hset(msg['Linkedid'],"Bill_time", msg['Timestamp'])
    elif channame in trunks:
        r.hset(msg['Linkedid'],"Bill_time", msg['Timestamp'])

    for key,value in redstring.items():
#        print(value,msg['Channel'])
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
 #           print('Newstate matched'+key,msg['ChannelStateDesc'])
  #          print(key)
            r.hset(msg['Linkedid'],key+"_state", msg['ChannelStateDesc'])
#            r.hset(msg['Linkedid'],key+"_dstate", msg['ChannelState'])
            #if key == 'SourceChannel' and msg['ChannelStateDesc'] == "Up":
####            if re.search(r'DstChannel', key) and  msg['ChannelStateDesc'] == "Up":
####                r.hset(msg['Linkedid'],key+"_Ans_Time",msg['Timestamp'])
  #          print(key)
#####            if redstring['SourceChannel_state'] == "Up" and msg['ChannelStateDesc'] == "Up":
#                print(msg)
#####                pass
 #               print("Uptime stored >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
 #               print(msg['Timestamp'])
#####                r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
####                r.hset(msg['Linkedid'],key+"_Ans_Time",msg['Timestamp'])
#####            elif key == 'SourceChannel' and msg['ChannelStateDesc'] == "Up":
  #              print("Inbound uptime ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
####                r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
         
        if re.search(r'Parent_', key):
        #if re.search(r'Orphan_ID_', key):
   #         print('???????????????????????????????????????????????????????????????????????????')
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
              #  print('???????????????????????????????????????????????????????????????????????????')
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
    #                print('???????????????????????????????????????????????????????????????????????????')
   #                 print(value,key)
    #                print('Newstate matched'+key,msg['ChannelStateDesc'])
  #          print(key)
                    r.hset(parentid,key+"_state", msg['ChannelStateDesc'])
#                    r.hset(parentid,key+"_dstate", msg['ChannelState'])
####                    if msg['ChannelStateDesc'] == "Up":
####                        r.hset(parentid,key+"_Ans_Time",msg['Timestamp'])

#        if re.search(r'Parent_', key):
        if re.search(r'Orphan_ID_', key):
   #         print('???????????????????????????????????????????????????????????????????????????')
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
              #  print('???????????????????????????????????????????????????????????????????????????')
                if value == msg['Channel'] and not  re.search(r'Qu_\d+_answer_chan', key):
                    r.hset(parentid,key+"_state", msg['ChannelStateDesc'])
#                    r.hset(msg['Linkedid'],key+"_dstate", msg['ChannelState'])
####                    if msg['ChannelStateDesc'] == "Up":
####                        r.hset(parentid,key+"_Ans_Time",msg['Timestamp'])
    #                print('???????????????????????????????????????????????????????????????????????????')
   #                 print(value,key)
            #if key == 'SourceChannel' and msg['ChannelStateDesc'] == "Up":
  #          print(key)
             #       if orfstring['SourceChannel_state'] == "Up" and msg['ChannelStateDesc'] == "Up":
             #           print(msg)
             #           print("Uptime stored >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
             #           print(msg['Timestamp'])
             #           r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
             #       elif key == 'SourceChannel' and msg['ChannelStateDesc'] == "Up":
             #           print("Inbound uptime ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
             #           r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
         







#    redstring=r.hgetall(msg['Linkedid'])
#    for key,value  in redstring.items():
#        print(key,value)
            
#    print(r.hgetall(msg['Linkedid']))

   #         print(r.hgetall(msg['Linkedid']))
        #r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
    #iter = int(r.hget(msg['TransfererLinkedid'],"Iterator"))
            #    print(msg['CallerIDNum'],msg['ConnectedLineNum'], msg['Exten'])
#@manager.register_event('NewConnectedLine')  # Register all events
#async def ami_callback(event, manager):
#    msg = dict(manager)
#    print(msg['CallerIDNum'],msg['ConnectedLineNum'], msg['Exten'])
#    print(msg)

@manager.register_event('VarSet')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)

    if msg['Variable'] == 'DIALSTATUS' and msg.get('Value'):
    #    print(msg['Variable'], msg.get('Value'))
    #    print(msg.get('Exten'))
        print(msg)
        if not r.hexists(msg['Linkedid'],"SourceChannel") and not r.hexists(msg['Linkedid'],"Iterator"):
            r.hset(msg['Linkedid'], "Iterator", 0)
#            r.hset(msg['Linkedid'], "Iterator", 0)
            rdefine=route_par(msg['CallerIDNum'], msg['Exten'])
            r.hset(msg['Linkedid'], "Route", rdefine)
            if  len(msg['CallerIDNum']) == 10:
                r.hset(msg['Linkedid'], "CallerIDNum", "7"+msg['CallerIDNum'])
            else:
                r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "CallerIDName", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "Destination", msg['Exten'])
            r.hset(msg['Linkedid'],"Main_duration", 0)
          #  r.hset(msg['Linkedid'],"Up_duration", 0)
            r.hset(msg['Linkedid'],"Bill_duration", 0)
#         if not r.hexists(msg['Linkedid'],"DstChannel") and not r.hexists(msg['Linkedid'],"Iterator"):
        redstring=r.hgetall(msg['Linkedid'])
        itera=redstring['Iterator']
        tritera = int(redstring["TRSFIterator"])
        for key,value  in redstring.items():
#            print('tr',key,value)
            if value == msg['Channel'] and not  re.search(r'_agent_', key) and not  re.search(r'_transfer_', key) and not  re.search(r'Qu_\d+_answer_', key) and not re.search(r'Qu_\d+:\d+:.+:.+', key):
                #print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@','Dialstatus found'+key,msg,itera,'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                r.hset(msg['Linkedid'],key+"_DStatus", msg['Value'])

            if re.search(r'_transfer_'+str(tritera), key) and not re.search(r'_DStatus', key) and not  re.search(r'Qu_\d+_answer_', key) and not re.search(r'Qu_\d+:\d+:.+:.+', key):
                #print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@**********************','Dialstatus transfer found',key,itera,'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                r.hset(msg['Linkedid'],key+"_DStatus", msg['Value'])

            if re.search(r'Qu_\d+:\d+:.+:transfer_'+str(tritera), key):
                print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@**********************','Dialstatus transfer found',key,itera,'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                tritera = int(redstring["TRSFIterator"])
            
                agefow=key.split(':')
                agefow.insert(3, 'transferstatus_'+str(tritera))
                agefow.pop(4)
                agefow=':'.join(str(x) for x in agefow)
#                splochan=msg['Channel'].split(";")[0]
                r.hset(msg['Linkedid'],agefow,msg['Value'])


        ##r.hset(msg['Linkedid'],"Dialstatus_"+r.hget(msg['Linkedid'],"Iterator"), msg['Value'])
        ##if len(msg['Exten']) >= 4 and not  msg['Exten'] == '<unknown>' and not msg['Context'] == 'trsf':
            #if not len(msg['CallerIDNum']) >= 4:
         ##   spchan=splitchan(msg['Channel'])

            #if not r.hexists(msg['Linkedid'],"CallThrough") and spchan in trunks:
            #if spchan in trunks:
 #           if re.search(r'originate', msg['Channel']):
 #               print('-----------------------------------------------------------------------------------------------CallThrough------------------------------------------------------------------')
#;                r.hset(msg['Linkedid'], "CallThrough", msg['CallerIDNum'])
#                action = {'Action': 'Getvar', 'EXTERNAL' :}
#                temp = await mngr.send_action(action,as_list=False)


    if msg['Variable'] in ivrvars:
        ivrit=int(r.hget(msg['Linkedid'], "IVRIterator"))+1
        r.hset(msg['Linkedid'], "IVRIterator", ivrit)
        ivrit=(str(ivrit))
        if not msg.get('Value'):
            r.hset(msg['Linkedid'],"IVR_digits_"+ivrit, "None")
        else:
            r.hset(msg['Linkedid'],"IVR_digits_"+ivrit, msg['Value'])

    if msg['Variable'] == 'EMPTYQUEUE':
        qit=int(r.hget(msg['Linkedid'], "QuIterator"))+1
        qit=(str(qit))
        r.hset(msg['Linkedid'], "QuIterator", qit)
        r.hset(msg['Linkedid'], "Qu_"+qit, msg['Value'])
        r.hset(msg['Linkedid'], "Qu_"+qit+"_status", 'Empty')
#        r.hset(msg['Linkedid'], "Qu_Empty"+qit, msg['Value'])
    if msg['Variable'] == 'tempnum':
        r.hset(msg['Linkedid'], "CallThrough", msg['Value'])
  
    if msg['Variable'] == 'forwarddest':
        print(msg)
#                        titera = int(redstring["Iterator"])
        itera=int(r.hget(msg['Linkedid'], "Iterator"))
        fitera=int(r.hget(msg['Linkedid'], "FWDIterator"))
        #r.hset(msg['Linkedid'], "FWD_"+str(itera), msg['Value'])
        r.hset(msg['Linkedid'], "FWDIterator", fitera+1)
        print('!!!!!!!!!!!!!!!!!',fitera,'!!!!!!!!!!!!!!!!!!!!!!!')
        redstring=r.hgetall(msg['Linkedid'])
        #print('Forward found')
        #print(msg)
        r.hset(msg['Linkedid'],"DstChannel_"+str(itera)+"_fwd_"+str(int(fitera)+1), msg['Value'])

    if msg['Variable'] == 'fwder1':
        print('00000000000000000',msg['Value'],msg,'00000000000000000000')
        redstring=r.hgetall(msg['Linkedid'])
        for key,value  in redstring.items():
            if value == msg['Value'] and re.search(r':channel', key):
                agefow=key.split(':')
                agefow.insert(3, 'fwd')
                agefow.pop(4)
                agefow=':'.join(str(x) for x in agefow)
                splochan=msg['Channel'].split(";")[0]
                r.hset(msg['Linkedid'],agefow,splochan)
    
    if msg['Variable'] == 'RATING':
        r.hset(msg['Linkedid'],'Rating',msg['Value'])
#    if msg['Variable'] == 'fwder2':
#        print('00000000000000000',msg['Value'],msg,'00000000000000000000')



#    if msg['Variable'] == 'S_USIDEID':
#        print(msg['Variable'], msg.get('Value'))
#        r.hset(msg['Linkedid'],"Src_USID", msg['Value'])
 
#    if msg['Variable'] == 'D_USIDEID':
#        print(msg['Variable'], msg.get('Value'))
#        r.hset(msg['Linkedid'],"DstChannel_"+r.hget(msg['Linkedid'],"Iterator")+"_USID", msg['Value'])
    
    if msg['Variable'] == 'FORWARD':
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!',msg)
     #   print(msg['Channel'], msg.get('Value'),  msg.get('CallerIDNum'))
        if not re.search(r'Local', msg['Channel']):
            fitera=int(r.hget(msg['Linkedid'], "FWDIterator"))
            
       # r.hset(msg['Linkedid'], "FWD_"+str(itera), msg['Value'])
#            if not re.search(r'forwards-',msg['Channel']):
#            Local/79110840162@forwards
#                r.hset(msg['Linkedid'], "FWDIterator", fitera+1)
#                print('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFWWWWWWWWDDDDDDDD')
            fitera=fitera+1
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!',fitera)
            r.hset(msg['Linkedid'], "FWDIterator", fitera)
            redstring=r.hgetall(msg['Linkedid'])
            for key,value  in redstring.items():
                if value == msg['Channel']:
                    r.hset(msg['Linkedid'],key+"_fwd_"+str(fitera), msg['CallerIDNum'])
#                    fitera=fitera+1
     #           print('Forward found')
                #titer=0
                    #for transf in redstring:
                   # if re.search(r'%s_fwd_' % key, transf):
                   #     titer=titer+1
                   #     print("fwd count ="+str(titer))
               # if not titer:
               #     titer=0
               # titer=titer+1
#                r.hset(msg['Linkedid'],key+"_fwd_", msg['CallerIDNum'])
               # r.hset(msg['Linkedid'],key+"_fwd_"+str(fitera), msg['CallerIDNum'])
    #    print(msg)
#    if msg['Variable'] == 'QUEUEPOSITION':
#        qit=int(r.hget(msg['Linkedid'], "QuIterator"))
#        qit=(str(qit))
#        print('3333333333333333333333333333333333',msg,qit,'33333333333333333333333333333333333333333333333')
#        r.hset(msg['Linkedid'], "Qu_"+qit+"_position", msg['Value'])
        



@manager.register_event('QueueCallerLeave')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
#    if 
    msg = dict(msg)
    #print(msg)
    qit=int(r.hget(msg['Linkedid'], "QuIterator"))
    qit=(str(qit))

#    r.hset(msg['Linkedid'], "QuIterator", qit)
    r.hset(msg['Linkedid'], "Qu_"+qit, msg['Queue'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_position", msg['Position'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_leave", msg['Timestamp'])

@manager.register_event('QueueCallerJoin')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
  #  for key,value  in redstring.items():
  #      if not "QuName" in key:
  #          print('first queue')
  #          r.incr(msg['Linkedid'], "QuIterator")
  #          print("QuIterator")
  #          print(r.hget(msg['Linkedid'], "QuIterator"))
  #      else:
  #          pass


    qit=int(r.hget(msg['Linkedid'], "QuIterator"))+1
    qit=(str(qit))

    r.hset(msg['Linkedid'], "QuIterator", qit)
    r.hset(msg['Linkedid'], "Qu_"+qit, msg['Queue'])
   # r.hset(msg['Linkedid'], "Qu_"+qit+"_position", msg['Position'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_init", msg['Position'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_count", msg['Count'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_join", msg['Timestamp'])
#    print(msg)
    #if msg['Variable'] == 'DIALSTATUS' and msg.get('Value'):


@manager.register_event('QueueCallerAbandon')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
        if msg['Queue'] in value and re.search(r'Qu_\d+$', key):
           # print('Adandon queue matched')
           # print(key, value)
            r.hset(msg['Linkedid'], key+"_status", "Abandon")
            r.hset(msg['Linkedid'], key+"_wait", msg['HoldTime'])



@manager.register_event('AgentCalled')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #opagent=(msg['MemberName'])
    #opagent=(opagent.replace(" ", ""))
    #opagent=(opagent.lower())
#    print(msg)
    redstring=r.hgetall(msg['Linkedid'])
    channame=splitchan(msg['DestChannel'])
    channum=splitchan(msg['DestChannel'])
    starsideid=getsid(channame)
    qit=str(redstring['QuIterator'])
    qit=(str(qit))


    print(starsideid,channame)
    for key,value  in redstring.items():
        if msg['Queue'] in value and re.search(r'Qu_'+qit, key):
            #print('Queue matched',key, value)
            #print(key, value)
            qname=key
            if not r.hexists(msg['Linkedid'],qname+':'+channum+':'+str(starsideid)+':start'):
             #   print('+++++++++++++++++++++',qname+':'+str(starsideid)+':start','++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])
 #               r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':noanswer',float(0))
            else:
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])

            for key,value  in redstring.items():
                #if msg['Interface'] in value and not re.search(r'_agent_chan_', key):
                if msg['DestChannel'] in value and not re.search(r'_agent_', key):
##                if msg['Interface'] in value and not re.search(r'_agent_', key):
#                    print('Agent matched'+msg['Interface'],value)
#                    print(key, value)
                    #r.hset(msg['Linkedid'], key+"_agent_"+qname, msg['MemberName'])
                  #  r.hset(msg['Linkedid'], key+"_agent_"+qname+"_qname", msg['Queue'])
                    r.hset(msg['Linkedid'], key+"_agent_"+qname+"_start", msg['Timestamp'])
                    r.hset(msg['Linkedid'], key+"_agent_"+qname+"_Starid", starsideid)
                    r.hset(msg['Linkedid'], key+"_agent_chan_"+qname, msg['DestChannel'])
#                    r.hset(msg['Linkedid'], key+"_agent_chan_"+qname, msg['DestChannel'])
#                    if not r.hexists((msg['Linkedid'], )
 #                   r.hset(msg['Linkedid'], key+"_agent_chan_"+qname, msg['DestChannel'])
            
           #     r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
           #     r.hset(msg['Linkedid'],key+"_state", "Hangup")
           #     r.hset(msg['Linkedid'],key+"_cause",msg['Cause'])
    

#    print(opagent,msg['Interface'],msg['Queue'])
@manager.register_event('AgentConnect')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
    channum=splitchan(msg['DestChannel'])
    if  re.search(r'phone_forward', channum):
        channum=splitlocal(channum)
        
    starsideid=getsid(channum)
    redstring=r.hgetall(msg['Linkedid'])
    qit=str(redstring['QuIterator'])
    qit=(str(qit))
    for key,value  in redstring.items():
        if msg['Queue'] in value and re.search(r'Qu_'+qit, key) and not re.search(r'_answer_chan'+qit, key):
            qname=key
            r.hset(msg['Linkedid'], key+"_wait", msg['HoldTime'])
            r.hset(msg['Linkedid'], key+"_status", 'Answered')
           #$ r.hset(msg['Linkedid'], key+"_answer_time", msg['Timestamp'])
            r.hset(msg['Linkedid'], key+"_answer_chan", msg['DestChannel'])
            r.hset(msg['Linkedid'], key+"_answer_time", msg['Timestamp'])
            if not re.search(r'phone_forward', msg['DestChannel']):
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':answer',msg['Timestamp'])
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])
#            r.hdel(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':noanswer')
            #print( qname+':'+channum+':'+str(starsideid)+':channel','~~~~~~~~~~~~~!!!!!!!!!!!!!~~~~~~~~~~~~~~~~')
        if re.search(r':fwd', key) and  re.search(value, msg['DestChannel']):
           # print(key,value,'111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111')
            agefow=key.split(':')
#                agefow.insert(3, 'answer')
            agefow.pop(3)
            agefow=':'.join(str(x) for x in agefow)
            r.hset(msg['Linkedid'], agefow+':answer',msg['Timestamp'])
#            r.hset(msg['Linkedid'], agefow+':lastchannel',msg['DestChannel'])

            r.hdel(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':noanswer')
        

@manager.register_event('AgentRingNoAnswer')
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    redstring=r.hgetall(msg['Linkedid'])
    channame=splitchan(msg['DestChannel'])
    channum=splitchan(msg['DestChannel'])
    starsideid=getsid(channame)
    qit=str(redstring['QuIterator'])
    qit=(str(qit))


    #print(starsideid,channame)
    for key,value  in redstring.items():
        if msg['Queue'] in value and re.search(r'Qu_'+qit, key):
#            print('Queue matched',key, value)
#            print(key, value)
            qname=key
            if not r.hexists(msg['Linkedid'],qname+':'+channum+':'+str(starsideid)+':start'):
                print(blah)
#                if 
 #               r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])
#    if msg['Queue'] in value and re.search(r'Qu_'+qit, key):
#        print('Queue matched',key, value)
#        print(key, value)
#        qname=key
#        if not r.hexists(msg['Linkedid'],qname+':'+channum+':'+str(starsideid)+':start'):
#            print('+++++++++++++++++++++',qname+':'+str(starsideid)+':start','++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
#        r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])
#        r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])
#        r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':noanswer',float(0))
#@manager.register_event('LocalOptimizationBegin')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    msg = dict(msg)
#    print(msg)

#@manager.register_event('LocalOptimizationEnd')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    msg = dict(msg)
#    print(msg)

@manager.register_event('AgentComplete')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    redstring=r.hgetall(msg['Linkedid'])
    qit=str(redstring['QuIterator'])
    qit=(str(qit))
    channum=splitchan(msg['DestChannel'])
    starsideid=getsid(channum)
    for key,value  in redstring.items():
        #if msg['Queue'] in value and re.search(r'Qu_'+qit, key) and not re.search(r':channel', key): 
        if msg['Queue'] in value and re.search(r'Qu_'+qit, key): 
            qname=key
#            qname=key
#            r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':reason',msg['Reason'])
#            r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':status','Answered')
            #print('Queue matched')
            #print(key, value)
            r.hset(msg['Linkedid'], key+"_wait", msg['HoldTime'])
            r.hset(msg['Linkedid'], key+"_talk", msg['TalkTime'])
            r.hset(msg['Linkedid'], key+"_release", msg['Reason'])
            r.hset(msg['Linkedid'], key+"_status", 'Answered')
##                r.hset(msg['Linkedid'],agefow+':release',msg['Timestamp'])


            for key,value  in redstring.items():
                if msg['DestChannel'] in value and msg['DestChannel'] == value and not re.search(r'_pickup', key) and not  re.search(r'_agent', key) and not  re.search(r'_answer_chan', key) and not  re.search(r'Qu_\d+:\d+:\d+:channel', key) and not  re.search(r'Qu_\d+:\d+:None:channel', key): #Qu_1:7007:None:channel
                #if msg['Interface'] in value and msg['DestChannel'] == msg['']:
                   # print('Agent matched ________________________________________________________________________________'+key,value)
                    #print(key, value)
#                    r.hset(msg['Linkedid'],key+"_state", msg['DestChannelStateDesc'])
                    #r.hset(msg['Linkedid'], key+"_agent_ans", msg['MemberName'])
                    r.hset(msg['Linkedid'], key+"_agent_ans", starsideid)
                    #r.hset(msg['Linkedid'], key+"_channel_ans", msg['DestChannel'])
                    r.hset(msg['Linkedid'], key+"_channel_ans", msg['DestChannel'])
                    r.hset(msg['Linkedid'], key+"_status", "Answered")

    for key,value  in redstring.items():
       # print(key,value,'+')
        if re.search(r'Qu_'+qit+':.+:answer', key):
            agefow=key.split(':')
   #             agefow.insert(3, 'fwd')
            agefow.pop(3)
            agefow=':'.join(str(x) for x in agefow)
                
            r.hset(msg['Linkedid'],agefow+':release',msg['Timestamp'])
#        else:
#            r.hset(msg['Linkedid'],agefow+':release',msg['Timestamp'])
            #r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':release',msg['Timestamp'])
            #r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':release',msg['Timestamp'])

            
           #     r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
           #     r.hset(msg['Linkedid'],key+"_state", "Hangup")

@manager.register_event('AgentDump')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)


    #if msg['Variable'] == 'DIALSTATUS' and msg.get('Value'):
    #if msg['Variable'] == 'DIALSTATUS' and msg.get('Value'):
#
#
#        print(msg['Variable'], msg.get('Value'))
#        print(msg)

##@manager.register_event('Newexten')  # Register all events
##async def ami_callback(mngr: Manager, msg: Message):
 #   msg = dict(msg)
#    if msg['Application'] == 'Answer':
#        print(msg['Value'])
        #print('++++++++++++++++++++++++++++++++++++++++++++++++++++ans+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        #print(msg)

#    print(msg)

#    print(msg['CallerIDNum'],msg['ConnectedLineNum'], msg['Exten'])

#@manager.register_event('Cdr*')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    print('------------------------------------------BridgeEnter---------------------------------------------------------------------------------------------------')
#    print(msg)

#@manager.register_event('BridgeInfoChannel')  # Register all events
##@manager.register_event('BridgeEnter')  # Register all events
#async def ami_callback(event, manager):
##async def ami_callback(event, manager):
##    msg = dict(manager)
#    print('------------------------------------------BridgeInfoChannel--------------------------------------------------------------------------------------------------')
##    print(msg)

@manager.register_event('UserEvent')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
#    print('------------------------------------------RECORDING PATh---------------------------------------------------------------------------------------------------')
    msg = dict(msg)
    if "RECORDING" in msg.keys():
        r.hset(msg['Linkedid'],"CallRec" , msg['RECORDING'])

        #print(msg['RECORDING'])
    if "TRANSFER" in msg.keys():
        print(msg)
 #       print(msg['TRANSFER'])
        redstring=r.hgetall(msg['Linkedid'])
        titera = int(redstring["Iterator"])
        tritera = int(redstring["TRSFIterator"])+1
        r.hset(msg['Linkedid'], "TRSFIterator", tritera)
#        print('-==================================Transferer found========','==========================')
        for key,value  in redstring.items():
            if value == msg['TRANSFER'] and not re.search(r'_agent_',key) and not re.search(r'Qu_\d+_answer_chan',key) and not  re.search(r'Qu_\d+:\d+:.+:channel', key):
                r.hset(msg['Linkedid'],key+"_transfer_"+str(tritera), msg['Exten'])
            
            if value == msg['TRANSFER'] and re.search(r'Qu_\d+:\d+:.+:channel', key):
            
                agefow=key.split(':')
                agefow.insert(3, 'transfer_'+str(tritera))
                agefow.pop(4)
                agefow=':'.join(str(x) for x in agefow)
#                splochan=msg['Channel'].split(";")[0]
                r.hset(msg['Linkedid'],agefow,msg['Exten'])
    
 #               print('-==================================Transferer found========',key,value,'==========================')
#        for key,value  in redstring.items():
#                titer=0
#                titera = int(redstring["TRSFIterator"])
#                titera=titera+1
#                r.hset(msg['Linkedid'], "TRSFIterator", titera)
#                for transf in redstring:
#                    if re.search(r'%s_transfer_' % key, transf):
#                        titer=titer+1
#                        print("transfer count ="+str(titer))
#                      #  if not re.search(r'_agent_' % key):
#                     #   r.hset(msg['Linkedid'],key+"_transfer_"+str(titer), msg['Exten'])
#                if not titer:
#                    titer=0
#                    titer=titer+1
#                    #if not re.search(r'_agent_' % key):
#            if not re.search(r'_agent_' % key) and not re.search(r'Channel_'+titera % key):
#                r.hset(msg['Linkedid'],key+"_transfer_"+str(titer), msg['Exten'])
#            if re.search(r'Source' % key):
#                r.hset(msg['Linkedid'],key+"_transfer_"+str(titer), msg['Exten'])
              #  titera=titera+1
                #r.hset(msg['Linkedid'], "TRSFIterator", titera)

#    if "FORWARD" in msg.keys():
#        print(msg)
#        print(msg['FORWARD'])
#        redstring=r.hgetall(msg['Linkedid'])
#        for key,value  in redstring.items():
#            print(key,value)
#            if value == msg['FWDCHAN']:
#                print('Forward found '+msg['FWDCHAN'])
#                titer=0
#                for forward in redstring:
#                    if re.search(r'%s_forward_' % key, forward):
#                        titer=titer+1
#                        print("forward count ="+str(titer))
#                if not titer:
#                    titer=0
#                titer=titer+1
#3                r.hset(msg['Linkedid'],key+"_forward_"+str(titer), msg['Exten'])
              #  r.hset(msg['Linkedid'], "TranferI", int(titer))
                #else:
                #    r.hset(msg['Linkedid'],key+"_transfer_"+str(titer), msg['Exten'])

#    if "TRANSFERED" in msg.keys():
#        redstring=r.hgetall(msg['Linkedid'])
#        print(msg['TRANSFERED'])
#        for key in redstring.items():
@manager.register_event('BlindTransfer')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    redstring=r.hgetall(msg['TransfereeLinkedid'])
    titera = int(redstring["TRSFIterator"])
    titera=titera+1
    r.hset(msg['TransfereeLinkedid'], "TRSFIterator", titera)
    for key,value in redstring.items():
        if value == msg['TransfererChannel']:
            titer=0
            for transf in redstring:
                if re.search(r'%s_transfer_' % key, transf):
                    titer=titer+1
                    #print("transfer count ="+str(titer))
                    r.hset(msg['Linkedid'], "TRSFIterator", titer)
            if not titer:
                titer=0
            titer=titer+1
#            r.hset(msg['Linkedid'], "TRSFIterator", titer)
            #print('Transferer found '+key)
            r.hset(msg['TransfereeLinkedid'],key+"_transfer_"+str(titer), msg['Extension'])
            titera = int(redstring["Iterator"])
            titera=titera+1
            r.hset(msg['Linkedid'], "TRSFIterator", titera)
#    if "TRANSFERED" in msg.keys():
#        redstring=r.hgetall(msg['Linkedid'])
#    print(msg)
#    print(iter)
#    print(msg['Extension'])
#    r.hset(msg['TransfererLinkedid'], "transfer_chan_"+str(iter),msg['Extension'])

@manager.register_event('HangupRequest')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
 #   print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
 #   print(msg)
 #   print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
##    for key,value in r.hscan_iter(msg['Linkedid'], match='*Channel_*'):
  ##      print(key,value)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
#        print(key,value)
        #if value == msg['Channel'] and not re.search(r'_agent_chan_', key):
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key)  and not  re.search(r'_ans_', key):
                    r.hset(parentid,key+"_state", "Hangup")
                    r.hset(parentid,key+"_hangtime",msg['Timestamp'])
                    r.hset(parentid,key+"_cause",msg['Cause'])
                    #r.hset(truedest, "Orphan_ID_"+str(itera), msg['Linkedid'])
        if value == msg['Channel'] and  not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
#                print('matched')
#                print(key)
                r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
                r.hset(msg['Linkedid'],key+"_state", "Hangup")


#                r.hset(msg['Linkedid'],key+"_cause",msg['Cause'])
#    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#    print(msg['Channel'])
#    temp = await mngr.send_action(action,as_list=False)
#    temp=dict(temp)
#    print(temp)

@manager.register_event('SoftHangupRequest')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
#        print(key,value)
        #if value == msg['Channel'] and not re.search(r'_agent_chan_', key):
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_ans_', key):
                    r.hset(parentid,key+"_state", "Hangup")
                    r.hset(parentid,key+"_hangtime",msg['Timestamp'])
                    r.hset(parentid,key+"_cause",msg['Cause'])
                    #r.hset(truedest, "Orphan_ID_"+str(itera), msg['Linkedid'])
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
#                print('matched')
#                print(key)
                r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
                r.hset(msg['Linkedid'],key+"_state", "Hangup")
#                print('matched')
#    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#    print(dict(msg))
#    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#    action = {'Action': 'Status'}
#    temp = await mngr.send_action(action,as_list=False)
#    temp=dict(temp)
#    print(temp)
    

@manager.register_event('DeviceStateChange')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
##    msg = dict(msg)
    if not re.search(r'Queue',  msg['Device']) and not re.search(r'Custom',  msg['Device']) and not re.search(r'MWI',  msg['Device']):
        r.hset('Devmon', msg['Device'], msg['State'])
#        print(msg['Device'],msg['State'])



@manager.register_event('Hangup')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    #print('------------------------------------------Caller hanged------------------------------------------------------------------------------------------------')
    msg = dict(msg)
    hstate=[]
    #print('------------------------------------------Caller hanged------------------------------------------------------------------------------------------------')
    ##action = {'Action': 'Getvar', 'Channel' :  msg['Channel'] , 'Variable' : "DIALSTATUS"}
    #action = {'Action': 'Getvar', 'Variable' : "DIALSTATUS"}
   ## temp = await mngr.send_action(action,as_list=False)
   ## temp=dict(temp)
  ##  print(temp)
    redstring=r.hgetall(msg['Linkedid'])
#    print(msg)
#    print(msg['Cause'])
    #starttm=int(redstring['Linkedid'].split(".")[0])
   # starttm=flat(redstring[''])
    #stoptm=int(msg['Timestamp'].split(".")[0])
    stoptm=float(msg['Timestamp'])
    for key,value  in redstring.items():
        #print(key,value)
        #if value == msg['Channel'] and not re.search(r'_agent_chan_', key):
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                #print(key,value)
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_ans_', key):
                    r.hset(parentid,key+"_hangtime",msg['Timestamp'])
                    r.hset(parentid,key+"_cause",msg['Cause'])
                    r.hset(parentid,key+"_state",'Hangup')
                    #r.hset(truedest, "Orphan_ID_"+str(itera), msg['Linkedid'])

#        if re.search(r'DstChannel_\d+_transfer_\d+', key):
#                    if "Up_time" in redstring:
#                        if "Up_duration" in  redstring:
#                          #  uptm=float(redstring['Up_time'])
#                            uptmi=float(redstring['Up_time'])
#                            #print(redstring['Up_duration'])
#                            origup=float(redstring['Up_duration'])
#                            if float(stoptm-uptmi) < 0.001:
#                               print('not set')
#                            else:
#                              r.hset(msg['Linkedid'],"Up_duration",str((stoptm-uptmi)+origup))
#                              print('1111111111111111111transferchannel111111111111111111111',str((stoptm-uptmi)+origup),'1111111111111111transferchannel111111111111111111111111111')

        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_ans_', key):
                #print('matched')
                #print(key)
                r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
                r.hset(msg['Linkedid'],key+"_state", "Hangup")
                r.hset(msg['Linkedid'],key+"_cause",msg['Cause'])
                #print(r.hgetall(msg['Linkedid']))

#                if re.search(r'Orphan_', key):
#                    addtored=activechan(value,'del')

#                    r.hset(value,key+"_hangtime",msg['Timestamp'])
#                    r.hset(value,key+"_state", "Hangup")
#                    r.hset(value,key+"_cause",msg['Cause'])

                if key == "SourceChannel":

                  #  print(float(redstring['Linkedid']))
                    r.hset(msg['Linkedid'],key+"_state", "Hangup")
                 #   print(float(msg['Timestamp']))
                    #print('Main duration is'+str(float(msg['Timestamp'])-float(redstring['Linkedid'])))
   #                 print('Main duration is'+str(int(stoptm-starttm)))
#                    r.hset(msg['Linkedid'],"Main_duration",str(float(msg['Timestamp'])-float(redstring['Main_start'])))
                    #r.hset(msg['Linkedid'],"Main_duration",int(stoptm-starttm))
#                    if "Up_time" in redstring:
#                        if "Up_duration" in  redstring:
#                          #  uptm=float(redstring['Up_time'])
#                            uptmi=float(redstring['Up_time'])
#                            #print(redstring['Up_duration'])
#                            origup=float(redstring['Up_duration'])
#                           # r.hset(msg['Linkedid'],"Up_duration",str((stoptm-uptmi)+origup))
#                            r.hset(msg['Linkedid'],"Up_duration",str((stoptm-uptmi)+origup))

        #                uptm=float(redstring['Up_time'])
                        #uptm=int(redstring['Up_time'].split(".")[0])
              #          temptimest=msg['Timestamp'].split(".")[0]
               #         print(temptimest)
        #                print(msg['Timestamp'].split()[0])
          #              r.hset(msg['Linkedid'],"Up_duration",str(stoptm-uptm))
                        #r.hset(msg['Linkedid'],"Up_duration",msg['Timestamp'].split()[0]-float(redstring['Up_time'])))
#                    else:
 #                       r.hset(msg['Linkedid'],"Up_duration",0)
#                    if "Bill_time" in redstring:
#                        r.hset(msg['Linkedid'],"Bill_duration",str(float(msg['Timestamp'])-float(redstring['Bill_time'])))
#                    else:
#                        r.hset(msg['Linkedid'],"Bill_duration",0)
  #                  if not msg['ChannelStateDesc'] == 'Up':
  #                  print('EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE')
  #                  print(msg['ChannelStateDesc'])
  #                  print(msg)
  #                  print('EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE')
                    #'ChannelStateDesc': 'Up'
        #            addtored=activechan(msg['Linkedid'],'del')
        #            print(addtored)
              #      print(r.hgetall(msg['Linkedid']))
            #        actionrt ={'Action': 'Status'}
           #         print(type(actionrt))
          #          print = manager()
                 #   ooo =  await manager.send_action(actionrt,as_list=True)
               #     exts = await get_chan(manager, msg['Channel'])
               #     print(exts)
 #   print(r.hgetall(msg['Linkedid']))
    redstring=r.hgetall(msg['Linkedid'])
    #print('YYYYYYYYYYYYYYYYYYYYYYY-FINAL-DATA-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY')
    qit=str(redstring['QuIterator'])
    qit=(str(qit))
        
    fitera=str(r.hget(msg['Linkedid'], "FWDIterator"))
    suppr=0
    for key,value  in redstring.items():
#        print(key,value)
        if  re.search(r'_state', key):
            hstate.append(value)
        if  re.search(r'DstChannel_\d+$', key):
            cnt=key.split('_')[1]
            #print(key,value)
            if redstring['DstChannel_'+str(cnt)+'_state'] == 'Hangup':
           #     print('DstChannel_'+str(cnt)+'_agent_chan_Qu_'+qit+'_state')
                if 'DstChannel_'+str(cnt)+'_agent_chan_Qu_'+qit+'_state' in redstring: #DstChannel_7_agent_chan_Qu_2_state
            #        print('Chan to supress',key,value)
                    suppr='DstChannel_'+str(cnt)
          #?          if 'DstChannel_'+str(cnt)+'_fwd_'+fitera in redstring:
          #?             suppr=0
                    
        if not suppr == 0:
   #         print(suppr)
            if re.search(suppr, key):
                if re.search(r'_hold', key):
                    pass
#                    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!',value,'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
#                print('wipe this shit off',key)
                r.hdel(msg['Linkedid'],key)
            

    print('YYYYYYYYYYYYYYYYYYYYYYY-FINAL-DATA-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY')
    #print('YYYYYYYYYYYYYYYYYYYYYYY-FINAL-DATA-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY')
    print(hstate)
    if len(set(hstate))==1:
        if hstate[0]=='Hangup':
            r.hset(msg['Linkedid'],"Main_duration",str(float(msg['Timestamp'])-float(redstring['Main_start'])))
            if "Up_time" in redstring:
                if "Up_duration" in  redstring:
                          #  uptm=float(redstring['Up_time'])
                    uptmi=float(redstring['Up_time'])
                            #print(redstring['Up_duration'])
                    origup=float(redstring['Up_duration'])
            #        print(stoptm-uptmi,origup)
                           # r.hset(msg['Linkedid'],"Up_duration",str((stoptm-uptmi)+origup))
                    # #r.hset(msg['Linkedid'],"Up_duration",str((stoptm-uptmi)+origup))
                    r.hset(msg['Linkedid'],"Up_duration",str(stoptm-uptmi))

            if "Bill_time" in redstring:
                r.hset(msg['Linkedid'],"Bill_duration",str(float(msg['Timestamp'])-float(redstring['Bill_time'])))
            else:
                r.hset(msg['Linkedid'],"Bill_duration",0)

            addtored=activechan(msg['Linkedid'],'del')
            r.hset(msg['Linkedid'],"Main_end",msg['Timestamp'])
            #print(addtored)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
        print(key,value)

    print('YYYYYYYYYYYYYYYYYYYYYYY-FINAL-DATA-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY')
        
    if len(set(hstate))==1:
        if hstate[0]=='Hangup':
            await serializer(msg['Linkedid'],'stop')

                    
@manager.register_event('QueueParams')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    qmsg = dict(msg)
#    print(qmsg)
    if 'Event' in qmsg:
        del qmsg['Event']
    if 'ActionID' in qmsg:
        del qmsg['ActionID']
    if 'content' in qmsg:
        del qmsg['content']
    if 'Queue' in qmsg:
        qname=qmsg['Queue']
#        del qmsg['Queue']
        qmsg={qname: qmsg}

#    print(qmsg)
    jsonqmsg=json.dumps(qmsg,skipkeys = True)
#    print(jsonqmsg)
    r.hset('Queuemon',qname,jsonqmsg)
 #   testred=r.hgetall('Queuemon')
#    for key,value in testred.items():
#        print(key,value)

@manager.register_event('QueueMemberStatus')  # Register all events
#@manager.register_event('QueueMember')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    mmsg = dict(msg)
    normalizer={}
#    if 'Event' in mmsg:
#        del mmsg['Event']
#    if 'File' in mmsg:
#        del mmsg['File']
#    if 'Privilege' in mmsg:
#        del mmsg['Privilege']
#    if 'Timestamp' in mmsg:
#        del mmsg['Timestamp']
#    if 'SequenceNumber' in mmsg:
#        del mmsg['SequenceNumber']
#    if 'Line' in mmsg:
#        del mmsg['Line']
#    if 'Func' in mmsg:
#        del mmsg['Func']
##    if 'Interface' in mmsg:
 #       mname=mmsg['Interface']
 #   if 'content' in mmsg:
 #       del mmsg['content']
 #   print(mmsg)
    for val in normagents:
#        print(val,mmsg[val])
#        tew=mmsg[val]
        normalizer[val]=mmsg[val]
        #normalizer={val,mmsg[tew]}
#    print(normalizer)    

#    mq=mmsg['Queue']
#    mname={mname:mq}
#        del qmsg['Queue']
#    thnxaster=r.hgetall('Agentmon',mname)
#    thnxaster=json.loads(mname,skipkeys = True)
        
#    jmmane=json.dumps(mname,skipkeys = True,sort_keys=True)
#    jsonqmsg=json.dumps(mmsg,skipkeys = True,sort_keys=True)

    #print(mmsg)
    for val in normagents:
        normalizer[val]=mmsg[val]

#    mname=normalizer
    mname=normalizer['Interface']
    mq=normalizer['Queue']
    mname={mname:mq}
    mmsg=normalizer

#        del qmsg['Queue']
        
    jmmane=json.dumps(mname,skipkeys = True)
    jsonqmsg=json.dumps(mmsg,skipkeys = True)
    r.hset('Agentmon',jmmane,jsonqmsg)
#    print(jsonqmsg)
#    r.hset('Agentmon',jmmane,jsonqmsg)
 #   testred=r.hgetall('Queuemon')
#    for key,value in testred.items():
#        print(key,value)

#    print(list(qmsg.keys())[0])
#@manager.register_event('QueueMemberStatus')  # Register all events
@manager.register_event('QueueMember')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    mmsg = dict(msg)
 #   print(mmsg)
    normalizer={}
    if not 'Ringinuse' in mmsg:
        mmsg['Ringinuse']='0'
#
    for key, value in mmsg.items():
        if 'Location' in mmsg:
            mname=mmsg['Location']
            mmsg['Interface']=mname
            del mmsg['Location']
        if 'Name' in  mmsg:
            mname=mmsg['Name']
            mmsg['MemberName']=mname
            del mmsg['Name']
        if not 'Ringinuse' in mmsg:
            mmsg['Ringinuse']='0'



    for val in normagents:
        normalizer[val]=mmsg[val]

    mname=normalizer
    mmsg=normalizer
    mname=normalizer['Interface']
    mq=normalizer['Queue']
    mname={mname:mq}

#        del qmsg['Queue']
        
    jmmane=json.dumps(mname,skipkeys = True)
    jsonqmsg=json.dumps(mmsg,skipkeys = True)
 #   print('---------------------------------------------------------------------------------')
#    print(normalizer)
    #print(jsonqmsg)
 #   print('---------------------------------------------------------------------------------')
    r.hset('Agentmon',jmmane,jsonqmsg)


            
        #print(key, value)
        

@manager.register_event('BridgeEnter')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
    r.hset(msg['Linkedid'], "Main_Status", 'ANSWER')
    redstring=r.hgetall(msg['Linkedid'])
 #   print(redstring)
#    md=r.hget(msg['Linkedid'], "Up_duration")
#    md=float(md)
#    print(md)
#    md=float(md)

    if "Iterator" in redstring:
        itera = int(redstring["Iterator"])
        for key,value in redstring.items():
            if key == "Up_time":
                if not re.search(r'trsf', msg['Channel']):
                    #print(key,value,'<',msg['Timestamp'], float(msg['Timestamp']) - float(value))

                    summuptime = float(msg['Timestamp']) - float(value)
                    if float(summuptime) > 0.1:
                #    summuptime = float(msg['Timestamp'] - float(md)
          #      r.hset(msg['Linkedid'],"Up_duration",str(summuptime))
                     #   print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',key,value,summuptime,'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',summuptime)
#                        r.hset(msg['Linkedid'],"Up_duration",summuptime+md)
                        #r.hset(msg['Linkedid'],"Up_time",float(msg['Timestamp'])-float(m)
#                        r.hset(msg['Linkedid'],"Up_time",float(msg['Timestamp']))
                        tempupa=float(msg['Timestamp'])
                        tempupb=float(summuptime)
                        tempupc=float(tempupa-tempupb)
                        r.hset(msg['Linkedid'],"Up_time",str(tempupc))
                        a=float(msg['Timestamp'])
                      #  print('a',type(a),a)
                        b=float(summuptime)
                       # print('b',type(b),b)
                        b=float(summuptime)
                        c=a-b
                       # print('c',type(c),c)
                    #    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',float(msg['Timestamp'])-float(summuptime),summuptime,'c is -',c,'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
       # #                 r.hset(msg['Linkedid'],"Up_duration",str(msg['Timestamp']-summuptime))
       # #                if md == 0.0:
       # #                    r.hset(msg['Linkedid'],"Up_duration",str(summuptime))


            if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_', key) and not  re.search(r'trsf', key):
                if 'Up_time' in redstring:
                    summuptime = float(msg['Timestamp']) - float(redstring["Up_time"])
                    if float(summuptime) > 0.1:
                        r.hset(msg['Linkedid'], key+"_answer",msg['Timestamp'])
            #r.hset(msg['Linkedid'], key+"_Ans_Time",msg['Timestamp'])
                        tempupa=float(msg['Timestamp'])
                        tempupb=float(summuptime)
                        tempupc=float(tempupa-tempupb)
                        r.hset(msg['Linkedid'],"Up_time",str(tempupc))
                        #r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
                        #print('1VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV',key,value,'')
                else:
                    r.hset(msg['Linkedid'], key+"_answer",msg['Timestamp'])
            #r.hset(msg['Linkedid'], key+"_Ans_Time",msg['Timestamp'])
                    r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
                   # print('2VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV',key,value,'')

@manager.register_event('QueueEntry')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
    active=r.lrange('activechan_id', 0, -1)
    for i,x in enumerate(active):
        redict=x
        if  x == msg['Uniqueid']:
            qit=int(r.hget(msg['Uniqueid'], "QuIterator"))
            qit=(str(qit))
            r.hset(msg['Uniqueid'], "Qu_"+qit+"_position", msg['Position'])


#@asyncio.coroutine
##async def get_chan(manager, channel):
##   # manager = manager()
##    print(channel)
##    manager = Manager(host='95.161.104.2', port=5038,
##                                   username='starside', secret='r7qyHX34JGxSogdbmnNMtPTl0w')
##    await manager.connect()
##    action = {'Action': 'Status','Channel': channel}
##    manager.send_action(action)
##    async for message in manager.send_action(action):
##        manager.close()
##    message = dict(message)
## #   time.sleep(4)
##    return message


        #r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])



if __name__ == '__main__':
    #asyncio.Queue()
    logging.basicConfig(level=logging.INFO)
    manager.on_connect = on_connect
    manager.on_login = on_login
    manager.on_disconnect = on_disconnect
    manager.connect(run_forever=True, on_startup=on_startup, on_shutdown=on_shutdown)
    #asyncio.looprun()
