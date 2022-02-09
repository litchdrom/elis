import asyncio
import logging
import os
import sys
import redis
import re
import time
import json
import pyodbc
from pprint import pprint
from datetime import datetime
#import websockets
import requests
import zlib

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

specnumber=['122','112','911','060']

ivrvars=['DIGITS']

trunks=['go']

mainpar=['Linkedid','Route','Originate','Callback','CallerIDNum','CallerIDName','Destination','CallThrough','Main_start','Main_end','Main_Status','Up_duration','Main_duration','Bill_duration','Main_Hold','Rating']


normagents=['Queue','MemberName','Interface',
        'StateInterface','Membership','Penalty',
        'CallsTaken','LastCall','LastPause',
        'InCall','Status','Paused',
        'PausedReason','Ringinuse','Wrapuptime']

async def qureload_onstart(mngr: Manager):
    action = {'Action': 'Command','Command':'queue show'}
    temp = await mngr.send_action(action,as_list=False)

async def dev_state(mngr: Manager):
    while True:
        r.delete('Devmon', 0, -1)
        action = {'Action':'DeviceStateList'}
        temp = await mngr.send_action(action,as_list=False)
        await asyncio.sleep(3)

def resizer(bigsize):
    smallsize=zlib.adler32(bigsize.encode('utf-8'))
    return(smallsize)




def activechan(linkeidid,action):
    if action == 'add':   
        redadd=r.rpush("activechan_id",linkeidid)
        radansv='add_ok'
    if action == 'del':
        redindex=r.lrem("activechan_id",0,linkeidid)
        radansv='del_ok'



##loop = asyncio.new_event_loop()

def route_par(source, dest, chan):
    print(source, dest, len(source), len(dest))
    if dest in specnumber:
        route ="Dialout"
    elif len(source) <= 4 and len(dest) <= 4 and not dest=="s":
        route="Local"
    elif len(source) >= 10 and len(dest) >= 10:
        route="Dialin"
    else: 
        route ="Dialout"
    
    channame=splitchan(chan)
    if channame in trunks:
        route ="Dialin"
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
    sql=("select id from starside.agents where is_deleted=0 and endpoint_id= ?")
   # sql=("select crm_id from starside.agents where is_deleted=0 and endpoint_id= ?")
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

async def serializer(dictionary,state):
    await asyncio.sleep(0.1)
    ser=r.hgetall(dictionary)
    main={}
    sourcepr={}
    mainpr={}
    iterators={}
    destpr={}
    channels={}
    chan={}
    brtalks={}
    maininfo={}
    iterator={}
    qupr={}
    quchan={}
    aqupr={}
    qntd=0
    cnt=str(0)
    if 'CallRec' in ser:
        rec={'CallRec':ser['CallRec']}
    else:
        rec={'CallRec':'None'}

    for key,value in r.hscan_iter(dictionary, match='Qu_*'):
        print(key,value)
    print('++++++++++++++++++','www','++++++++++++++++++')

    for key,value in ser.items():
        if  re.search(r'SRC', key) and not  re.search(r'hold', key) and not  re.search(r'_SRC', key):
            sourcepr[key]=value
        if not re.search(r'Chann', key) and not re.search(r'Qu', key) and not key == 'CallRecord' and not re.search(r'agent', key):
            mainpr[key]=value
        if  re.search(r'Iterator', key):
            iterators[key]=value
        if  re.search(r'DST', key) and not re.search(r'Qu', key) and not re.search(r'_agent_', key) and not  re.search(r'hold', key):
            destpr[key]=value

        if  re.search(r'Qu_', key) and not re.search(r'DST_', key)  and not re.search(r'DST_', key) and not re.search(r'Qu_\d+:\d+:.+:.+', key):
            qupr[key]=value

        if  re.search(r'Qu_\d+:\d+:.+:.+', key):
            quchan[key]=value
        
        if  re.search(r'Br:talk:', key):
            brtalks[key]=value

        if  re.search(r'Iterator', key) and not re.search(r'DST_', key):
            iterator[key]=value

    for key,value in  qupr.items():
        if  re.search(r'^Qu_\d+$', key):
            qnt=key.split('_')[1]
            if not 'Qu_'+str(qnt)+'_status' in qupr:
                qntd=qnt
    

    for key,value in  qupr.items():
        if not qntd == 0:
            if re.search(r'Qu_'+qntd, key):
                print(key,value)

    for key,value in  destpr.items():
        if  re.search(r'DST_\d+$', key):
            cnt=key.split('_')[1]
            if state == 'active':
                if destpr['DST_'+str(cnt)+'_state'] == 'Hangup':
                    cnt=str(0)
            else:
                cnt=cnt
            channum=splitchan(value)
            channum=channum+"_"+cnt
            schan="DST_"+cnt

            chan={channum:{}}
            for key,value in  destpr.items():
                if re.search(fr'{schan}', key):
                    chan[channum][key]=value
                    channels.update(chan)

    for key,value in  mainpr.items():
        if key in mainpar:
            maininfo[key]=value
        if re.search(r'IVR_digits', key):
            maininfo[key]=value

            

    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print(maininfo,sourcepr,channels,qupr,quchan,iterators,rec,brtalks)
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    if state == 'stop':
        call={}
        calllist={}
        mainparm=[maininfo,sourcepr,channels,qupr,quchan,brtalks,iterators]
        mainparmn=['maininfo','source','channels','queues','queuechannels','brtalks','iterators','rec']
        print(maininfo['Linkedid'])
        for i, x in enumerate(mainparm):
            call={mainparmn[i]:x}
            calllist.update(call)

        print(calllist)
        findict={maininfo['Linkedid']:calllist}
        params=findict
        response = requests.post(requesturl, json=params,timeout=6)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),response)

async def calltoweb():
    while True:
        active=r.lrange('activechan_id', 0, -1)
        if not  active:
            r.delete('activechan_web', 0, -1)
        else: 
            for i,x in enumerate(active):
                redict=x
                try:
                    await serializer(redict,'active')
                except Exception as e:
                    print("type error: " + str(e))
        await asyncio.sleep(2)

    
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
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"Disconnect")


async def on_startup(mngr: Manager):
    await asyncio.sleep(0.1)
    logging.info('Something action...')
    r.delete("activechan_id",0,-1)
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"Start")
   # killinit()
    await asyncio.gather(calltoweb(),  queuesdata(mngr), qureload_onstart(mngr) ,dev_state(mngr))


async def on_shutdown(mngr: Manager):
    await asyncio.sleep(0.1)
    logging.info(
        'Shutdown AMI connection on %s:%s' % (mngr.config['host'], mngr.config['port'])
    )

@manager.register_event('*')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    if msg.Event == 'FullyBooted':
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),msg)

@manager.register_event('Newchannel')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
#    print(msg)
    skipmark=0

    if not r.hexists(msg['Linkedid'],"SRC") and not r.hexists(msg['Linkedid'],"Iterator"):
        if re.search(r'originate', msg['Channel']):
            r.hset(msg['Linkedid'], "Originate", "1")
#            r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=2

        if re.search(r'trsf', msg['Channel']):
            r.hset(msg['Linkedid'], "Orphan", "1")
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=1

        if re.search(r'callback_', msg['Channel']):
            r.hset(msg['Linkedid'], "Callback", "1")
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=1

        if  re.search(r'forwards', msg['Channel']):
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=1

        if  msg['Exten'] == '*8' and msg['Context'] == 'internal':
            r.hset(msg['Linkedid'], "Orphan", "1")
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            skipmark=1


        if  msg['Exten'] == 's' and msg['Context'] == 'internal':
            if not r.hexists(msg['Linkedid'],"Originate"):
                r.hset(msg['Linkedid'], "Orphan", "1")
                skipmark=3
            else:
                r.hset(msg['Linkedid'], "Orphan", "1")
#                r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
        if skipmark==0:

            rdefine=route_par(msg['CallerIDNum'], msg['Exten'],msg['Channel'])
            r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
            r.hset(msg['Linkedid'], "Route", rdefine)
            r.hset(msg['Linkedid'], "SRC", msg['Channel'])
            r.hset(msg['Linkedid'], "SRC_Uid", msg['Uniqueid'])
            r.hset(msg['Linkedid'], "SRC_state", msg['ChannelStateDesc'])
            if not r.hexists(msg['Linkedid'],"CallerIDNum"):
                #print('skip')
                if  len(msg['CallerIDNum']) == 10:
                    r.hset(msg['Linkedid'], "CallerIDNum", "7"+msg['CallerIDNum'])
                else:
                    r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
                r.hset(msg['Linkedid'], "CallerIDName", msg['CallerIDNum'])
            #print("CallerIDName", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "Destination", msg['Exten'])
            r.hset(msg['Linkedid'], "Iterator", 0)
            r.hset(msg['Linkedid'], "QuIterator", int(0))
            r.hset(msg['Linkedid'], "IVRIterator", int(0))
            r.hset(msg['Linkedid'], "TRSFIterator", int(0))
            r.hset(msg['Linkedid'], "FWDIterator", int(0))
            r.hset(msg['Linkedid'], "TlkIterator", int(0))
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            r.hset(msg['Linkedid'], "Main_Status", 'NOANSWER')
            r.hset(msg['Linkedid'], "Main_Hold", 0)
            r.hset(msg['Linkedid'], "Up_duration", 0)
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)
            
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['Linkedid'], "SRC_starid", starsideid)

        if skipmark==3:

            rdefine=route_par(msg['CallerIDNum'], msg['Exten'],msg['Channel'])
            r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
            r.hset(msg['Linkedid'], "Route", rdefine)
            r.hset(msg['Linkedid'], "SRC", msg['Channel'])
            r.hset(msg['Linkedid'], "SRC_Uid", msg['Uniqueid'])
            r.hset(msg['Linkedid'], "SRC_state", msg['ChannelStateDesc'])
            if  len(msg['CallerIDNum']) == 10:
                r.hset(msg['Linkedid'], "CallerIDNum", "7"+msg['CallerIDNum'])
            else:
                r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "CallerIDName", msg['CallerIDNum'])
            #print("CallerIDName", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "Destination", msg['Exten'])
            r.hset(msg['Linkedid'], "Iterator", 0)
            r.hset(msg['Linkedid'], "QuIterator", int(0))
            r.hset(msg['Linkedid'], "IVRIterator", int(0))
            r.hset(msg['Linkedid'], "TRSFIterator", int(0))
            r.hset(msg['Linkedid'], "FWDIterator", int(0))
            r.hset(msg['Linkedid'], "TlkIterator", int(0))
            r.hset(msg['Linkedid'], "Main_start", msg['Timestamp'])
            r.hset(msg['Linkedid'], "Main_Status", 'NOANSWER')
            r.hset(msg['Linkedid'], "Main_Hold", 0)
            r.hset(msg['Linkedid'], "Up_duration", 0)
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['Linkedid'], "SRC_starid", starsideid)
            
        if not r.hexists(msg['Linkedid'],"Orphan"):
            if r.hexists(msg['Linkedid'],"Linkedid"):
                addtored=activechan(msg['Linkedid'],'add')

    if r.hexists(msg['Linkedid'],"SRC") and r.hexists(msg['Linkedid'],"Iterator"):
        skipmark=0
    
        if re.search(r'originate', msg['Channel']):
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)
            temp=dict(temp)
            skipmark=1
        if re.search(r'trsf', msg['Channel']):
            skipmark=1

        if re.search(r'callback_', msg['Channel']):
            skipmark=1

        if re.search(r'forwards', msg['Channel']):
            pass
            skipmark=1

        if re.search(r'phone_forwards', msg['Channel']):
            skipmark=1

        if r.hget(msg['Linkedid'],"SRC") == msg['Channel']:
            skipmark=1

        if skipmark==0:
            spchan=msg['Channel'].split('-')
            spchan=spchan[0]
            itera = int(r.hget(msg['Linkedid'],"Iterator"))
            itera = itera+1
            r.hset(msg['Linkedid'], "Iterator", itera)
            r.hset(msg['Linkedid'], "DST_"+str(itera), msg['Channel'])
            r.hset(msg['Linkedid'], "DST_"+str(itera)+"_state", msg['ChannelStateDesc'])
            r.hset(msg['Linkedid'], "DST_"+str(itera)+"_Uid", msg['Uniqueid'])
            r.hset(msg['Linkedid'], "DST_"+str(itera)+"_start", msg['Timestamp'])
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['Linkedid'], "DST_"+str(itera)+"_starid", starsideid)
            action = {'Action': 'Status', 'Channel' :  msg['Channel'] , 'AllVariables' : 'true'}
            temp = await mngr.send_action(action,as_list=False)

@manager.register_event('Hold')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value in redstring.items():
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'Br:', key):
            r.hset(msg['Linkedid'],key+"_state", 'Hold')
            if value == msg['Channel']  and not re.search(r'_agent_', key):
                r.hset(msg['Linkedid'],key+"_hold", msg['Timestamp'])

@manager.register_event('Unhold')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    delchan=0
    redstring=r.hgetall(msg['Linkedid'])
    holdtm=r.hget(msg['Linkedid'],"Main_Hold")
    for key,value in redstring.items():
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'Br:', key):
            delchan=key
            r.hset(msg['Linkedid'],key+"_state", msg['ChannelStateDesc'])
            if value == msg['Channel']  and not re.search(r'_agent_', key):
                r.hset(msg['Linkedid'],key+"_unhold", msg['Timestamp'])
            if not re.search(r'_hold', key):
                r.hset(msg['Linkedid'],key+"_hold", msg['Timestamp'])
        if  re.search(r'_hold', key):
            r.hset(msg['Linkedid'],"Main_Hold", float(holdtm)+float(msg['Timestamp'])-float(value))
            r.hdel(msg['Linkedid'],key)

    redstring=r.hgetall(msg['Linkedid'])
    for key,value in redstring.items():
        if  re.search(r'_unhold', key) and re.search(delchan, key):
            r.hdel(msg['Linkedid'],key)

@manager.register_event('Status')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    sitatusskip=0
    redstring=r.hgetall(msg['Linkedid'])
    if msg['Exten'] == "s":
        channame=splitchan(msg['Channel'])
        if channame in trunks:
            for key,value in redstring.items():
                if msg['Channel'] == value:
                    r.hset(msg['Linkedid'],key+'_dial', msg['CallerIDNum'])
        speervariables=msg['Variable']
        varindex = [i for i, x in enumerate(speervariables) if re.search(r'DIALEDPEER', x)]
        if varindex:
            dialedpeer=speervariables[varindex[0]].split('=')[1]
            if dialedpeer and 'Callback' in redstring:
                r.hset(msg['Linkedid'],"Destination",msg['ConnectedLineNum'])
                r.hset(msg['Linkedid'],"CallerIDNum",msg['ConnectedLineNum'])
                svariables=msg['Variable']
                varindex = [i for i, x in enumerate(svariables) if re.search(r'TRUECID', x)]
                if varindex:
                    truedest=svariables[varindex[0]].split('=')[1]
                    r.hset(msg['Linkedid'],"Destination",truedest)
        else:
            svariables=msg['Variable']
            returned = [i for i, x in enumerate(speervariables) if re.search(r'TRUELINKED', x)]
            #print(svariables[returned[0]],'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            if returned:
                redstring=r.hgetall(msg['Linkedid'])
                for key,value in redstring.items():
                    if msg['Channel'] in value and re.search(r'DST_\d+$', key):
                        #print(key,value,'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                        statusskip=0
                        break
                    else:
                        statusskip=1
                #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!','statusskip',statusskip)
                if statusskip == 1:
                    truedest=svariables[returned[0]].split('=')[1]
                    redstring=r.hgetall(truedest)
                    r.hset(msg['Linkedid'],"Destination",redstring["Destination"])
                
                    itera = int(r.hget(truedest,"Iterator"))
                    #print('Achtung 521')
                    itera = itera+1
                    r.hset(truedest, "Iterator", itera)
                    r.hset(truedest, "DST_"+str(itera), msg['Channel'])
                    r.hset(truedest, "DST_"+str(itera)+"_state", msg['ChannelStateDesc'])
                    r.hset(truedest, "DST_"+str(itera)+"_Uid", msg['Uniqueid'])
                    r.hset(truedest, "DST_"+str(itera)+"_start", msg['Uniqueid'])
                    channame=splitchan(msg['Channel'])
                    starsideid=getsid(channame)
                    r.hset(truedest, "DST_"+str(itera)+"_starid", starsideid)
                    if float(msg['Linkedid']) > float(truedest):
                        r.hset(msg['Linkedid'], "Parent_ID_"+str(itera), truedest)
                        r.hset(truedest, "Orphan_ID_"+str(itera), msg['Linkedid'])


@manager.register_event('AttendedTransfer')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['TransfereeLinkedid'])
    for key,value in redstring.items():
        if value == msg['OrigTransfererChannel']:
            for key,value in redstring.items():
                if re.search(r'_hold', key):
                    holdtm=redstring["Main_Hold"]
                    r.hset(msg['TransfereeLinkedid'],"Main_Hold", float(holdtm)+float(msg['Timestamp'])-float(value))
    talkit=redstring['TlkIterator']
    for key,value in redstring.items():
        if re.search(rf"Br:talk:{str(talkit)}:\d", key) and value == msg['OrigTransfererChannel']:
            if not 'Br:talk:'+str(talkit)+':stop' in redstring:
                #print('##################close talk atd trsf',key,value)
                r.hset(msg['TransfereeLinkedid'], 'Br:talk:'+str(talkit)+':stop',msg['Timestamp'])

@manager.register_event('Pickup')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    r.hset(msg['Linkedid'],"Orphan", "1")
    r.hset(msg['Linkedid'],"Pickup","Pickup")
    r.hset(msg['Linkedid'],"PickupChannel",msg['TargetChannel'])
    r.hset(msg['Linkedid'],"PickupLinkedid",msg['TargetLinkedid'])
    r.hset(msg['Linkedid'],"PickupConnectedCli",msg['TargetConnectedLineNum'])
    r.hset(msg['TargetLinkedid'],"Pickup","Pickup")

    truedest=r.hget(msg['TargetLinkedid'],"Linkedid")
    redstring=r.hgetall(msg['TargetLinkedid'])
    itera = int(redstring["Iterator"])
    for key,value in redstring.items():
        if value == msg['TargetChannel'] and not re.search(r'_agent_', key) and not re.search(r'_pickup', key)  and not  re.search(r'Qu_\d+:\d+:\d+:channel', key) and not  re.search(r'Qu_\d+:\d+:None:channel', key) and not re.search(r'Br:', key):
            r.hset(msg['TargetLinkedid'],key+"_pickup", msg['Channel'])
            itera = itera+1
            r.hset(msg['TargetLinkedid'], "Orpan_ID_"+str(itera), truedest)
            r.hset(msg['Linkedid'], "Parent_ID_"+str(itera), msg['TargetLinkedid'])
            r.hset(msg['TargetLinkedid'], "Iterator", itera)
            r.hset(msg['TargetLinkedid'], "DST_"+str(itera), msg['Channel'])
            r.hset(msg['TargetLinkedid'], "DST_"+str(itera)+"_state", msg['ChannelStateDesc'])
            r.hset(msg['TargetLinkedid'], "DST_"+str(itera)+"_Uid", msg['Uniqueid'])
            r.hset(msg['TargetLinkedid'], "DST_"+str(itera)+"_answer", msg['Timestamp'])
            r.hset(msg['TargetLinkedid'], "DST_"+str(itera)+"_start", msg['Timestamp'])
            channame=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            r.hset(msg['TargetLinkedid'], "DST_"+str(itera)+"_starid", starsideid)
        if value == msg['TargetChannel'] and re.search(r'Qu_\d+:\d+:\d+|None:channel+$', key):
            agefow=key.split(':')
            agefow.insert(3, 'release')
            agefow.pop(4)
            agefow=':'.join(str(x) for x in agefow)
            r.hset(msg['TargetLinkedid'],agefow,msg['Timestamp'])
            channame=splitchan(msg['Channel'])
            channum=splitchan(msg['Channel'])
            starsideid=getsid(channame)
            qit=str(redstring['QuIterator'])
            qit=(str(qit))
            qname='Qu_'+qit
            r.hset(msg['TargetLinkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])

@manager.register_event('OriginateResponse')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    if r.hget(msg['Uniqueid'],"Destination") == "s":
        r.hset(msg['Uniqueid'],"Destination",msg['Exten'])

@manager.register_event('Newstate')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    channame=splitchan(msg['Channel'])
    if 'Up_time' not in redstring and channame in trunks:
        r.hset(msg['Linkedid'],"Bill_time", msg['Timestamp'])
    elif channame in trunks:
        r.hset(msg['Linkedid'],"Bill_time", msg['Timestamp'])

    for key,value in redstring.items():
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
            r.hset(msg['Linkedid'],key+"_state", msg['ChannelStateDesc'])
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key):
                    r.hset(parentid,key+"_state", msg['ChannelStateDesc'])
        if re.search(r'Orphan_ID_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r'Qu_\d+_answer_chan', key):
                    r.hset(parentid,key+"_state", msg['ChannelStateDesc'])
         
@manager.register_event('VarSet')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    if msg['Variable'] == 'DIALSTATUS' and msg.get('Value'):
        if not r.hexists(msg['Linkedid'],"SRC") and not r.hexists(msg['Linkedid'],"Iterator"):
            r.hset(msg['Linkedid'], "Iterator", 0)
            rdefine=route_par(msg['CallerIDNum'], msg['Exten'],msg['Channel'])
            r.hset(msg['Linkedid'], "Route", rdefine)
            if  len(msg['CallerIDNum']) == 10:
                r.hset(msg['Linkedid'], "CallerIDNum", "7"+msg['CallerIDNum'])
            else:
                r.hset(msg['Linkedid'], "CallerIDNum", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "CallerIDName", msg['CallerIDNum'])
            r.hset(msg['Linkedid'], "Destination", msg['Exten'])
            r.hset(msg['Linkedid'],"Main_duration", 0)
            r.hset(msg['Linkedid'],"Bill_duration", 0)
        redstring=r.hgetall(msg['Linkedid'])
        itera=redstring['Iterator']
        tritera = int(redstring["TRSFIterator"])
        for key,value  in redstring.items():
            if value == msg['Channel'] and not  re.search(r'_agent_', key) and not  re.search(r'_transfer_', key) and not  re.search(r'Qu_\d+_answer_', key) and not re.search(r'Qu_\d+:\d+:.+:.+', key) and not re.search(r'Br:', key):
                r.hset(msg['Linkedid'],key+"_DStatus", msg['Value'])

            if re.search(r'_transfer_'+str(tritera), key) and not re.search(r'_DStatus', key) and not  re.search(r'Qu_\d+_answer_', key) and not re.search(r'Qu_\d+:\d+:.+:.+', key) and not re.search(r'Br:', key):
                r.hset(msg['Linkedid'],key+"_DStatus", msg['Value'])

            if re.search(r'Qu_\d+:\d+:.+:transfer_'+str(tritera), key):
                tritera = int(redstring["TRSFIterator"])
            
                agefow=key.split(':')
                agefow.insert(3, 'transferstatus_'+str(tritera))
                agefow.pop(4)
                agefow=':'.join(str(x) for x in agefow)
                r.hset(msg['Linkedid'],agefow,msg['Value'])
        if re.search(r'callback_out', msg['Channel']):
            r.hset(msg['Linkedid'],"SRC_DStatus", msg['Value'])


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

    #if msg['Variable'] == 'tempnum':
    if msg['Variable'] == 'TEMPNUM':
        r.hset(msg['Linkedid'], "CallThrough", msg['Value'])
  
    if msg['Variable'] == 'FORWARDDEST':
        itera=int(r.hget(msg['Linkedid'], "Iterator"))
        fitera=int(r.hget(msg['Linkedid'], "FWDIterator"))
        r.hset(msg['Linkedid'], "FWDIterator", fitera+1)
        redstring=r.hgetall(msg['Linkedid'])
        r.hset(msg['Linkedid'],"DST_"+str(itera)+"_fwd_"+str(int(fitera)+1), msg['Value'])

    if msg['Variable'] == 'FWDER1':
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

    if msg['Variable'] == 'QUDTMF':
        qit=int(r.hget(msg['Linkedid'], "QuIterator"))
        qit=(str(qit))
        qjoin=r.hget(msg['Linkedid'], "Qu_"+qit+"_join")
        qqwait=int(float(msg['Timestamp']))-int(float(qjoin))
#        r.hset(msg['Linkedid'], "QuIterator", qit)
#        r.hset(msg['Linkedid'], "Qu_"+qit, msg['Value'])
        r.hset(msg['Linkedid'], "Qu_"+qit+"_status", 'Abandon')
        r.hset(msg['Linkedid'], "Qu_"+qit+"_wait", qqwait)


    if msg['Variable'] == 'GCBCID':
        r.hset(msg['Linkedid'],'Destination',msg['Value'])

    if msg['Variable'] == 'PARENT_LINK':
        r.hset(msg['Linkedid'],'Callback',msg['Value'])

    if msg['Variable'] == 'ORIGCLI':
        r.hset(msg['Linkedid'],'CallerIDNum',msg['Value'])
        r.hset(msg['Linkedid'],'CallerIDName',msg['Value'])
    
    if msg['Variable'] == 'FORWARD':
        if not re.search(r'Local', msg['Channel']):
            fitera=int(r.hget(msg['Linkedid'], "FWDIterator"))
            fitera=fitera+1
            r.hset(msg['Linkedid'], "FWDIterator", fitera)
            redstring=r.hgetall(msg['Linkedid'])
            for key,value  in redstring.items():
                if value == msg['Channel']:
                    r.hset(msg['Linkedid'],key+"_fwd_"+str(fitera), msg['CallerIDNum'])
    if msg['Variable'] == 'QUEUESTRATEGY':
        try:
            qit=int(r.hget(msg['Linkedid'], "QuIterator"))
            qit=(str(qit))
            r.hset(msg['Linkedid'], "Qu_"+qit+"_strategy",msg['Value'])
            #print(msg)
        except Exception as e:
                    print('Pickup try',e)

@manager.register_event('QueueCallerLeave')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    qit=int(r.hget(msg['Linkedid'], "QuIterator"))
    qit=(str(qit))

    r.hset(msg['Linkedid'], "Qu_"+qit, msg['Queue'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_position", msg['Position'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_leave", msg['Timestamp'])

@manager.register_event('QueueCallerJoin')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    qit=int(r.hget(msg['Linkedid'], "QuIterator"))+1
    qit=(str(qit))
    r.hset(msg['Linkedid'], "QuIterator", qit)
    r.hset(msg['Linkedid'], "Qu_"+qit, msg['Queue'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_init", msg['Position'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_count", msg['Count'])
    r.hset(msg['Linkedid'], "Qu_"+qit+"_join", msg['Timestamp'])

@manager.register_event('QueueCallerAbandon')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
        if msg['Queue'] in value and re.search(r'Qu_\d+$', key):
            r.hset(msg['Linkedid'], key+"_status", "Abandon")
            r.hset(msg['Linkedid'], key+"_wait", msg['HoldTime'])



@manager.register_event('AgentCalled')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    channame=splitchan(msg['DestChannel'])
    channum=splitchan(msg['DestChannel'])
    starsideid=getsid(channame)
    qit=str(redstring['QuIterator'])
    qit=(str(qit))
    strat=r.hget(msg['Linkedid'], "Qu_"+qit+"_strategy")


    #print(starsideid,channame)
    for key,value  in redstring.items():
        if msg['Queue'] in value and re.search(r'Qu_'+qit, key):
            qname=key
            if not r.hexists(msg['Linkedid'],qname+':'+channum+':'+str(starsideid)+':start'):
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':start',msg['Timestamp'])
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])
            else:
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])
                if not strat == 'ringall':
                    addtime=r.hget(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':start')
                    r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':start',addtime+':'+msg['Timestamp'])


@manager.register_event('AgentConnect')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
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
            r.hset(msg['Linkedid'], key+"_answer_chan", msg['DestChannel'])
            r.hset(msg['Linkedid'], key+"_answer_time", msg['Timestamp'])
            if not re.search(r'phone_forward', msg['DestChannel']):
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':answer',msg['Timestamp'])
                r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':channel',msg['DestChannel'])
        if re.search(r':fwd', key) and  re.search(value, msg['DestChannel']):
            agefow=key.split(':')
            agefow.pop(3)
            agefow=':'.join(str(x) for x in agefow)
            r.hset(msg['Linkedid'], agefow+':answer',msg['Timestamp'])
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
    qname='Qu_'+qit
    strat=r.hget(msg['Linkedid'], "Qu_"+qit+"_strategy")
    for key,value  in redstring.items():
        if msg['DestChannel'] in value and re.search(r'DST_\d+$', key):
            r.hset(msg['Linkedid'], key+'_delete',str(1))
    if not strat == 'ringall':
        if not r.hexists(msg['Linkedid'],qname+':'+channum+':'+str(starsideid)+':stop'):
            r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':stop',msg['Timestamp'])
        else:
            addtime=r.hget(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':stop')
            r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':stop',addtime+':'+msg['Timestamp'])


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
        if msg['Queue'] in value and re.search(r'Qu_'+qit, key): 
            qname=key
            r.hset(msg['Linkedid'], key+"_wait", msg['HoldTime'])
            r.hset(msg['Linkedid'], key+"_talk", msg['TalkTime'])
            r.hset(msg['Linkedid'], key+"_release", msg['Reason'])
            r.hset(msg['Linkedid'], key+"_status", 'Answered')

    for key,value  in redstring.items():
        if re.search(r'Qu_'+qit+':.+:answer', key):
            agefow=key.split(':')
            agefow.pop(3)
            agefow=':'.join(str(x) for x in agefow)
            r.hset(msg['Linkedid'],agefow+':release',msg['Timestamp'])

@manager.register_event('AgentDump')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)

@manager.register_event('UserEvent')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    if "RECORDING" in msg.keys():
        r.hset(msg['Linkedid'],"CallRec" , msg['RECORDING'])

    if "TRANSFER" in msg.keys():
        #print(msg)
        redstring=r.hgetall(msg['Linkedid'])
        titera = int(redstring["Iterator"])
        tritera = int(redstring["TRSFIterator"])+1
        r.hset(msg['Linkedid'], "TRSFIterator", tritera)
        for key,value  in redstring.items():
            if value == msg['TRANSFER'] and not re.search(r'_agent_',key) and not re.search(r'Qu_\d+_answer_chan',key) and not  re.search(r'Qu_\d+:\d+:.+:channel', key) and not re.search(r'Br:',key):
                r.hset(msg['Linkedid'],key+"_transfer_"+str(tritera), msg['Exten'])
                r.hset(msg['Linkedid'],key+"_trsftime_"+str(tritera), msg['Timestamp'])
            
            if value == msg['TRANSFER'] and re.search(r'Qu_\d+:\d+:.+:channel', key):
            
                agefow=key.split(':')
                agefow.insert(3, 'transfer_'+str(tritera))
                agefow.pop(4)
                agefow=':'.join(str(x) for x in agefow)
                r.hset(msg['Linkedid'],agefow,msg['Exten'])
                agefow=key.split(':')
                agefow.insert(3, 'trsftime_'+str(tritera))
                agefow.pop(4)
                agefow=':'.join(str(x) for x in agefow)
                r.hset(msg['Linkedid'],agefow,msg['Timestamp'])
    
@manager.register_event('BlindTransfer')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print('!!!!!!!!!!',msg)
    redstring=r.hgetall(msg['TransfereeLinkedid'])
    titera = int(redstring["TRSFIterator"])
    titera=titera+1
    r.hset(msg['TransfereeLinkedid'], "TRSFIterator", titera)
    for key,value in redstring.items():
        if value == msg['TransfererChannel'] and not re.search(r'Qu_\d+:\d+:.+:channel', key) and not re.search(r'Br:talk', key) and not re.search(r'Qu_', key) and not re.search(r'Br:', key):
            titer=0
            titer=titera
            r.hset(msg['TransfereeLinkedid'],key+"_transfer_"+str(titer), msg['Extension'])
            r.hset(msg['TransfereeLinkedid'],key+"_trsftime_"+str(titer), msg['Timestamp'])
            r.hset(msg['TransfereeLinkedid'], "TRSFIterator", titera)
            
    for key,value in redstring.items():
        if re.search(r'Qu_\d+:\d+:.+:channel', key) and value == msg['TransfererChannel']:
            
            agefow=key.split(':')
            agefow.insert(3, 'transfer_'+str(titer))
            agefow.pop(4)
            agefow=':'.join(str(x) for x in agefow)
            r.hset(msg['TransfereeLinkedid'],agefow,msg['Extension'])
            agefow=key.split(':')
            agefow.insert(3, 'trsftime_'+str(titer))
            agefow.pop(4)
            agefow=':'.join(str(x) for x in agefow)
            r.hset(msg['TransfereeLinkedid'],agefow,msg['Timestamp'])
    
@manager.register_event('HangupRequest')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key)  and not  re.search(r'_ans', key) and not  re.search(r'_pickup', key) and not  re.search(r'Br:', key):
                    #print('HangupRequest',key)
                    r.hset(parentid,key+"_state", "Hangup")
                    r.hset(parentid,key+"_hangtime",msg['Timestamp'])
                    r.hset(parentid,key+"_cause",msg['Cause'])
                    #r.hset(truedest, "Orphan_ID_"+str(itera), msg['Linkedid'])
        if value == msg['Channel'] and  not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_pickup', key) and not  re.search(r'Br:', key):
                r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
                r.hset(msg['Linkedid'],key+"_state", "Hangup")

@manager.register_event('SoftHangupRequest')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    redstring=r.hgetall(msg['Linkedid'])
    for key,value  in redstring.items():
        #if value == msg['Channel'] and not re.search(r'_agent_chan_', key):
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_ans', key) and not  re.search(r'Br:', key):
                    r.hset(parentid,key+"_state", "Hangup")
                    r.hset(parentid,key+"_hangtime",msg['Timestamp'])
                    r.hset(parentid,key+"_cause",msg['Cause'])
        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'Br:', key):
                r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
                r.hset(msg['Linkedid'],key+"_state", "Hangup")
                talkit=redstring['TlkIterator']
                for key,value in redstring.items():
                    if re.search(rf"Br:talk:{str(talkit)}:\d", key) and value == msg['Channel']:
                        if not 'Br:talk:'+str(talkit)+':stop' in redstring:
                            r.hset(msg['Linkedid'], 'Br:talk:'+str(talkit)+':stop',msg['Timestamp'])
    if not r.hexists(msg['Linkedid'],"Linkedid") and r.hexists(msg['Linkedid'],"Originate"):
        r.hset(msg['Linkedid'], "Linkedid", msg['Linkedid'])
        r.hset(msg['Linkedid'], "Main_Status", 'NOANSWER')
        await serializer(msg['Linkedid'],'stop')

#        addtored=activechan(msg['Linkedid'],'add')


@manager.register_event('DeviceStateChange')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    if not re.search(r'Queue',  msg['Device']) and not re.search(r'Custom',  msg['Device']) and not re.search(r'MWI',  msg['Device']):
        r.hset('Devmon', msg['Device'], msg['State'])



@manager.register_event('Hangup')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    hstate=[]
    redstring=r.hgetall(msg['Linkedid'])
    stoptm=float(msg['Timestamp'])
    for key,value  in redstring.items():
        if re.search(r'Parent_', key):
            parentid=value
            orfstring=r.hgetall(parentid)
            for key,value in orfstring.items():
                if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_ans', key) and not  re.search(r'Br:', key):
                    r.hset(parentid,key+"_hangtime",msg['Timestamp'])
                    r.hset(parentid,key+"_cause",msg['Cause'])
                    r.hset(parentid,key+"_state",'Hangup')

        if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_\d+_answer_chan', key) and not  re.search(r'_ans', key)  and not  re.search(r'_pickup', key) and not  re.search(r'Br:', key) and not re.search(r'_agent_chan_Qu_\d+', key):
                r.hset(msg['Linkedid'],key+"_hangtime",msg['Timestamp'])
                r.hset(msg['Linkedid'],key+"_state", "Hangup")
                r.hset(msg['Linkedid'],key+"_cause",msg['Cause'])
                talkit=redstring['TlkIterator']
                for key,value in redstring.items():
                    if re.search(rf"Br:talk:{str(talkit)}:\d", key) and value == msg['Channel']:
                        if not 'Br:talk:'+str(talkit)+':stop' in redstring:
                            r.hset(msg['Linkedid'], 'Br:talk:'+str(talkit)+':stop',msg['Timestamp'])

                if key == "SRC":
                    r.hset(msg['Linkedid'],key+"_state", "Hangup")
    redstring=r.hgetall(msg['Linkedid'])
    qit=str(redstring['QuIterator'])
    qit=(str(qit))
        
    fitera=str(r.hget(msg['Linkedid'], "FWDIterator"))
    suppr=0
    for key,value  in redstring.items():
        if  re.search(r'DST_\d+_state', key):
            hstate.append(value)
        if  re.search(r'SRC_state', key):
            hstate.append(value)
        if  re.search(r'DST_\d+$', key):
            cnt=key.split('_')[1]
            if redstring['DST_'+str(cnt)+'_state'] == 'Hangup':
                if 'DST_'+str(cnt)+'_delete' in redstring: #DST_7_agent_chan_Qu_2_state
                    print('Chan to supress',key,value)
                    suppr='DST_'+str(cnt)
                    
                for key,value  in redstring.items():
                    if not suppr == 0:
                        if re.search(suppr, key) and value :
                            if re.search(r'_hold', key):
                                pass
                            print('wipe this shit off',key,value)
                            r.hdel(msg['Linkedid'],key)
            

#    print('YYYYYYYYYYYYYYYYYYYYYYY-FINAL-DATA-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY')
#    print(hstate)
    if len(set(hstate))==1:
        if hstate[0]=='Hangup':
            r.hset(msg['Linkedid'],"Main_duration",str(float(msg['Timestamp'])-float(redstring['Main_start'])))
            if "Up_time" in redstring:
                if "Up_duration" in  redstring:
                    uptmi=float(redstring['Up_time'])
                    origup=float(redstring['Up_duration'])
                    r.hset(msg['Linkedid'],"Up_duration",str(stoptm-uptmi))

            if "Bill_time" in redstring:
                r.hset(msg['Linkedid'],"Bill_duration",str(float(msg['Timestamp'])-float(redstring['Bill_time'])))
            else:
                r.hset(msg['Linkedid'],"Bill_duration",0)

            addtored=activechan(msg['Linkedid'],'del')
            r.hset(msg['Linkedid'],"Main_end",msg['Timestamp'])
    #redstring=r.hgetall(msg['Linkedid'])
    #for key,value  in redstring.items():
    #    print(key,value)

        
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
        qmsg={qname: qmsg}

    jsonqmsg=json.dumps(qmsg,skipkeys = True)
    r.hset('Queuemon',qname,jsonqmsg)

@manager.register_event('QueueMemberStatus')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    mmsg = dict(msg)
    normalizer={}
    for val in normagents:
        normalizer[val]=mmsg[val]

    for val in normagents:
        normalizer[val]=mmsg[val]

    mname=normalizer['Interface']
    mq=normalizer['Queue']
    mname={mname:mq}
    mmsg=normalizer
        
    jmmane=json.dumps(mname,skipkeys = True)
    jsonqmsg=json.dumps(mmsg,skipkeys = True)
    r.hset('Agentmon',jmmane,jsonqmsg)

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

        
    jmmane=json.dumps(mname,skipkeys = True)
    jsonqmsg=json.dumps(mmsg,skipkeys = True)

    r.hset('Agentmon',jmmane,jsonqmsg)


            
#@manager.register_event('QueueMemberPause')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    msg = dict(msg)
#    print(msg)
#    redstring=r.hgetall(msg['Linkedid'])
#    channame=splitchan(msg['Interface'])
#    channum=splitchan(msg['Interface'])
#    starsideid=getsid(channame)
#    qit=str(redstring['QuIterator'])
#    qit=(str(qit))
#
#
#    print(starsideid,channame)
#    for key,value  in redstring.items():
#        if msg['Queue'] in value and re.search(r'Qu_'+qit, key):
#            qname=key
#            r.hset(msg['Linkedid'], qname+':'+channum+':'+str(starsideid)+':stop',msg['Timestamp'])
        

@manager.register_event('BridgeEnter')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    #print(msg)
    r.hset(msg['Linkedid'], "Main_Status", 'ANSWER')
    redstring=r.hgetall(msg['Linkedid'])

    if "Iterator" in redstring:
        itera = int(redstring["Iterator"])
        talks=[]
        comparer=[]
        tmpcomparer=[]
        fincom=[]
        talkslist=[]
        for key,value in redstring.items():
            if key == "Up_time":
                if not re.search(r'trsf', msg['Channel']):
                    summuptime = float(msg['Timestamp']) - float(value)
                    if float(summuptime) > 0.1:
                        tempupa=float(msg['Timestamp'])
                        tempupb=float(summuptime)
                        tempupc=float(tempupa-tempupb)
                        r.hset(msg['Linkedid'],"Up_time",str(tempupc))
                        a=float(msg['Timestamp'])
                        b=float(summuptime)
                        b=float(summuptime)
                        c=a-b

            if value == msg['Channel'] and not  re.search(r':channel', key) and not  re.search(r'Qu_', key) and not  re.search(r'trsf', key) and not  re.search(r'Br:', key):
                if 'Up_time' in redstring:
                    summuptime = float(msg['Timestamp']) - float(redstring["Up_time"])
                    if float(summuptime) > 0.1:
                        if not redstring[key]+"_answer" in redstring:
                            r.hset(msg['Linkedid'], key+"_answer",msg['Timestamp'])
                        tempupa=float(msg['Timestamp'])
                        tempupb=float(summuptime)
                        tempupc=float(tempupa-tempupb)
                        r.hset(msg['Linkedid'],"Up_time",str(tempupc))
                else:
                    r.hset(msg['Linkedid'], key+"_answer",msg['Timestamp'])
                    r.hset(msg['Linkedid'],"Up_time",msg['Timestamp'])
    
#        for key,value in redstring.items():
 #           print(key,value)
        

        for key,value in redstring.items():
            if value == msg['Uniqueid'] and not re.search(r'Linkedid', key):
                brsid=resizer(msg['BridgeUniqueid'])
                channum=splitchan(msg['Channel'])
                if redstring['SRC'] in msg['Channel']:
                    channum=str(channum)+'_SRC'
                else:
                    channum=str(channum)+'_'+str(itera)
                r.hset(msg['Linkedid'], 'Br:'+str(brsid)+':'+'id',msg['BridgeUniqueid'])
                if 'Br:'+str(brsid)+':'+'start' in redstring:
                    if float(msg['Timestamp'])-float(redstring['Br:'+str(brsid)+':'+'start']) < float(2):
                        r.hset(msg['Linkedid'], 'Br:'+str(brsid)+':'+'chan:'+channum,msg['Channel'])
                    else:
                        r.hset(msg['Linkedid'], 'Br:'+str(brsid)+':'+'chan:'+channum,msg['Channel'])
                else:
                    r.hset(msg['Linkedid'], 'Br:'+str(brsid)+':'+'start',msg['Timestamp'])
                    r.hset(msg['Linkedid'], 'Br:'+str(brsid)+':'+'chan:'+channum,msg['Channel'])


    if 'PickupLinkedid' in redstring:
        #print(redstring['PickupLinkedid'],'!!!!!!!!!!!!!!!!!!!')
        trlinkd=redstring['PickupLinkedid']
        brsid=resizer(msg['BridgeUniqueid'])
        channum=splitchan(msg['Channel'])
        r.hset(trlinkd, 'Br:'+str(brsid)+':'+'chan:'+channum,msg['Channel'])


    redstring=r.hgetall(msg['Linkedid'])
#    for key,value in redstring.items():
#        print(key,value)

    if redstring:
        if int(msg['BridgeNumChannels']) > 1:
            brsid=resizer(msg['BridgeUniqueid'])
            print ('Connect detected',msg['BridgeNumChannels'],1)
            brsearch='Br:'+str(brsid)+':'+'chan:'
            for key,value in redstring.items():
                if  re.search(r"Br:\d+:chan", key):
                    if not value in talks:
                        if not re.search(r"forwards", msg['Channel']):
                            tchan=value
                            talks.append(tchan)
            if not msg['Channel'] in talks:
                if not re.search(r"Local", msg['Channel']):
                    if not value in talks:
                        talks.append(msg['Channel'])
            #    print('why is it here', msg['Channel'],talks)

#        print(talks,len(talks))

        if len(talks) > 1:
            for key,value in redstring.items():
                if re.search(r"Br:talk:\d+:start", key):
                    talkslist.append(value)
#            print('talklist',talkslist)

            talkit=redstring['TlkIterator']
            for key,value in redstring.items():
                if re.search(rf"Br:talk:{str(talkit)}:\d", key):
                    #print('**************************************',key,value)
                    comparer.append(value)
#            print(comparer,talks,"******************************************")
#            print(list(set(comparer) & set(talks)),"*******************intersect***********************")
            fincom=list(set(comparer) & set(talks))
            print('compare',fincom)
            if not len(fincom) == len(talks):
                print('Something new, go')
                talkit=int(talkit)+1
                r.hset(msg['Linkedid'],'TlkIterator',talkit)
                r.hset(msg['Linkedid'], 'Br:talk:'+str(talkit)+':start',msg['Timestamp'])
            talkit= int(r.hget(msg['Linkedid'], "TlkIterator"))
            for i,x in enumerate(talks):
             #   print(i,x)
                if not re.search(r"Localz", x):
              #      if not x in tmpcomparer:
                    r.hset(msg['Linkedid'], 'Br:talk:'+str(talkit)+':'+str(i),x)
                    #tmpcomparer.append(value)

@manager.register_event('BridgeLeave')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    brsid=resizer(msg['BridgeUniqueid'])
    redstring=r.hgetall(msg['Linkedid'])
    if not re.search(r'Local', msg['Channel']):
        brsid=resizer(msg['BridgeUniqueid'])
    for key,value in redstring.items():
        if value == msg['Channel'] and 'Br:'+str(brsid)+':'+'chan:' in key:
#            print(key,value,'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DELETE FROM BRIDGES')
            r.hdel(msg['Linkedid'],key)
    
#@manager.register_event('BridgeMerge')  # Register all events
#async def ami_callback(mngr: Manager, msg: Message):
#    msg = dict(msg)
    #print(msg)

@manager.register_event('QueueEntry')  # Register all events
async def ami_callback(mngr: Manager, msg: Message):
    msg = dict(msg)
    active=r.lrange('activechan_id', 0, -1)
    for i,x in enumerate(active):
        redict=x
        if  x == msg['Uniqueid']:
            qit=int(r.hget(msg['Uniqueid'], "QuIterator"))
            qit=(str(qit))
            r.hset(msg['Uniqueid'], "Qu_"+qit+"_position", msg['Position'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    manager.on_connect = on_connect
    manager.on_login = on_login
    manager.on_disconnect = on_disconnect
    manager.connect(run_forever=True, on_startup=on_startup, on_shutdown=on_shutdown)
