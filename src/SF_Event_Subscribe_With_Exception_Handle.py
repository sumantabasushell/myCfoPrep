# Purpose of this code is to perform following set of activities:
#       1. Read Salesforce Credentials from .properties file
#       2. Asynchronously create a Salesforce Client object using the 
#          credentials configured as connect app in Salesforce
#       3. Subscribe to the specific Salesforce topic for Platform Events 
#          (Platform Event must be configured in Salesforce)
#       4. Asynscronously wait for new Platform events
#       5. Whenever new message found, write that back into RedPanda topic.
#       6. This program is internally calling another application for the 
#          purpose of writing message into RedPanda, however, it is recommended
#          to write python code to produce messages into RedPanda topic



import logging
import asyncio
import configparser
import subprocess
import sys 
from io import StringIO


from aiosfstream import SalesforceStreamingClient

logging.basicConfig(format='Date-Time : %(asctime)s : Line No. : %(lineno)d - %(message)s', \
                    level = logging.DEBUG,filename = 'Salesforce-Redpanda-Connector.log',filemode = 'w')




v_eventName = "AccountEvent__e" 
v_sandbox = False

config = configparser.ConfigParser()

## Read configurations details from properties file

config.read('SalesforceConnection.properties')

async def streaming_events():
    """ This asynchronous subroutine connects to Salesforce org and listen for events """
   
    print()
    logging.info("Welcome to the Salesforce Platform Event listener.")
    logging.info("****************************************************************")
    print()
  
    
    v_ptfevt = "/event/"+v_eventName

    v_username=config.get("sf", "username")
    v_password=config.get("sf", "password")
    v_consumer_key=config.get("sf", "consumer_key")
    v_consumer_secret=config.get("sf", "consumer_secret")


    # asynchronously create client object out of SalesforceStreamingClient
    async with SalesforceStreamingClient(
            consumer_key=v_consumer_key, 
            consumer_secret=v_consumer_secret, 
            username=v_username, 
            password=v_password, 
            sandbox=v_sandbox) as sfclient:

    # subscribe to Salesforce topics (via Platform Events)
        logging.info("Connection successful to Salesforce!")
        logging.info("Subscribing to the Platform Event ...")
        await sfclient.subscribe(v_ptfevt)
        
        # logging.basicConfig(format='Date-Time : %(asctime)s : Line No. : %(lineno)d - %(message)s', \
        #            level = logging.DEBUG,filename = 'Salesforce-Redpanda-Connector.log',filemode = 'w')
        
        logging.debug("Platform event subscription successful!")
        logging.debug("Listening for new message ...")
        # listen for incoming messages
        async for events in sfclient:
            topic = events["channel"]
            data = events["data"]
            payload = events["data"]["payload"]

          
            logging.debug ("New event identified!")
            logging.debug (f"Payload is: {payload}")
            print(f"Payload is: {payload}")
            messg = str(payload)
            
        
            ## Read invoke a different application to write message into RedPanda Topic.
            ## Will recommend to implement the code to write to RedPanda directly from within this Python program.

            cmd = ['/usr/local/go/bin/go', 'run','/Users/sumanta.basu/MyRnD/golang/kafka/produce_sf_message.go', messg]

            with subprocess.Popen(cmd, stdout=subprocess.PIPE) as proc:  
                print(proc.stdout.read())
            
if __name__ == "__main__":
    """ This is main module """
    while True: # Repeat till the point there is an Keyboard Interrupt
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(streaming_events())
        except KeyboardInterrupt:
            print()
            print()
            logging.debug("... Keyboard Interrup received ...Stopping the listener...")
            break
       
        except asyncio.exceptions.TimeoutError:
            logging.error (" ... Timeout Exception!")
        except asyncio.exceptions.CancelledError:
            logging.error (" ... cancelled Error!")
        else:
            logging.error (" ... Unknown exception ...trying to connect")
        
