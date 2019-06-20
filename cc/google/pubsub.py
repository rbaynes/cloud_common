"""
https://google-cloud-python.readthedocs.io/en/stable/pubsub/
"""

import logging, json

from google.cloud import pubsub

from typing import Dict, Callable


# Globals
__subs_client = None
__pubs_client = None


#------------------------------------------------------------------------------
# Clients for google cloud pubsub.
# This takes 15 seconds or so to happen.
def create_clients() -> None:
    global __subs_client
    global __pubs_client 

    if __subs_client is None:
        __subs_client = pubsub.SubscriberClient()

    if __pubs_client is None:
        __pubs_client = pubsub.PublisherClient()


#------------------------------------------------------------------------------
# Note: this function never returns.
# Subscribe for pub-sub messages.
# Args:
#   project: the google cloud project name.
#   subscription: the subscription name.
#   callback: function to be called when a message is received.
#             function needs to call message.ack() when done.
def subscribe(project: str, subscription: str, callback: Callable) -> None:

    # on demand client creation
    global __subs_client
    create_clients()

    # create our gcloud project + subscription path
    subs_path = __subs_client.subscription_path(project, subscription)

    # subscribe for messages
    logging.info(f'Waiting for message sent to {subs_path}')

    # in case of subscription timeout, use a loop to resubscribe.
    while True:  
        try:
            future = __subs_client.subscribe(subs_path, callback)

            # result() blocks until future is complete 
            # (when message is ack'd by server)
            message_id = future.result()
            logging.debug('\tmessage_id: {}'.format(message_id))

        except Exception as e:
            logging.critical(f'cloud_common.cc.google.pubsub.subscribe: {e}')


#------------------------------------------------------------------------------
# Publish a pub-sub message.
# Args:
#   project: the google cloud project name.
#   topic: the pub-sub topic we publish the message to.
#   message: the dict to publish 
def publish(project: str, topic: str, message: Dict) -> None:
    try:
        # on demand client creation
        global __pubs_client 
        create_clients()

        message_json = json.dumps(message)
        path = f'projects/{project}/topics/{topic}'
        logging.debug(f'publishing: {message_json} to {path}')
        __pubs_client.publish(path, message_json.encode('utf-8'))

    except Exception as e:
        logging.error(f'cloud_common.cc.google.pubsub.publish: {e}')





