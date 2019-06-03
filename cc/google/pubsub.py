# https://google-cloud-python.readthedocs.io/en/stable/pubsub/

from google.cloud import pubsub

from .env_vars import *


# Pub-sub client for Google Cloud, created when this module is loaded.
_pubsub_client = pubsub.SubscriberClient()


#------------------------------------------------------------------------------
# Note: this function never returns.
# Subscribe for pub-sub messages.
# Args:
#   project: the google cloud project name.
#   subscription: the subscription name.
#   callback: function to be called when a message is received.
#             function needs to call message.ack() when done.
def subscribe(project, subscription, callback):

    # create our gcloud project + subscription path
    subs_path = _pubsub_client.subscription_path(project, subscription)

    # subscribe for messages
    logging.info(f'Waiting for message sent to {subs_path}')

    # in case of subscription timeout, use a loop to resubscribe.
    while True:  
        try:
            future = _pubsub_client.subscribe(subs_path, callback)

            # result() blocks until future is complete 
            # (when message is ack'd by server)
            message_id = future.result()
            logging.debug('\tmessage_id: {}'.format(message_id))

        except Exception as e:
            logging.critical(e)







