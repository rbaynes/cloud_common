#!/usr/bin/env python3

""" Notifications class.
    
    - Handles recipe start/stop/end messages.
    - Uses the Scheduler class.
    - User the Runs class.

    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import os, sys, json, argparse, logging, signal

from typing import Dict, Callable

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import pubsub # takes 15 secs to load...
from cloud_common.cc.notifications.scheduler import Scheduler
from cloud_common.cc.notifications.runs import Runs

class Notifications:

    # Message types we send and receive
    recipe_start: str = 'recipe_start' # new recipe started
    recipe_stop: str  = 'recipe_stop'  # user stopped it
    recipe_end: str   = 'recipe_end'   # recipe concluded naturally

    # Message dict keys
    device_ID_key: str    = "device_ID"
    message_type_key: str = "message_type"
    message_key: str      = "message"

    # Notification types for display to user
    nofification_type_OK: str = "OK"   # show an OK button for user to click
    # other types later, such as Yes/No, Error, Warning, etc

    # Notification dict keys
    nofification_ID_key: str            = "ID"
    nofification_type_key: str          = "type"
    nofification_message_key: str       = "message"
    nofification_created_key: str       = "created"
    nofification_acknowledged_key: str  = "acknowledged"

    # For logging
    name: str = 'cloud_common.cc.notifications.notifications'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        pass # nothing to do to construct this class


    #--------------------------------------------------------------------------
    # Publish a message to the notifications topic (defined in env. var).
    def publish(self, device_ID: str, message_type: str, message: str) -> None:

        if len(device_ID) == 0:
            logging.error(f'{self.name}.publish: invalid device_ID.')
            return

        if message_type is not self.recipe_start and \
           message_type is not self.recipe_stop and \
           message_type is not self.recipe_end:
            logging.error(f'{self.name}.publish: '
                    f'invalid message_type={message_type}')
            return

        message = {
            self.device_ID_key: device_ID,
            self.message_type_key: message_type,
            self.message_key: message,
        }
        pubsub.publish(env_vars.cloud_project_id, 
            env_vars.notifications_topic_subs, message)


    #--------------------------------------------------------------------------
    # Validate the pubsub message we received.
    # Returns True for valid, False otherwise.
    def validate_message(self, message: Dict[str, str]) -> bool:
        if not utils.key_in_dict(message, self.device_ID_key):
            return False
        if not utils.key_in_dict(message, self.message_type_key):
            return False
        if not utils.key_in_dict(message, self.message_key):
            return False
        message_type = message.get(self.message_type_key)
        if not (message_type == self.recipe_start or \
                message_type == self.recipe_stop or \
                message_type == self.recipe_end):
            return False
        return True


    #--------------------------------------------------------------------------
    # Parse a pubsub message and take action.
    def parse(self, message: Dict[str, str]) -> None:
        if not self.validate_message(message):
            logging.error(f'{self.name}.parse: invalid message={message}')
            return 

        message_type = message.get(self.message_type_key)
        if message_type == self.recipe_start:
            pass
#debugrob: create scheduler and runs classes first
            """debugrob
Scheduler.add( device_ID, Scheduler.check_fluid, 48 )
Scheduler.add( device_ID, Scheduler.take_measurements, 24 * 7 )
Runs.start( device_ID, value )
            """

        elif message_type == self.recipe_stop:
            pass
            """debugrob
Scheduler.remove_all( device_ID )
Runs.stop( device_ID )
            """

        elif message_type == self.recipe_end:
            pass
            """debugrob
Scheduler.remove_all( device_ID )
Scheduler.add( device_ID, Scheduler.harvest_plant )
Runs.stop( device_ID )
            """

        # for all messages received, call:
        """debugrob
        Scheduler.check( device_ID )
        """


    #--------------------------------------------------------------------------
    # Returns a list of unacknowledged notifications dicts.
    def get_for_device(self, device_ID: str ) -> List[ Dict[ str, str ]]:
        #debugrob
        return [{}]


    #--------------------------------------------------------------------------
    # Find notification by ID and update the acknowledged timestamp to now().
    def ack(self, device_ID: str, notification_ID: str) -> None:
        #debugrob
        return


    #--------------------------------------------------------------------------
    # Add a new notification for this device, set created TS to now().
    # Returns the notification ID string.
    def add(self, device_ID: str, message: str, 
            notification_type: str = nofification_type_OK ) -> str:
        notification_ID = None
        #debugrob
        """
    nofification_ID_key
    nofification_type_key
    nofification_message_key
    nofification_created_key
    nofification_acknowledged_key
        """
        return notification_ID



