#!/usr/bin/env python3

""" Notification Messaging class.
    
    - Handles recipe start/stop/end messages.
    - Uses the Scheduler class.
    - Uses the Runs class.

    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import os, sys, json, argparse, logging, signal

from typing import Dict

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import pubsub # takes 15 secs to load...
from cloud_common.cc.notifications.scheduler import Scheduler
from cloud_common.cc.notifications.runs import Runs

class NotificationMessaging:

    # Message types we send and receive
    recipe_start: str = 'recipe_start' # new recipe started
    recipe_stop: str  = 'recipe_stop'  # user stopped it
    recipe_end: str   = 'recipe_end'   # recipe concluded naturally

    # Message dict keys
    device_ID_key: str    = "device_ID"
    message_type_key: str = "message_type"
    message_key: str      = "message"  # recipe_name for the recipe_* messages

    # For logging
    name: str = 'cloud_common.cc.notifications.notification_messaging'


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

        s = Scheduler()
        r = Runs()

        device_ID    = message.get(self.device_ID_key)
        message_type = message.get(self.message_type_key)
        recipe_name  = message.get(self.message_key)

        if message_type == self.recipe_start:
            s.add(device_ID, Scheduler.check_fluid_command, 48)
            s.add(device_ID, Scheduler.take_measurements_command, 24 * 7)
            r.start(device_ID, recipe_name)

        elif message_type == self.recipe_stop:
            s.remove_all_commands(device_ID)
            r.stop(device_ID)

        elif message_type == self.recipe_end:
            s.remove_all_commands(device_ID)
            s.add(device_ID, Scheduler.harvest_plant_command, 0)
            r.stop(device_ID)

        # For all messages received, check the schedule to see if anything
        # is due to be run.
        s.check(device_ID)



