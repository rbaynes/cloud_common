#!/usr/bin/env python3

""" Scheduler class.
    - Maintains a schedule of repeating events in datastore.
    Design: https://github.com/OpenAgricultureFoundation/notification-service/blob/master/docs/API.pdf
"""

import datetime as dt
import json, logging, pprint

from typing import Dict, List

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import datastore


"""
Store schedule in datastore.DeviceData<device_ID>.schedule as a dict:
{
    command: str <command>,
    message: str <message to display>,
    run_at:  str <timestamp to run on>,
    repeat:  int <number of hours, can be 0 for a one time command>,
    count:   int <execution count>
}
"""
class Scheduler:

    # Keys used in DeviceData.schedule dict we store.
    command_key = 'command'
    message_key = 'message'
    run_at_key  = 'run_at'
    repeat_key  = 'repeat'
    count_key   = 'count'

    # DeviceData property
    schedule_property = "schedule"

    # Commands
    check_fluid_command       = 'check_fluid'
    take_measurements_command = 'take_measurements'
    harvest_plant_command     = 'harvest_plant'

    # Sub keys
    default_repeat_hours_key = 'default_repeat_hours'

    # Commands, there can only be one of each per device.
    commands = {
        check_fluid_command: 
            {message_key: 'Check your fluid level', 
             default_repeat_hours_key: 48},
        take_measurements_command: 
            {message_key: 'Record your plant measurementsl', 
             default_repeat_hours_key: 24},
        harvest_plant_command: 
            {message_key: 'Time to harvest your plant', 
             default_repeat_hours_key: 0},
    }

    # For logging
    name: str = 'cloud_common.cc.notifications.scheduler'


    #--------------------------------------------------------------------------
    def __init__(self) -> None:
        self.testing_hours = 0
        datastore.create_client(env_vars.cloud_project_id)


    #--------------------------------------------------------------------------
    # Get the list of commands we support for display.
    def get_commands(self) -> str:
        pp = pprint.PrettyPrinter()
        out = pp.pformat(self.commands)
        return f'{self.name} Commands:\n{out}'


    #--------------------------------------------------------------------------
    # Private getter of the schedule property.
    # Returns a list of dicts.
    def __get_schedule(self, device_ID: str) -> List[Dict[str, str]]:
        return datastore.get_device_data_property(device_ID, 
                self.schedule_property)


    #--------------------------------------------------------------------------
    # Private command validator.
    # Returns True if command is valid, False otherwise.
    def __validate_command(self, command: str) -> bool:
        if command in self.commands:
            return True
        return False


    #--------------------------------------------------------------------------
    # Return the schedule for a device.  For testing and debugging.
    def to_str(self, device_ID: str) -> str:
        pp = pprint.PrettyPrinter()
        out = pp.pformat(self.__get_schedule(device_ID))
        return out


    #--------------------------------------------------------------------------
    # Get a command by device ID and command name.
    # Returns the command dict if found, or an empty dict for not found.
    def get_command_dict(self, device_ID: str, command: str) -> Dict[str, str]:
        if not self.__validate_command(command):
            logging.error(f'{self.name}.get_command_dict '
                    f'invalid command {command}')
            return {}
        sched_list = self.__get_schedule(device_ID)
        for cmd_dict in sched_list:
            if cmd_dict.get(self.command_key) == command:
                return cmd_dict
        return {}


    #--------------------------------------------------------------------------
    # Creates a DS dict entry for this command, 
    # setting timestamp = now() + hours and count = 0.
    def add(self, device_ID: str, command: str, repeat_hours: int = -1) -> None:
        if not self.__validate_command(command):
            logging.error(f'{self.name}.add invalid command {command}')
            return

        # customize the command template
        template = self.commands.get(command, {})
        repeat = template.get(self.default_repeat_hours_key, 0)
        if repeat_hours >= 0:
            repeat = repeat_hours

        # calculate when to run this command: now + repeat hours
        utc_in_repeat_hours = dt.datetime.utcnow() + dt.timedelta(hours=repeat)
        run_at_time = utc_in_repeat_hours.strftime('%FT%XZ')

        # get any existing dict for this command (or an empty dict)
        cmd_dict = self.get_command_dict(device_ID, command)

        # fill in the command properties
        cmd_dict[self.command_key] = command
        cmd_dict[self.message_key] = template.get(self.message_key, '')
        cmd_dict[self.run_at_key]  = run_at_time
        cmd_dict[self.repeat_key]  = repeat
        cmd_dict[self.count_key]   = 0

        # get the list of all command dicts
        sched_list = self.__get_schedule(device_ID)

        # see if this command already exists in the list
        cmd_index = -1
        for i in range(len(sched_list)):
            if sched_list[i].get(self.command_key) == command:
                cmd_index = i
                break
        if cmd_index >= 0:
            sched_list[cmd_index] = cmd_dict # yes, so replace it
        else:
            sched_list.append(cmd_dict)      # no, so append it

        # Store in datastore.DeviceData<device_ID>.schedule as a list of dicts:
        datastore.save_list_as_device_data_queue(device_ID, 
                self.schedule_property, sched_list)
        logging.debug(f'{self.name}.add-ed to list: {sched_list}')


    #--------------------------------------------------------------------------
    # Remove a command for this device, from the list.
    def remove_command(self, device_ID: str, command: str) -> None:
        new_list = []

        # get the list of all command dicts
        sched_list = self.__get_schedule(device_ID)

        # see if this command already exists in the list
        for cd in sched_list:
            if cd.get(self.command_key) != command:
                # build a new list that doesn't include the removed command
                new_list.append(cd)
        if len(new_list) > 0:
            datastore.save_list_as_device_data_queue(device_ID, 
                self.schedule_property, new_list)
        logging.debug(f'{self.name}.remove_command {command} from list: {new_list}')


    #--------------------------------------------------------------------------
    # Removes all commands for this device.
    def remove_all_commands(self, device_ID: str) -> None:
        # save an empty list
        datastore.save_list_as_device_data_queue(device_ID, 
                self.schedule_property, [])
        logging.debug(f'{self.name}.remove_all_commands from list.')


    #--------------------------------------------------------------------------
    # Set the number of hours, for use when testing check().
    def testing_hours(self, hours: int = 0) -> None:
        self.testing_hours = hours
#debugrob: use above


    #--------------------------------------------------------------------------
    # Check the schedule for this device to see if there is anything to run.
    def check(self, device_ID: str) -> None:
#debugrob: fix here down
        return
        """
iterate the schedule entries for device_ID acting upon entries that have a timestamp <= now() 
if a command has a repeat_hours value > 0, then update its timestamp when executing it.
update the count of times the command has been executed.
write notifications the UI will render 
Notifications.add(..)
notification_ID: UUID generated when notification created
message: <yada>
timestamp: <TS in UTC>
acknowledged: <TS in UTC>
Handle init. and term. logic, such as:
if we are repeating the take_measurements command and count == 1, then set the repeat interval to 24 hours.

        cmd_dict[self.count_key]   = cmd_dict.get(self.count_key, 0) + 1
        """


