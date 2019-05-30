# https://google-cloud-python.readthedocs.io/en/stable/firestore/index.html

import base64
import json
import os
import random
import string
import sys
import time
import traceback
from datetime import datetime

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from .env_vars import path_to_firebase_service_account


#------------------------------------------------------------------------------
# Returns an authorized API client by discovering the IoT API
# using the service account credentials JSON.
def get_firestore_client(fb_service_account_json):
    cred = credentials.Certificate(fb_service_account_json)
    firebase_admin.initialize_app(cred)
    return firestore.client()


#------------------------------------------------------------------------------
# Get a firebase client using the firebase auth
fs_client = get_firestore_client(path_to_firebase_service_account)


#------------------------------------------------------------------------------
# Get firebase device registration stats (first step of IoT registration)
def get_firebase_device_reg_stats():

    # get a firestore DB collection of the RSA public keys uploaded by
    # a setup script on the device:
    keys_ref = fs_client.collection(u'devicePublicKeys')

    # snaps = keys_ref.get()  # get snapshots (partial data) of all docs
    # for snap in snaps:
    #    doc = snap.reference
    #    key_id = snap.id
    #    keyd = snap.to_dict()
    #    print(u'doc.id={}, doc={}'.format( key_id, keyd ))
    #    key = keyd['key']
    #    cksum = keyd['cksum']
    #    state = keyd['state']
    #    print('key={}, cksum={}, state={}'.format(key,cksum,state))

    # query the collection for docs in a specific state
    query = keys_ref.where(u'state', u'==', u'verified')
    docs = list(query.get())

    res = {}
    res['verified'] = len(docs)

    query = keys_ref.where(u'state', u'==', u'unclaimed')
    docs = list(query.get())
    res['unclaimed'] = len(docs)

    return res


#------------------------------------------------------------------------------
# Delete the verified keys.
def delete_firestore_verified_keys():
    return delete_firestore_keys(u'verified')


#------------------------------------------------------------------------------
# Delete the unclaimed keys.
def delete_firestore_unclaimed_keys():
    return delete_firestore_keys(u'unclaimed')


#------------------------------------------------------------------------------
# Delete the keys by type.
def delete_firestore_keys(key_type):

#debugrob: In the future use the (new) timestamp field of the doc to get docs that are older than one day.

    if key_type != u'verified' and key_type != u'unclaimed':
        return {}

    keys_ref = fs_client.collection(u'devicePublicKeys')
    query = keys_ref.where(u'state', u'==', key_type)
    doc_snapshots = query.get() # get partial data document snapshots

    for snap in doc_snapshots:
        snap.reference.delete() # de-reference and delete this document

    # now check if they were deleted
    query = keys_ref.where(u'state', u'==', key_type)
    docs = list(query.get())
    res = {}
    res[key_type] = len(docs)
    return res  # return the updated number of keys





