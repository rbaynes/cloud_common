#!/usr/bin/env python3

""" Deprecated image chunking class.
    - Still used by older brains to send base64 encoded chunks of images
      in MQTT messages.  
    - Deprecated because the new brain code uses a simpler and faster
      method to upload images (curl POST to a cloud function).
    - This code can be removed when all our old v3 PFCs (and containers) 
      are all upgraded to the latest v5 brain.  Hopefully in Fall 2019.
"""

import sys, logging, ast, time, traceback, base64
from datetime import datetime

from typing import Dict

from google.cloud import datastore as gcds

from cloud_common.cc import utils 
from cloud_common.cc.google import env_vars 
from cloud_common.cc.google import storage 
from cloud_common.cc.google import datastore 
from cloud_common.cc.google import bigquery 


# You get the idea this is deprecated old code, right?  It will go away soon.
class DeprecatedImageChunking:

    # common keys
    var_KEY = 'var'
    values_KEY = 'values'
    messageType_KEY = 'messageType'
    messageType_Image = 'Image' 

    # message keys valid for any image message (even new uploads one)
    varName_KEY = 'varName'
    imageType_KEY = 'imageType'

    # deprecated keys for the deprecated image chunk pubsub message.
    chunk_KEY = 'chunk'
    totalChunks_KEY = 'totalChunks'
    imageChunk_KEY = 'imageChunk'
    messageID_KEY = 'messageID'


    #--------------------------------------------------------------------------
    def __init__(self):
        self.DS = datastore.create_client()
        if self.DS is None:
            logging.critical('deprecated_image_chunking has no DS')


    # This datastore code is here (instead of in cc.google.datastore) 
    # because it is DEPRECATED and I don't want to be able to delete it without
    # conflict.

    #--------------------------------------------------------------------------
    # Save a partial b64 chunk of an image to a cache in the datastore.
    def saveImageChunkToDatastore(self, deviceId, messageId, varName, \
            imageType, chunkNum, totalChunks, imageChunk ):

        key = self.DS.key( 'MqttServiceCache' )
        # string properties are limited to 1500 bytes if indexed, 
        # 1M if not indexed.  
        chunk = gcds.Entity( key, exclude_from_indexes=['imageChunk'] )
        chunk.update( {
            'deviceId': deviceId,
            'messageId': messageId,
            'varName': varName,
            'imageType': imageType,
            'chunkNum': chunkNum,
            'totalChunks': totalChunks,
            'imageChunk': imageChunk,
            'timestamp': datetime.now()
            } )
        self.DS.put( chunk )  
        logging.debug( 'saveImageChunkToDatastore: saved to MqttServiceCache '
            '{}, {} of {} for {}'.format( 
                messageId, chunkNum, totalChunks, deviceId ))
        return 


    #--------------------------------------------------------------------------
    # Returns list of dicts, each with a chunk.
    def getImageChunksFromDatastore( self, deviceId, messageId ):
        query = self.DS.query( kind='MqttServiceCache' )
        query.add_filter( 'deviceId', '=', deviceId )
        query.add_filter( 'messageId', '=', messageId )
        qiter = list( query.fetch() )
        results = list( qiter )
        resultsToReturn = []
        for row in results:
            pydict = {
                'deviceId': row.get( 'deviceId', '' ),
                'messageId': row.get( 'messageId', '' ),
                'varName': row.get( 'varName', '' ),
                'imageType': row.get( 'imageType', '' ),
                'chunkNum': row.get( 'chunkNum', '' ),
                'totalChunks': row.get( 'totalChunks', '' ),
                'imageChunk': row.get( 'imageChunk', '' ) 
            }
            resultsToReturn.append( pydict )
        return resultsToReturn


    #--------------------------------------------------------------------------
    def deleteImageChunksFromDatastore( self, deviceId, messageId ):
        query = self.DS.query( kind='MqttServiceCache' )
        query.add_filter( 'deviceId', '=', deviceId )
        query.add_filter( 'messageId', '=', messageId )
        qiter = query.fetch()
        for entity in qiter:
            self.DS.delete( entity.key )
            logging.debug( "deleteImageChunksFromDatastore: chunk {} of messageId {} deleted.".format( entity.get( 'chunkNum', '?' ), messageId ))
        return


    #--------------------------------------------------------------------------
    # Save the ids of an invalid image, so we can clean up the cache.
    def saveTurd( self, deviceId, messageId ):
        key = self.DS.key( 'MqttServiceTurds' )
        turd = gcds.Entity( key )
        turd.update( {
            'deviceId': deviceId,
            'messageId': messageId,
            'timestamp': datetime.now()
            } )
        self.DS.put( turd )  
        logging.debug( 'saveTurd: saved to MqttServiceTurds {} for {}'.format( 
                messageId, deviceId ))
        return 


    #--------------------------------------------------------------------------
    # Returns list of dicts, each with a chunk.
    def getTurds( self, deviceId ):
        resultsToReturn = []
        query = self.DS.query( kind='MqttServiceTurds' )
        if query is None:
            return resultsToReturn
        query.add_filter( 'deviceId', '=', deviceId )
        qiter = list( query.fetch() )
        results = list( qiter )
        for row in results:
            pydict = {
                'deviceId': row.get( 'deviceId', '' ),
                'messageId': row.get( 'messageId', '' )
            }
            resultsToReturn.append( pydict )
        return resultsToReturn


    #--------------------------------------------------------------------------
    def deleteTurd( self, deviceId, messageId ):
        query = self.DS.query( kind='MqttServiceTurds' )
        query.add_filter( 'deviceId', '=', deviceId )
        query.add_filter( 'messageId', '=', messageId )
        qiter = query.fetch()
        for entity in qiter:
            self.DS.delete( entity.key )
        logging.debug( "deleteTurd: messageId {} deleted.".format( messageId ))
        return


    #--------------------------------------------------------------------------
    # Parse and save the image chunk from the old device (brain) code.
    def save_old_chunked_image(self, pydict, deviceId):
        try:
            # each received image message must have these fields
            if not utils.key_in_dict( pydict, self.varName_KEY ) or \
                    not utils.key_in_dict( pydict, self.imageType_KEY ) or \
                    not utils.key_in_dict( pydict, self.chunk_KEY ) or \
                    not utils.key_in_dict( pydict, self.totalChunks_KEY ) or \
                    not utils.key_in_dict( pydict, self.imageChunk_KEY ) or \
                    not utils.key_in_dict( pydict, self.messageID_KEY ):
                logging.error('save_old_chunked_image: Missing key(s) in dict.')
                return

            messageId =   pydict[ self.messageID_KEY ]
            varName =     pydict[ self.varName_KEY ]
            imageType =   pydict[ self.imageType_KEY ]
            chunkNum =    pydict[ self.chunk_KEY ]
            totalChunks = pydict[ self.totalChunks_KEY ]
            imageChunk =  pydict[ self.imageChunk_KEY ]

            # Get rid of all chunks if we receive one bad chunk - so we don't 
            # make bad partial images.
            if 0 == len(imageChunk):
                logging.error( "save_old_chunked_image: received empty imageChunk from {}, cleaning up turds".format( deviceId ))
                self.deleteImageChunksFromDatastore( deviceId, messageId )
                self.saveTurd( deviceId, messageId )
                return

            # Clean up any smelly old turds from previous images (if they don't
            # match the current messageId from this device).
            turds = self.getTurds( deviceId )
            for badImage in turds:
                badMessageId = badImage['messageId'] 
                if badMessageId != messageId:
                    self.deleteImageChunksFromDatastore(deviceId, badMessageId)
                    self.deleteTurd( deviceId, badMessageId )

            # Save this chunk to the datastore cache.
            self.saveImageChunkToDatastore(deviceId, messageId, varName, 
                imageType, chunkNum, totalChunks, imageChunk )

            # For every message received, check data store to see if we can
            # assemble chunks.  Messages will probably be received out of order.

            # Start with a list of the number of chunks received:
            listOfChunksReceived = []
            for c in range( 0, totalChunks ):
                listOfChunksReceived.append( False )

            # What chunks have we already received? 
            oldChunks = self.getImageChunksFromDatastore( deviceId, messageId )
            for oc in oldChunks:
                listOfChunksReceived[ oc[ 'chunkNum' ] ] = True
                logging.debug( 'save_old_chunked_image: received {} of {} '
                    'for messageId={}'.format( oc[ 'chunkNum'], 
                        totalChunks, messageId))

            # Do we have all chunks?
            haveAllChunks = True
            chunkCount = 0 
            for c in listOfChunksReceived:
                logging.debug( 'save_old_chunked_image: listOfChunksReceived [{}]={}'.format(
                    chunkCount, c))
                chunkCount += 1 
                if not c:
                    haveAllChunks = False
            logging.debug( 'save_old_chunked_image: haveAllChunks={}'.format(haveAllChunks))

            # No, so just add this chunk to the datastore and return
            if not haveAllChunks:
                logging.debug('save_old_chunked_image: returning to wait for more chunks')
                return

            # YES! We have all our chunks, so reassemble the binary image.

            # Delete the temporary datastore cache for the chunks
            self.deleteImageChunksFromDatastore( deviceId, messageId )
            self.deleteTurd( deviceId, messageId )

            # Sort the chunks by chunkNum (we get messages out of order)
            oldChunks = sorted( oldChunks, key=lambda k: k['chunkNum'] )

            # Reassemble the b64 chunks into one string (in order).
            b64str = ''
            for oc in oldChunks:
                b64str += oc[ 'imageChunk' ]
                logging.debug( 'save_old_chunked_image: assemble {} of {}'.format( 
                    oc[ 'chunkNum' ], oc['totalChunks'] ))
            
            # Now covert our base64 string into binary image bytes
            imageBytes = base64.b64decode( b64str )

            # Put the image bytes in cloud storage as a file, and get an URL
            publicURL = storage.saveFile( varName, imageType,
                    imageBytes, deviceId )
        
            # Put the URL in the datastore for the UI to use.
            datastore.saveImageURL( deviceId, publicURL, varName )

            # Put the URL as an env. var in BQ.
            message_obj = {}
            message_obj[ self.messageType_KEY ] = self.messageType_Image
            message_obj[ self.var_KEY ] = varName
            valuesJson = "{'values':["
            valuesJson += "{'name':'URL', 'type':'str', 'value':'%s'}" % \
                                ( publicURL )
            valuesJson += "]}"
            message_obj[ self.values_KEY ] = valuesJson
            rowsList = []
            self.makeBQEnvVarRowList(message_obj, deviceId, rowsList)
            bigquery.data_insert(rowsList)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.critical( "Exception in save_old_chunked_image(): %s" % e)
            traceback.print_tb( exc_traceback, file=sys.stdout )


    #--------------------------------------------------------------------------
    # Make a BQ row that matches the table schema for the 'vals' table.
    # (python will pass only mutable objects (list) by reference)
    def makeBQEnvVarRowList(self, valueDict, deviceId, rowsList):
        # each received EnvVar type message must have these fields
        if not utils.key_in_dict(valueDict, self.var_KEY ) or \
           not utils.key_in_dict(valueDict, self.values_KEY ):
            logging.error('makeBQEnvVarRowList: Missing key(s) in dict.')
            return

        idKey = 'Env'
        varName = valueDict[ self.var_KEY ]
        values = valueDict[ self.values_KEY ]

        # clean / scrub / check the values.  
        deviceId = deviceId.replace( '~', '' ) 
        varName = varName.replace( '~', '' ) 

        # NEW ID format:  <KEY>~<valName>~<created UTC TS>~<deviceId>
        ID = idKey + '~{}~{}~' + deviceId

        row = (ID.format(varName, 
            time.strftime('%FT%XZ', time.gmtime())), # id column
            values, 0, 0) # values column, with zero for X, Y

        rowsList.append(row)

