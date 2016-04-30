'''
File: metadata.py

Functions to manage the extraction and saving of attributes (metadata) from various sources.

Metadata may come from the following sources (depending on configuration as defined in configuration file):
    object key (i.e., filename. Parsed via a custom regex as defined in configuration file)
    object update event metadata
    object metadata (if any exists)
    object head
    object body (via a custom plugin)
    companion metdata file

Author: Ken Robbins, March 2016

   Copyright 2016 Novartis Institutes for BioMedical Research

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''

import logging
import unittest
import json
import urllib
import boto3
import esutils
import filenamemeta
import objectmeta
import metafile

Debug = True

s3 = boto3.client('s3')

'''
    # TODO KLR: Optionally save attributes on the S3 object itself
    if configs.get('attachMetadataToObject', False):
        Hint from stackoverflow
        k.metadata.update({'myKey':'myValue'})
        k2 = k.copy(k.bucket.name, k.name, k.metadata, preserve_acl=True)
        k2.metadata = k.metadata    # boto gives back an object without *any* metadata
        k = k2;
'''


def get_attributes(event, configs):
    '''
    Use various methods, guided by configuration parameters, to extract meta data about the file.
    event is an event passed to a Lambda function in response to an s3 create or update action.
    configs is a dictionary of configs

    Returns: dictionary with key/value pairs
    '''
    # Event extracted attributes
    attributes = get_attributes_from_event(event)
    bucket = attributes['bucket']
    key = attributes['key']
    parts = key.split('/')
    if len(parts) >= 2:
        prefix = parts[0] # Expect to be 'data' or 'meta'

    # Companion metadata file extracted attributes
    metafileFormat = configs.get('metafileFormat', 'json') # One of  "json", "csv", "custom"
    metafileParserModule = configs.get('metafileParserModule', None) # Consider empty string the same as None
    metafileMode = configs.get('metafileMode', 'disable') # One of "disable", "written_first", "written_last"
    if metafileMode != 'disable':
        metafileAttributes = metafile.get_attributes_from_metadatafile(s3, bucket, key, metafileFormat, metafileParserModule)
        if metafileAttributes is not None:
            attributes.update(metafileAttributes)
        else:
            pass # Silently ignore no matches for now

    # Filename extracted attributes
    dataFilenameRegex = configs.get('dataFilenameRegex', '')
    if len(dataFilenameRegex) > 0:
        key_attributes = filenamemeta.get_attributes_from_filename(key, dataFilenameRegex)
        if key_attributes is not None:
            attributes.update(key_attributes)
        else:
            pass # Silently ignore no filename matches for now

    # S3 object attributes (head and/or body)
    inspectHead = configs.get('inspectS3head', False)
    getMetadataFromS3Object = configs.get('getMetadataFromObject', False)
    dataMaxBodyBytes = configs.get('dataBodyParserMaxBytes', 0)
    dataPlugin = configs.get('dataBodyParserModule', '')
    s3attributes = objectmeta.get_attributes_from_object(s3, bucket, key,
            inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, dataPlugin)
    if s3attributes is not None:
            attributes.update(s3attributes)
    else:
        pass # Silently ignore no matches for now
 
    return attributes

def save_attributes(attributes, esEndpoint):
    '''
    Store the extracted attributes into a search index.
    In the future we could also save values to a database, but I think that ElasticSearch
    seems to solve most needs so far.

    Returns: objectId on success, None otherwise
    '''
    if Debug:
        logging.info('Attributes: ' + str(attributes))

    objectId = esutils.indexAttributes(attributes, esEndpoint)
    if objectId is None:
        logging.error('TODO KLR: Decide what to do when index fails. Perhaps write to a queue that is is connected to SNS?')
        return None
    else:
        return objectId

def get_attributes_from_event(event):
    '''
    Extract attributes from the event that we want to index

    Return a new dict with the attributes inserted
    '''
    region = event['Records'][0]['awsRegion']
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    user = event['Records'][0]['userIdentity']['principalId']
    size = event['Records'][0]['s3']['object']['size']

    attributes = {
            'region': region,
            'bucket': bucket,
            'key': key,
            'user': user,
            'size': size, # Keep this or rely on ContentLength?
            }
    # We should use the eventName ('s3:ObjectCreated:' prefix) and current time
    # to a CreatedTime since that is otherwise lost

    return attributes


#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        import configutils
        self.configs = configutils.load_configs()

        ''' Assumes that a test file has been uploaded and matches the expected name and content '''
        self.region = self.configs['region']
        self.bucket = self.configs['bucket']
        self.assayId = 'a1234'
        self.runId = '15'
        self.key = 'data/unittest-{}-{}-imager_1234567890.tif'.format(self.assayId, self.runId)
        self.user = 'aPrincipalId'
        self.contentLength = 115
        self.size = 1024

        self.event = {
          "Records": [
            {
              "eventVersion": "2.0",
              "eventTime": "1970-01-01T00:00:00.000Z",
              "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
              },
              "s3": {
                "configurationId": "testConfigRule",
                "object": {
                  "eTag": "0123456789abcdef0123456789abcdef",
                  "sequencer": "0A1B2C3D4E5F678901",
                  "key": self.key,
                  "size": self.size
                },
                "bucket": {
                  "arn": "arn:aws:s3:::" + self.bucket,
                  "name": self.bucket,
                  "ownerIdentity": {
                    "principalId": "EXAMPLE"
                  }
                },
                "s3SchemaVersion": "1.0"
              },
              "responseElements": {
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                "x-amz-request-id": "EXAMPLE123456789"
              },
              "awsRegion": self.region,
              "eventName": "ObjectCreated:Put",
              "userIdentity": {
                "principalId": self.user
              },
              "eventSource": "aws:s3"
            }
          ]
        }

        self.expected_head_attributes = {
                'LastModified': '2016-03-26T16:14:13+00:00',
                'ContentLength': self.contentLength
                }

        self.expected_event_attributes = {
                'region': self.region,
                'bucket': self.bucket,
                'user': self.user,
                'key': self.key,
                'size': self.size
                }

        self.expected_body_attributes = {
                'bodykey1': 'value1',
                'bodykey2': 'value2'
                }

        self.expected_filename_attributes = {
                'assayId': self.assayId,
                'runId': self.runId
                }

        self.expected_s3meta_attributes = {
                's3meta1': 'metaValue1',
                's3meta2': 'metaValue2'
                }

        self.expected_attributes = {}
        self.expected_attributes.update(self.expected_event_attributes)
        self.expected_attributes.update(self.expected_head_attributes)
        self.expected_attributes.update(self.expected_s3meta_attributes)
        self.expected_attributes.update(self.expected_body_attributes)
        self.expected_attributes.update(self.expected_filename_attributes)

    def test_get_all_attributes(self):
        attributes = get_attributes(self.event, self.configs)
        self.assertIsNotNone(attributes.get('LastModified', None), 'Missing the LastModified attribute')
        attributes['LastModified'] = '2016-03-26T16:14:13+00:00'
        self.assertEqual(attributes, self.expected_attributes, 'Attribute dictionaries do not match')

    def test_get_attributes_from_event(self):
        attributes = get_attributes_from_event(self.event)
        self.assertEqual(attributes, self.expected_event_attributes, 'Event dictionaries do not match')

    def test_save_attributes(self):
        expectedObjectId = '{}/{}'.format(self.bucket, self.key)
        objectId = save_attributes(self.expected_attributes, self.configs['esEndpoint'])
        print 'Object ID:', str(objectId)
        self.assertEqual(objectId, expectedObjectId, 'Object Ids do not match')

def AllModuleTests():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestController)
    return unittest.TestSuite([suite1])
        
########
# MAIN #
########
if __name__ == '__main__':
    logFormat = '%(levelname)s:%(asctime)s:HABITAT:%(module)s-%(lineno)d: %(message)s'
    logLevel = logging.INFO
    logging.basicConfig(format=logFormat, level=logLevel)

    unittest.main()
