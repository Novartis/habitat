'''
File: habitat_handler.py

Called by an S3 create/update event trigger.

Collect and extract metadata about and from the object and save the object key and metadata into a search index.
Metadata may come from a variety of sources (depending on configuration as defined in configuration file)

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
import metadata
import configutils

Debug = True

logFormat = '%(levelname)s:%(asctime)s:HABITAT:%(module)s-%(lineno)d: %(message)s'
logLevel = logging.INFO
logging.basicConfig(format=logFormat, level=logLevel)
logging.info('Loading lambda function habitat_handler...')


def event_handler(event, context):
    logging.info('Received s3 object event... ')
    logging.debug('Event = ' + json.dumps(event, indent=4))

    configs = configutils.load_configs()

    attributes = metadata.get_attributes(event, configs)
    if attributes is None:
        logging.error('get_attributes returned None. Nothing will be indexed')
        return False

    objectId = metadata.save_attributes(attributes, configs['esEndpoint'])
    if objectId is not None:
        logging.info('Successfully handled s3 object created/updated event. objectID=' + objectId)
        return objectId
    else:
        logging.error('save_attributes returned an error. Indexing may not be complete.')
        return None

#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        self.configs = configutils.load_configs()
        self.bucket = self.configs['bucket']

    def test_handler(self):
        event = {
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
                  "key": "data/unittest-a1234-15-imager_1234567890.tif",
                  "size": 1024
                },
                "bucket": {
                  "arn": "arn:aws:s3:::"+self.bucket,
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
              "awsRegion": "us-east-1",
              "eventName": "ObjectCreated:Put",
              "userIdentity": {
                "principalId": "aPrincipalId"
              },
              "eventSource": "aws:s3"
            }
          ]
        }

        # Get the existing version if already indexed
        # objectId creation assumes a lot and is therefore brittle
        import esutils
        objectId = self.bucket + '/data/unittest-a1234-15-imager_1234567890.tif'
        result = esutils.getById(objectId, self.configs['esEndpoint'])
        if result is not None:
            version = result['_version'] + 1
        else:
            version = 1

        # Do the actual test
        objectId = event_handler(event, None)
        self.assertIsNotNone(objectId, 'event_handler failed')

        # Check the results
        result = esutils.getById(objectId, self.configs['esEndpoint'])
        expected = {
                u'_type': u'habitat-testtype',
                u'_source': {
                    u'bodykey2': u'value2',
                    u'bodykey1': u'value1',
                    's3meta1': u'metaValue1',
                    's3meta2': u'metaValue2',
                    u'LastModified': 'DUMMY_LASTMODIFIED',
                    u'ContentLength': 115,
                    u'runId': u'15',
                    u'bucket': self.bucket,
                    u'user': u'aPrincipalId',
                    u'key': u'data/unittest-a1234-15-imager_1234567890.tif',
                    u'region': u'us-east-1',
                    u'assayId': u'a1234',
                    u'size': 1024
                    },
                u'_index': u'habitatunittest',
                u'_version': version,
                u'found': True,
                u'_id': self.bucket + '/data/unittest-a1234-15-imager_1234567890.tif'
                }

        source = result['_source']
        self.assertIsNotNone(source.get('LastModified', None), 'Missing the LastModified attribute')
        result['_source']['LastModified'] = 'DUMMY_LASTMODIFIED'
        self.assertEqual(result, expected, 'Get from ES did not match what was expected')

def AllModuleTests():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestController)
    return unittest.TestSuite([suite1])

########
# MAIN #
########
if __name__ == '__main__':
    unittest.main()
