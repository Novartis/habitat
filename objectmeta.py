'''
File: objectmeta.py

Extract the meta data from the s3 object.
Metadata may come from head, metadata field in head, or body of file itself.

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

import unittest
import logging
import boto3

def get_attributes_from_object(s3, bucket, key, inspectHead, getMetadataFromS3Object, maxBodyBytes, plugin):
    '''
    Extract metadata attributes from the s3 object.
    
    Only read and parse the body if maxBodyBytes is > 0.
    Only extract values from head if inspectHead is True.
    Only extract value from the Metadata field of the S3 object if getMetadataFromS3Object is True.

    When parsing the body of a file, only read and as far as needed up to maxBodyBytes.
    Then runs a custom function on that data to return a dictionary of key/values.
    plugin is the name of the Python module to execute on the body.
    The module must contain a function called parsebody:
         attributes = parsebody(body)
    where body is a string containing the data to be parsed and attributes is a Dict()
    of key/value pairs. The values can be arbitrary.

    s3 is an existing boto3 client object created from: s3 = boto3.client('s3')

    Returns:
        Dictionary of matched key/values extracted according to the various options
        Dictionary may be empty if no matches are found
        None on error
    '''
    try:
        logging.info('Getting object for bucket {} and key {}...'.format(bucket, key))

        if maxBodyBytes > 0:
            response = s3.get_object(Bucket=bucket, Key=key)
        elif inspectHead or getMetadataFromS3Object:
            response = s3.head_object(Bucket=bucket, Key=key)

        attributes = {}

        if maxBodyBytes > 0:
            get_body_attributes_from_s3Response(response, maxBodyBytes, plugin, attributes)

        if inspectHead or getMetadataFromS3Object:
            get_head_attributes_from_s3Response(response, attributes, inspectHead, getMetadataFromS3Object)

        return attributes
    except Exception as e:
        logging.error(e)
        logging.error('Error getting object {} from bucket {}.'.format(key, bucket))
        return None

def get_body_attributes_from_s3Response(s3Response, maxBodyBytes, plugin, attributes):
    '''
    Read the specified number of bytes from the body and update attributes in place.
    Use the custom plugin to parse the content of the body
    '''
    try:
        body = s3Response['Body'].read(maxBodyBytes)
        logging.info('About to run custom data body parser: ' + plugin)
        parsermodule = __import__(plugin)
        body_attributes = parsermodule.parsebody(body)
        attributes.update(body_attributes)
    except:
        logging.error('Problem during read() or parsing of S3 object body')
        raise

def get_head_attributes_from_s3Response(s3Response, attributes, inspectHead, getMetadataFromS3Object):
    ''' Grab the selected attributes and update the attributes dictionary in place '''
    head_attributes = {}

    if inspectHead:
        head_attributes['LastModified'] = s3Response['LastModified']
        head_attributes['ContentLength'] = s3Response['ContentLength']
        attributes.update(head_attributes)

    if getMetadataFromS3Object:
        attributes.update(s3Response['Metadata'])


#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        import configutils
        self.configs = configutils.load_configs()
        self.bucket = self.configs['bucket']
        self.key = 'data/unittest-a1234-15-imager_1234567890.tif'
        self.body_expected = {
                'bodykey1': 'value1',
                'bodykey2': 'value2'
                }
        self.head_expected = {
                'LastModified': 'DUMMY LASTMODIFIED',
                'ContentLength': 115
                }
        self.s3meta_expected = {
                's3meta1': 'metaValue1',
                's3meta2': 'metaValue2'
                }

    def test_get_no_attributes_from_object(self):
        s3 = boto3.client('s3')
        inspectHead = False
        getMetadataFromS3Object = False
        dataMaxBodyBytes = 0
        attributes = get_attributes_from_object(s3, self.bucket, self.key,
                inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, None)
        expected = {}
        self.assertEqual(attributes, expected, 'Attribute dictionaries do not match')

    def test_get_just_head_attributes_from_object(self):
        s3 = boto3.client('s3')
        inspectHead = True
        getMetadataFromS3Object = False
        dataMaxBodyBytes = 0
        attributes = get_attributes_from_object(s3, self.bucket, self.key,
                inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, None)
        self.assertIsNotNone(attributes.get('LastModified', None), 'Missing the LastModified attribute')
        attributes['LastModified'] = 'DUMMY LASTMODIFIED'
        self.assertEqual(attributes, self.head_expected, 'Attribute dictionaries do not match')

    def test_get_just_s3meta_attributes_from_object(self):
        s3 = boto3.client('s3')
        inspectHead = False
        getMetadataFromS3Object = True
        dataMaxBodyBytes = 0
        attributes = get_attributes_from_object(s3, self.bucket, self.key,
                inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, None)
        self.assertEqual(attributes, self.s3meta_expected, 'Attribute dictionaries do not match')

    def test_get_head_and_s3meta_attributes_from_object(self):
        s3 = boto3.client('s3')
        inspectHead = True
        getMetadataFromS3Object = True
        dataMaxBodyBytes = 0
        attributes = get_attributes_from_object(s3, self.bucket, self.key,
                inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, None)
        self.assertIsNotNone(attributes.get('LastModified', None), 'Missing the LastModified attribute')
        attributes['LastModified'] = 'DUMMY LASTMODIFIED'
        expected = {}
        expected.update(self.head_expected)
        expected.update(self.s3meta_expected)
        self.assertEqual(attributes, expected, 'Attribute dictionaries do not match')

    def test_get_just_body_attributes_from_object(self):
        s3 = boto3.client('s3')
        inspectHead = False
        getMetadataFromS3Object = False
        dataMaxBodyBytes = 40
        dataPlugin = 'defaultDataBodyParser'
        attributes = get_attributes_from_object(s3, self.bucket, self.key,
                inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, dataPlugin)
        self.assertEqual(attributes, self.body_expected, 'Attribute dictionaries do not match')

    def test_get_all_attributes_from_object(self):
        s3 = boto3.client('s3')
        inspectHead = True
        getMetadataFromS3Object = True
        dataMaxBodyBytes = 40
        dataPlugin = 'defaultDataBodyParser'
        attributes = get_attributes_from_object(s3, self.bucket, self.key,
                inspectHead, getMetadataFromS3Object, dataMaxBodyBytes, dataPlugin)
        print '\n\nAttributes:'
        print attributes
        self.assertIsNotNone(attributes.get('LastModified', None), 'Missing the LastModified attribute')
        attributes['LastModified'] = 'DUMMY LASTMODIFIED'
        expected = {}
        expected.update(self.body_expected)
        expected.update(self.head_expected)
        expected.update(self.s3meta_expected)
        self.assertEqual(attributes, expected, 'Attribute dictionaries do not match')

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
