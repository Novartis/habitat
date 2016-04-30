'''
File: metafile.py

Extract the meta data from the companion metadata file s3 object.
Format may be json, csv with a header row, or custom via a custom plugin parser.

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
import json

def get_attributes_from_metadatafile(s3, bucket, key, metafileFormat, metafileParserModule):
    '''
    Extract attributes from a companion metadata file.

    Returns:
        Dictionary of matched key/values extracted according to the various options
        Dictionary may be empty if no matches are found
        None on error
    '''
    data = get_body_data_from_object(s3, bucket, key)
    if data is None:
        return None

    if metafileFormat == 'json':
        attributes = get_attributes_as_json(data)
    elif metafileFormat == 'csv':
        attributes = get_attributes_as_csv(data)
    elif metafileFormat == 'custom':
        attributes = get_attributes_using_custom(data, metafileParserModule)

    return attributes

def get_attributes_as_json(data):
    ''' Interpret data as a json document (as a string) '''
    return json.loads(data)

def get_attributes_as_csv(data):
    '''
    Interpret data as a csv file with the first line being the comma separated list of keys
    and the second line being the comma separated list of corresponding values.

    The current implementation assumes a very well behaved input format.
    Fields may optionally be enclosed in double quotes. They will be stripped off.
    Extra white space and CR and LFs are also removed.

    For example:
    key1,key2,key3
    value1,value2,value3
    '''
    (keyRow, valueRow) = data.splitlines()
    keyList = keyRow.split(',')
    keyList = [key.strip('"\n\r\t ') for key in keyList]
    valueList = valueRow.split(',')
    valueList = [value.strip('"\n\r\t ') for value in valueList]
    return dict(zip(keyList, valueList))

def get_attributes_using_custom(data, plugin):
    '''
    Use a custom module to parse the meta data file body.

    Runs a custom function on the data to return a dictionary of key/values.
    plugin is the name of the Python module to execute on the body data.
    The module must contain a function called parsebody:
         attributes = parsebody(body)
    where body is a string containing the data to be parsed and attributes is a Dict()
    of key/value pairs. The values can be arbitrary.
    '''
    logging.info('About to run custom metadata parser: ' + plugin)
    try:
        parsermodule = __import__(plugin)
        attributes = parsermodule.parsebody(data)
        return attributes
    except:
        logging.error('Problem running custom parser on metadata file')
        raise
        return {}


def get_body_data_from_object(s3, bucket, key):
    '''
    Read the body data from the object.
    
    s3 is an existing boto3 client object created from: s3 = boto3.client('s3')

    Returns: buffer of all body data from object
    '''
    try:
        logging.info('Getting metadata object for bucket {} and key {}...'.format(bucket, key))

        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()

        return body
    except Exception as e:
        logging.error(e)
        logging.error('Error getting object {} from bucket {}.'.format(key, bucket))
        return None


#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        import configutils
        self.configs = configutils.load_configs()

        self.bucket = self.configs['bucket']
        self.keyBase = 'meta/unittest-a1234-15-imager_1234567890.'
        self.json_expected = {
                'json1': 'jsonValue1',
                'json2': 'jsonValue2'
                }
        self.csv_expected = {
                'csv1': 'csvValue1',
                'csv2': 'csvValue2',
                'csv3': 'csvValue3'
                }
        self.custom_expected = {
                'custom1': 'customValue1',
                'custom2': 'customValue2'
                }
        self.plugin = 'defaultMetafileParser'

    def test_get_body_data_from_object(self):
        s3 = boto3.client('s3')
        data = get_body_data_from_object(s3, self.bucket, self.keyBase + 'json')
        expected = '''{
    "json1": "jsonValue1",
    "json2": "jsonValue2"
}
'''
        self.assertEqual(data, expected, 'Body data does not match expectation')

    def test_get_attributes_as_json(self):
        data = json.dumps(self.json_expected)
        attributes = get_attributes_as_json(data)
        self.assertEqual(attributes, self.json_expected, 'Attribute dictionaries do not match')

    def test_get_attributes_as_csv(self):
        data = '''csv1,"csv2", csv3
csvValue1,"csvValue2", csvValue3'''
        attributes = get_attributes_as_csv(data)
        self.assertEqual(attributes, self.csv_expected, 'Attribute dictionaries do not match')

    def test_get_attributes_using_custom(self):
        data = '''custom1=customValue1
custom2=customValue2'''
        attributes = get_attributes_using_custom(data, self.plugin)
        self.assertEqual(attributes, self.custom_expected, 'Attribute dictionaries do not match')

    def test_get_attributes_from_metadatafile_json(self):
        ''' Test the json file format case '''
        s3 = boto3.client('s3')
        attributes = get_attributes_from_metadatafile(s3, self.bucket, self.keyBase + 'json', 'json', None)
        self.assertEqual(attributes, self.json_expected, 'Attribute dictionaries do not match')
        
    def test_get_attributes_from_metadatafile_csv(self):
        ''' Test the csv file format case '''
        s3 = boto3.client('s3')
        attributes = get_attributes_from_metadatafile(s3, self.bucket, self.keyBase + 'csv', 'csv', None)
        self.assertEqual(attributes, self.csv_expected, 'Attribute dictionaries do not match')
        
    def test_get_attributes_from_metadatafile_custom(self):
        ''' Test the custom file format case '''
        s3 = boto3.client('s3')
        attributes = get_attributes_from_metadatafile(s3, self.bucket, self.keyBase + 'custom', 'custom', self.plugin)
        self.assertEqual(attributes, self.custom_expected, 'Attribute dictionaries do not match')

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
