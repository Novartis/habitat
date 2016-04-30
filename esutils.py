'''
File: esutils.py

Utility functions to interact with the AWS Elastic Search service

https://elasticsearch-py.readthedocs.org/en/master/

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
import time
import json
import configutils
import secret

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# TODO KLR: Review all es calls and surround with try/except

Configs = configutils.load_configs()
HabitatIndex = Configs['esHabitatIndex']
DocType = Configs['esDocType']

awsauth = AWS4Auth(secret.AWS_ACCESS_KEY_ID, secret.AWS_SECRET_ACCESS_KEY, secret.AWS_DEFAULT_REGION, 'es')

def putEndpointInConfigFile():
    '''
    Utility function to get the endpoint from an existing domain and write it to the config file

    Usage: 
        python -m esutils putEndpointInConfigFile
    '''
    # Get the es endpoint given the domain name
    esclient = boto3.client('es')
    response = esclient.describe_elasticsearch_domain(DomainName=Configs['esDomain'])

    endpoint = response['DomainStatus'].get('Endpoint', None)
    import sys
    if endpoint is not None:
        Configs['esEndpoint'] = response['DomainStatus']['Endpoint']
        configutils.store_configs(Configs)
        sys.exit(0)
    else:
        print 'Domain is not yet created. Try again (it may take 10-15 minutes).'
        sys.exit(1)

def makeUniqueId(attributes):
    ''' Create the unique index key based on the attributes '''
    # TODO KLR: I need to think more about what this should really be. This is a place holder.
    # Old: objectId = '{}:{}:{}'.format( attributes['region'], attributes['bucket'], attributes['key'])
    objectId = '{}/{}'.format(attributes['bucket'], attributes['key'])
    return objectId

def esInit(esEndpoint):
    ''' Initialize the Elasticsearch object '''
    es = Elasticsearch(
            hosts=[{'host': esEndpoint, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
            )
    return es

def indexAttributes(attributes, esEndpoint):
    '''
    Store the provided attributes into the elasticsearch index

    At present, does not require index to be unique and will overrite and create a new version.

    Return: objectId on success, None otherwise
    '''
    objectId = makeUniqueId(attributes)

    es = esInit(esEndpoint)

    logging.debug('esinfo:')
    logging.debug(es.info())

    try:
        res = es.index(index=HabitatIndex, doc_type=DocType, id=objectId, body=attributes)
        if res is None or res.get('created', None) is None:
            logging.debug('Problem creating index for objectId {}'.format(objectId))
            logging.debug(res)
            return None
        else:
            return objectId
    except Exception as e:
        # TODO KLR: This is not quite right. How should I do this?
        logging.error('HTTP Status: %d', e.info['status'])
        logging.error('Error detail: ' +  e.info['error'])
        logging.error('Error creating index for objectId {}'.format(objectId))
        return None

def getById(objectId, esEndpoint):
    ''' Get the item with id objectId '''
    es = esInit(esEndpoint)
    res = es.get(index=HabitatIndex, doc_type=DocType, id=objectId)
    return res

def queryAll(esIndex, esEndpoint):
    ''' A debug function to see what's in the index '''
    es = esInit(esEndpoint)

    res = es.search(index=esIndex, body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        print json.dumps(hit, indent=4)
        #print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

def getcli():
    '''
    Quick and dirty utility to all quick command line query to ES

    Usage:
        python -m esutils getcli <an_id>
    '''
    import sys
    Configs = configutils.load_configs()
    objectId=sys.argv[2]

    result = getById(objectId, Configs['esEndpoint'])
    print json.dumps(result, indent=4)
    exit(0)


#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        self.esEndpoint = Configs['esEndpoint']

    def test_putEndpointInConfigFile(self):
        putEndpointInConfigFile()
        print 'Manually inspect sanity of endpoint value...'
        print 'Endpoint = ', Configs['esEndpoint']

    def test_queryAll(self):
        queryAll(HabitatIndex, self.esEndpoint)

    def test_indexAttributes(self):
        ''' Simple test of unique insert of a new index '''
        from time import gmtime, strftime
        key = strftime("%Y-%m-%dT%H:%M:%S", gmtime())
        attributes = {
                'region': 'us-east-1',
                'bucket': 'mybucket',
                'key': key
                }
        objectId = indexAttributes(attributes, self.esEndpoint)
        print 'ObjectId:', objectId
        self.assertIsNotNone(objectId, 'Could not insert into index')

        result = getById(objectId, self.esEndpoint)
        expected = {
                u'_type': DocType,
                u'_source': attributes,
                u'_index': HabitatIndex,
                u'_version': 1,
                u'found': True,
                u'_id': objectId
                }
        self.assertEqual(result, expected, 'Verification fetch did not match expected result')

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
