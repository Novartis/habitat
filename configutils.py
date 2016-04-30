'''
File: configutils.py

Utility functions to read and write the config file. Also, provides an interface to make it easy to call from a shell script.
Config file name is hard coded in this module.

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

import sys
import json

configFile = 'habitatconfig.json'
'''
    TODO KLR:
    For now, this is a file in the local directory.
    In the future, this could be a DynamoDB call or an open of an S3 file.
    When DAX (cache front-end to DDB) is released, then perhaps that would be a good place to get the configs.
    Actually, since each repo should probably have its own lambda function, then perhaps the content of the config
    file should be a Python file and directly part of the function. If config changes are required, just update the function.
    There are pros/cons, so think this through more later when we are worrying more about performance.
'''

def load_configs():
    '''
    Read the json config file.

    Note that the regex (as in the value for the dataFilenameRegex key) must have each backslashes escaped with an additional backslash
    so that the config file remains valid json.
    '''
    with open(configFile, 'r') as f:
        configs = json.loads(f.read())
    return configs

def get_config():
    ''' 
    Called from the command line, print to stdout the value for the specified key.
    If not key is specified, pretty print the full json of the config.
    '''
    if len(sys.argv) == 1:
        configs = load_configs()
        print 'Config file: ', configFile
        print json.dumps(configs, indent=4)
    elif len(sys.argv) == 2:
        configs = load_configs()
        print configs[sys.argv[1]]
    else:
        print 'Usage:'
        print '  {} prints this usage message'.format(sys.argv[0])
        print '  {} pretty prints config file to stdout'.format(sys.argv[0])
        print '  {} <topLevelKey> - prints the value for key to stdout'.format(sys.argv[0])

def store_configs(configs):
    '''
    Write the json config file.

    Note that the regex (as in the value for the dataFilenameRegex key) must have each backslashes escaped with an additional backslash
    so that the config file remains valid json.
    '''
    with open(configFile, 'w') as f:
        f.write(json.dumps(configs, indent=4))

########
# MAIN #
########
if __name__ == '__main__':
    # Normal function to execute from command line
    get_config()

    # No unit tests. Just inspect manually using one or more of the following...
    '''
    # Test load_configs
    print json.dumps(load_configs(), indent=4)
    '''

    '''
    # Test store_configs
    configs = load_configs()
    configs['esEndpoint'] = 'ES-ENDPOINT-SET'
    store_configs(configs)
    '''

