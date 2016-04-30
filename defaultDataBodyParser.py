'''
File: defaultDataBodyParser.py

Copy this file as a starting point for a custom body parser
and replace with customer version of parsebody (maintain signature).

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

def parsebody(body):
    '''
    Place holder example for a custom body parser plugin to extract meta data attributes
    from the body at the start of a file.

    body is a string

    Returns:
        Dictionary of matched key/values extracted from the body
        Dictionary may be empty if no matches are found
        None on error
    '''
    attributes = {}

    # TODO: Replace this code with your code to parse body
    lines = body.split('\n')
    for line in lines:
        parts = line.split('=')
        if len(parts) == 2:
            attributes[parts[0]] = parts[1]
    # TODO: End of custom code

    return attributes


#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        pass

    def test_parsebody(self):
        ''' Happy path test case '''
        body = '''
key1=value1
key2=value2
        '''
        attributes = parsebody(body)
        expected = {
                'key1': 'value1',
                'key2': 'value2'
                }
        self.assertEqual(attributes, expected, 'Attribute dictionaries do not match')

def AllModuleTests():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestController)
    return unittest.TestSuite([suite1])

########
# MAIN #
########
if __name__ == '__main__':
    unittest.main()
