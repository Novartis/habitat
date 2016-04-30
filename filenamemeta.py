'''
File: filenamemeta.py

Extract the meta data from a file name

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
import re

def get_attributes_from_filename(fname, regex):
    '''
    Extract metadata attributes from a filename string.
    The regex must be a valid Python 2.7 regex.
    Matched terms must be in a named group using the syntax:
        (?P<keyName> ... )
        where keyName is the name of the metadata key and the '...' represents
        the regex portion that matches the particular value for the key
    For example, a file name with an assay ID and a run ID embedded and delimited by dashes might be:
        a1234-15-imager_1234567890.tif
    The regex could be
        ^(?P<assayId>\w+)-(?P<runId>\d+)-\w+.tif$

    Non-named groups may be used in the regex, but they will not be included in the returned dictionary.

    Keys may be required or not depending on how the regex is written. If written flexibly, the value for
    missing keys will be None. If written rigidly, then the function will return None if all values are
    not matched.

    If Regex is None or '', then it is assumed that no file name processing is required and None is returnd

    Returns:
        Dictionary of match key/values where the keys are the names from the named groups in the regex
        None if no matches
    '''
    p = re.compile(regex)
    m = p.search(fname)
    if m is None:
        return None
    else:
        return m.groupdict()


#############
# unittests #
#############
class TestController(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_attributes_from_filename(self):
        ''' Happy path test case '''
        fname = 'a1234-15-imager_1234567890.tif'
        regex = '^(?P<assayId>\w+)-(?P<runId>\d+)-\w+.tif$'
        attributes = get_attributes_from_filename(fname, regex)
        expected = {
                'assayId': 'a1234',
                'runId': '15'
                }
        self.assertEqual(attributes, expected, 'Attribute dictionaries do not match')

    def test_get_attributes_from_filename_nomatch(self):
        ''' No matching keys are found '''
        fname = 'readme.txt'
        regex = '^(?P<assayId>\w+)-(?P<runId>\d+)-\w+.tif$'
        attributes = get_attributes_from_filename(fname, regex)
        self.assertIsNone(attributes, 'Expected None since there should be no matches')

    def test_get_attributes_from_filename_partialmatch_rigid(self):
        ''' At least one matching key is found, but not all, and regex expects all '''
        fname = 'a1234-imager_1234567890.tif'
        regex = '^(?P<assayId>\w+)-(?P<runId>\d+)-\w+.tif$'
        attributes = get_attributes_from_filename(fname, regex)
        self.assertIsNone(attributes, 'Expected None since the regex was rigid and expected all matches')

    def test_get_attributes_from_filename_partialmatch_flexible(self):
        ''' At least one matching key is found, but not all, and regex is okay with just some '''
        fname = 'a1234-imager_1234567890.tif'
        regex = '^(?P<assayId>\w+)-((?P<runId>\d+)-)?\w+.tif$'
        attributes = get_attributes_from_filename(fname, regex)
        expected = {
                'assayId': 'a1234',
                'runId': None
                }
        self.assertEqual(attributes, expected, 'Expected some key/values since the regex was flexible')

def AllModuleTests():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestController)
    return unittest.TestSuite([suite1])

########
# MAIN #
########
if __name__ == '__main__':
    unittest.main()
