'''
File: test_all.py

Local (i.e., this need not be deployed) script to run all unit tests

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
import sys

'''
This is the master test sript to run all test suites for the application.

To run:
    First make sure that credential environment variables are set as appropriate.
    python test_all.py > test.out
    This will send unittest output to console and stdout (print statements that are part of tests) to test.out

Each module to be included should include the following pattern:
def AllModuleTests():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(nameOfTestCaseObject1GoesHere)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(nameOfTestCaseObject2GoesHere) # Repeat as necessary
    return unittest.TestSuite([suite1, suite2]) # If only suite1 is defined, can just return suite1 directly

OTHER UNIT TESTING NOTES:
To test a single module using pyunit, just execute the module since by convention, the unittest is defined for main. E.g.,:
	python filenamemeta.py

To test a single test within a test case, follow this pattern:
        python -m unittest filenamemeta.TestController.test_get_attributes_from_filename
'''

# Build the master test suite
# N.B. that if new modules are created, they will need to be manually added in two places below (import, suites.append)
import habitat_handler
import esutils
import metadata
import filenamemeta
import objectmeta
import metafile
import defaultMetafileParser
import defaultDataBodyParser

fastSuites = []
slowSuites = []
fastSuites.append(habitat_handler.AllModuleTests())
fastSuites.append(esutils.AllModuleTests())
fastSuites.append(metadata.AllModuleTests())
fastSuites.append(filenamemeta.AllModuleTests())
fastSuites.append(objectmeta.AllModuleTests())
fastSuites.append(metafile.AllModuleTests())
fastSuites.append(defaultMetafileParser.AllModuleTests())
fastSuites.append(defaultDataBodyParser.AllModuleTests())

# For now everything is in the fast suite. Move later if necessary.
# slowSuites.append(objectmeta.AllModuleTests())

fastTests = unittest.TestSuite(fastSuites)
slowTests = unittest.TestSuite(slowSuites)
allTests = unittest.TestSuite(fastSuites + slowSuites)

if __name__ == '__main__':
    logFormat = '%(levelname)s:%(asctime)s:HABITAT:%(module)s-%(lineno)d: %(message)s'
    logLevel = logging.INFO
    logging.basicConfig(format=logFormat, level=logLevel)

    if len(sys.argv) == 1:
        tests = allTests
        print 'Running allTests'
    else:
        if sys.argv[1] == 'fast':
            tests = fastTests
            print 'Running fastTests'
        elif sys.argv[1] == 'slow':
            tests = slowTests
            print 'Running slowTests'
        else:
            tests = allTests
            print 'Running allTests'

    # verbosity=1 is mostly quiet, verbosity=2 is verbose
    unittest.TextTestRunner(stream=sys.stderr, descriptions=True, verbosity=2).run(tests)
