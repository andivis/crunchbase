import sys
import os
import logging
import datetime
import traceback
import random
import string

if '--debug' in sys.argv:
    import helpers as helpers

    from helpers import get
else:
    import program.library.helpers as helpers

    from program.library.helpers import get

from program.other.crunchbase import Crunchbase

class Main:
    def run(self):
        self.log.info('Starting')

        inputRows = helpers.getCsvFile(self.options['inputFile'])

        try:
            crunchbase = Crunchbase(self.options, self.credentials)

            crunchbase.runRepeatedly(inputRows)
        except Exception as e:
            helpers.handleException(e)
        
        self.cleanUp()

    def cleanUp(self):
        self.log.info('Done')
        self.log.info('Exiting')
        helpers.wait(30)

    def __init__(self):
        self.loggerHandlers = helpers.setUpLogging('user-data/logs')
        self.log = self.loggerHandlers['logger']

        # set default options
        self.options = {
            'inputFile': 'user-data/input/input.csv',
            'outputFile': 'user-data/output/output.csv',
            'proxyListUrl': helpers.getFile('program/resources/resource'),
            'proxyProvider': 'smartproxy',
            'hoursBetweenRuns': 7 * 24,
            'runRepeatedly': 1,
            'defaultSearchUrl': '',
            'secondsBetweenSearches': 0,
            'secondsBetweenProfiles': 0,
            'loggerName': self.log.name,
            'searchResultLimit': -1,
            'customFilterStep': 0,
            'randomizeUserAgent': 1,
            'resumeSearch': 1,
            'refreshOnly': 0,
            'dateForNewCompaniesSearch': '01/01/2020',
            'useGoogle': 1
        }

        optionsFileName = helpers.getParameter('--optionsFile', False, 'user-data/options.ini')

        if '--refresh' in sys.argv:        
            self.options['refreshOnly'] = 1
            
        # read the options file
        helpers.setOptions(optionsFileName, self.options)

        self.credentials = {}

        helpers.setOptions('user-data/credentials/credentials.ini', self.credentials, '')

if __name__ == '__main__':
    main = Main()
    main.run()