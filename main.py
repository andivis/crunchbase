import sys
import os
import logging
import datetime
import traceback
import random
import string

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

    def __init__(self):
        self.loggerHandlers = helpers.setUpLogging('user-data/logs')
        self.log = self.loggerHandlers['logger']

        # set default options
        self.options = {
            'inputFile': 'user-data/input/input.csv',
            'outputDirectory': 'user-data/output',
            'proxyListUrl': 'program/resources/resource',
            'hoursBetweenRuns': 7 * 24,
            'loggerName': self.log.name,
            'searchResultLimit': 100 * 1000,
        }

        optionsFileName = helpers.getParameter('--optionsFile', False, 'user-data/options.ini')
        
        # read the options file
        helpers.setOptions(optionsFileName, self.options)

        self.credentials = {}

        helpers.setOptions('user-data/credentials/credentials.ini', self.credentials, '')

if __name__ == '__main__':
    main = Main()
    main.run()