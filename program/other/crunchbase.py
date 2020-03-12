import os
import sys
import logging
import json
import time

from datetime import datetime, timedelta

if '--debug' in sys.argv:
    import helpers as helpers

    from database import Database
    from api import Api
    from other import Internet

    from helpers import get
else:
    from ..library import helpers

    from ..library.database import Database
    from ..library.api import Api
    from ..library.other import Internet

    from ..library.helpers import get

class Crunchbase:
    def runRepeatedly(self, inputRows):
        self.gmDateStarted = datetime.utcnow()

        if '--debug' in sys.argv:
            self.database.execute(f'delete from result')

        while True:
            self.run(inputRows)
            self.waitForNextRun()

    def run(self, inputRows):
        self.requestCount = 0
        self.newSearchResultsCount = 0

        for i, inputRow in enumerate(inputRows):
            try:
                self.log.info(f'On item {i + 1} of {len(inputRows)}: {get(inputRow, "POSTCODE")}: {get(inputRow, "VIEW_NAME")}')
                self.search(inputRow)

                if self.options['searchResultLimit'] and i >= self.options['searchResultLimit']:
                    break
            except Exception as e:
                helpers.handleException(e)

            self.outputNewResults(inputRow)

    def outputNewResults(self, inputRow):
        self.log.info('Writing to csv file')

        outputFile = self.options['outputDirectory'] + '/' + self.gmDateStarted.strftime('%Y-%m-%d') + '.csv'

        # to avoid duplicates
        helpers.removeFile(outputFile)

        rows = self.database.get('result', '*', f"gmDate >= '{self.gmDateStarted}'",  'price', 'asc')

        for row in rows:
            self.outputResult(inputRow, row, outputFile)

    def outputResult(self, inputRow, newItem, outputFile):
        if not get(newItem, 'id'):
            return

        helpers.makeDirectory(os.path.dirname(outputFile))

        fields = ['gmDate', 'name', 'city', 'region', 'country', 'id', 'url']

        printableNames = {
            'gmDate': 'date found'
        }

        if not os.path.exists(outputFile):
            printableFields = []
            
            for field in fields:
                printableName = None
                
                if field in printableNames:
                    printableName = get(printableNames, field)
                else:
                    printableName = helpers.getPrintableName(field)
                
                printableFields.append(printableName)
            
            helpers.toFile(','.join(printableFields), outputFile)


        if f',{get(newItem, "id")},' in helpers.getFile(outputFile):
            self.log.debug('Skipping. Already in the output file')
            return

        values = []

        jsonColumn = json.loads(get(newItem, 'json'))

        otherValues = {}

        for field in fields:
            value = ''

            if get(otherValues, field):
                value = get(otherValues, field)
            else:
                value = get(newItem, field)

            if not value:
                value = get(jsonColumn, field)

            values.append(value)

        # this quote fields that contain commas
        helpers.appendCsvFile(values, outputFile)

    def search(self, inputRow):
        self.api.proxies = self.internet.getRandomProxy()
        
        if not inputRow:
            return

        # subtract a little to allow a margin of error
        if self.requestCount >= self.maximumRequestsPerHour - 5:
            self.log.info(f'Reached maximum of {self.maximumRequestsPerHour}. Waiting 1 hour.')
            helpers.wait(3600)
            self.requestCount = 0

        searchParameters = {
            'keyword': get(inputRow, 'keyword'),
        }

        try:
            searchResults = self.api.get('', searchParameters)
        except Exception as e:
            helpers.handleException(e)

            self.log.info(f'Reached maximum requests per hour. Waiting 1 hour.')
            helpers.wait(3600,)

        self.requestCount += 1

        self.log.info(f'Found {len(searchResults)} results')

        for searchResult in searchResults:
            if not self.passesFilters(searchResult):
                continue

            self.newSearchResultsCount += 1
            
            self.log.info(f'New results: {self.newSearchResultsCount}. Result: {get(searchResult, "name")}.')

            self.store(inputRow, searchResult)

    def passesFilters(self, searchResult):
        result = True
        
        if get(searchResult, 'property_type') and get(searchResult, 'property_type') in self.options['ignorePropertyTypes']:
            self.log.info(f'Skipping. Property type is {get(searchResult, "property_type")}.')
            result = False

        if self.inDatabase(searchResult):
            result = False

        return result

    def inDatabase(self, searchResult):
        result = False

        id = get(searchResult, 'listing_id')
        row = self.database.getFirst('result', 'id', f"id = '{id}'")

        if row:
            self.log.debug(f'Skipping {id}. Already in the database.')
            result = True
        
        return result

    def store(self, inputRow, searchResult):
        jsonObject = searchResult

        jsonColumn = json.dumps(jsonObject, indent=4, sort_keys=False, default=str)
        
        newRow = {
            'id': get(searchResult, 'listing_id'),
            'searchTerm': get(inputRow, 'keyword'),
            'name': get(searchResult, 'name'),
            'city': get(searchResult, 'city'),
            'region': get(searchResult, 'region'),
            'country': get(searchResult, 'country'),
            'gmDate': str(datetime.utcnow()),
            'json': jsonColumn
        }

        self.database.insert('result', newRow)
    
    def markDone(self, inputRow, newSearchResults):
        if not newSearchResults:
            return

        history = {
            'gmDate': str(datetime.utcnow())
        }

        self.database.insert('history', history)

    def waitForNextRun(self):
        nextDay = self.gmDateStarted + timedelta(hours=self.options['hoursBetweenRuns'])

        self.log.info('Done this run')
        
        helpers.waitUntil(nextDay)

        if '--debug' in sys.argv:
            time.sleep(3)

    def __init__(self, options, credentials):
        self.options = options
        self.log = logging.getLogger(get(self.options, 'loggerName'))

        self.database = Database('program/resources/tables.json')

        self.api = Api('', self.options)
        self.internet = Internet(self.options)

        self.maximumRequestsPerHour = 3600