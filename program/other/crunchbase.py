import os
import sys
import logging
import json
import time

from datetime import datetime, timedelta

# pip packages
import lxml.html as lh

if '--debug' in sys.argv:
    import helpers as helpers

    from database import Database
    from api import Api
    from other import Internet
    from website import Website

    from helpers import get
else:
    from ..library import helpers

    from ..library.database import Database
    from ..library.api import Api
    from ..library.other import Internet
    from ..library.website import Website

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

        self.getReady()

        for i, inputRow in enumerate(inputRows):
            try:
                self.log.info(f'On item {i + 1} of {len(inputRows)}: {get(inputRow, "POSTCODE")}: {get(inputRow, "VIEW_NAME")}')

                profile = self.getProfile('/organization/monzo')
                self.output(inputRow, profile)

                self.search(inputRow)

                if self.options['searchResultLimit'] and i >= self.options['searchResultLimit']:
                    break
            except Exception as e:
                helpers.handleException(e)

    def output(self, inputRow, newResult):
        if not get(newResult, 'id'):
            return

        self.storeToDatabase(inputRow, newResult)

        self.log.info('Writing to csv file')

        outputFile = self.options['outputFile']

        ignoreColumns = [
            'json'
        ]

        fields = list(newResult.keys())

        for ignoreColumn in ignoreColumns:
            fields.remove(ignoreColumn)

        self.writeHeaders(fields, outputFile)

        values = []

        otherValues = {}

        for field in fields:
            value = ''

            if get(otherValues, field):
                value = get(otherValues, field)
            else:
                value = get(newResult, field)

            if not value:
                value = helpers.getNested(newResult, ['json', field])

            values.append(value)

        # this quotes fields that contain commas
        helpers.appendCsvFile(values, outputFile)

    def writeHeaders(self, fields, outputFile):
        if not os.path.exists(outputFile):
            printableNames = {
                'gmDate': 'date found'
            }

            printableFields = []
            
            for field in fields:
                printableName = None
                
                if field in printableNames:
                    printableName = get(printableNames, field)
                else:
                    printableName = helpers.getPrintableName(field)
                
                printableFields.append(printableName)
            
            helpers.toFile(','.join(printableFields), outputFile)

    def getProfile(self, url):
        result = {}
        
        document = self.getDocument(url)
        
        jsonElements = self.website.getXpath('', "//script[@type = 'application/ld+json' or @type = 'application/json']", False, None, document)

        for i, jsonElement in enumerate(jsonElements):
            text = jsonElement.text_content()

            isMain = False

            if '&q;' in text:
                text = text.replace('&q;', '"')

                isMain = True
            
            dictionary = json.loads(text)

            if isMain:
                result = self.getMainInformation(dictionary)

            #debug helpers.toFile(json.dumps(dictionary, indent=4), f'user-data/logs/j{i}.json')
        
        return result

    def getMainInformation(self, dictionary):
        dictionary = get(dictionary, 'HttpState')

        found = False

        for key in dictionary:
            if key.startswith('GET/'):
                dictionary = helpers.getNested(dictionary, [key, 'data'])
                found = True
                break

        if not found:
            return {}

        locations = helpers.getNested(dictionary, ['cards', 'overview_image_description', 'location_identifiers'])

        employees = helpers.getNested(dictionary, ['cards', 'current_employees_featured_order_field'])

        employeeStrings = []
        
        for employee in employees:
            name = helpers.getNested(employee, ['person_identifier', 'value'])
            title = get(employee, 'title')

            string = f'{name} ({title})'

            employeeStrings.append(string)

        employees = '; '.join(employeeStrings)

        numberOfEmployees = helpers.getNested(dictionary, ['cards', 'overview_fields', 'num_employees_enum'])
        numberOfEmployees = helpers.findBetween(numberOfEmployees, 'c_', '')
        numberOfEmployees = numberOfEmployees.replace('_', ' to ')

        fundingRoundsStrings = []

        fundingRounds = helpers.getNested(dictionary, ['cards', 'funding_rounds_list'])

        for fundingRound in fundingRounds:
            date = get(fundingRound, 'announced_on')
            
            name = helpers.getNested(fundingRound, ['identifier', 'value'])
            
            money = helpers.getNested(fundingRound, ['money_raised', 'value'])

            if money:
                money = helpers.compactNumber(money)

            currency = helpers.getNested(fundingRound, ['money_raised', 'currency'])

            string = f'{date} {name}: {money} {currency}'

            fundingRoundsStrings.append(string)

        fundingRounds = '; '.join(fundingRoundsStrings)

        result = {
            'gmDate': str(datetime.utcnow()),
            'id': helpers.getNested(dictionary, ['properties', 'identifier', 'uuid']),
            'name': helpers.getNested(dictionary, ['properties', 'title']),
            'legalName': helpers.getNested(dictionary, ['cards', 'overview_fields', 'legal_name']),
            'city': self.findByValue(locations, 'location_type', 'city', 'value'),
            'region': self.findByValue(locations, 'location_type', 'region', 'value'),
            'country': self.findByValue(locations, 'location_type', 'country', 'value'),
            'description': helpers.getNested(dictionary, ['properties', 'short_description']),
            'website': helpers.getNested(dictionary, ['cards', 'overview_fields2', 'website', 'value']),
            'email': helpers.getNested(dictionary, ['cards', 'overview_fields2', 'contact_email']),
            'linkedin': helpers.getNested(dictionary, ['cards', 'overview_fields2', 'linkedin', 'value']),
            'phone': helpers.getNested(dictionary, ['cards', 'overview_fields2', 'phone_number']),
            'founded': helpers.getNested(dictionary, ['cards', 'overview_fields', 'founded_on', 'value']),
            'operatingStatus': helpers.getNested(dictionary, ['cards', 'overview_fields', 'operating_status']),
            'fundingStatus': helpers.getNested(dictionary, ['cards', 'overview_fields', 'funding_stage']),
            'fundingType': helpers.getNested(dictionary, ['cards', 'overview_fields', 'last_funding_type']),
            'crunchbaseUrl': 'https://www.crunchbase.com/organization/' + helpers.getNested(dictionary, ['properties', 'identifier', 'permalink']),
            'employees': employees,
            'numberOfEmployees': numberOfEmployees,
            'fundingTotal': helpers.getNested(dictionary, ['cards', 'funding_rounds_headline', 'funding_total', 'value']),
            'currency': helpers.getNested(dictionary, ['cards', 'funding_rounds_headline', 'funding_total', 'currency']),
            'fundingRounds': fundingRounds,
            'json': dictionary
        }

        return result

    def findByValue(self, array, keyToFind, valueMustMatch, keyToReturn):
        for dictionary in array:
            for key, value in dictionary.items():
                if key == keyToFind and value == valueMustMatch:
                    return get(dictionary, keyToReturn)

        return ''

    def search(self, inputRow):
        self.api.proxies = self.internet.getRandomProxy()
        
        if not inputRow:
            return

        # subtract a little to allow a margin of error
        if self.requestCount >= self.maximumRequestsPerHour - 5:
            self.log.info(f'Reached maximum of {self.maximumRequestsPerHour}. Waiting 1 hour.')
            helpers.wait(3600)
            self.requestCount = 0

        searchResults = self.api.get('', None, False)

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

        if f',{get(newResult, "id")},' in helpers.getFile(outputFile):
            self.log.debug('Skipping. Already in the output file.')
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

    def storeToDatabase(self, inputRow, newResult):
        if get(newResult, 'json'):
            newResult['json'] = json.dumps(newResult['json'], indent=4)

        self.database.insert('result', newResult)

    def getReady(self):
        helpers.makeDirectory(os.path.dirname(self.options['outputFile']))

        # should restart or resume search from where left off?
        if not self.options['resumeSearch'] and os.path.exists(self.options['outputFile']):
            # move old output file
            rotatedFileName = self.options['outputFile'] + '.old'
            os.rename(self.options['outputFile'], rotatedFileName)

    def getDocument(self, url):
        response = self.api.get(url, None, False, True)
        document = lh.fromstring(response.content)
        return document
    
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

        self.api = Api('https://www.crunchbase.com', self.options)
        self.api.setHeadersFromHarFile('program/resources/headers.txt', '')
        self.internet = Internet(self.options)
        self.website = Website(self.options)

        self.maximumRequestsPerHour = 3600