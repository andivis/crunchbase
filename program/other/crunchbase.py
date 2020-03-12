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
    from google import Google

    from helpers import get
else:
    from ..library import helpers

    from ..library.database import Database
    from ..library.api import Api
    from ..library.other import Internet
    from ..library.website import Website
    from ..library.google import Google

    from ..library.helpers import get

class Crunchbase:
    def runRepeatedly(self, inputRows):
        while True:
            self.run(inputRows)
            self.waitForNextRun()

    def run(self, inputRows):
        self.gmDateStarted = datetime.utcnow()
        self.newSearchResultsCount = 0

        self.getReady()

        if self.isDone():
            return

        for i, inputRow in enumerate(inputRows):
            try:
                self.log.info(f'On item {i + 1} of {len(inputRows)}: {get(inputRow, "keyword")}')

                self.search(inputRow)

                if self.options['searchResultLimit'] and i >= self.options['searchResultLimit']:
                    break
            except Exception as e:
                helpers.handleException(e)

        self.markDone()

    def output(self, inputRow, newResult):
        if not get(newResult, 'id'):
            return

        self.log.info('Writing to csv file')

        outputFile = self.options['outputFile']

        ignoreColumns = [
            'permalink',
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

        self.storeToDatabase(inputRow, newResult)

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
        self.api.proxies = self.internet.getRandomProxy()

        original = self.api.urlPrefix;
        self.api.urlPrefix = '';
        ipInformation = self.api.get('https://ipinfo.io/json')
        self.log.debug(ipInformation)
        self.api.urlPrefix = original;
        
        self.api.setHeadersFromHarFile('program/resources/headers.json', '')

        result = {}
        
        document = self.getDocument(url)

        if document == None:
            return result
        
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
            'permalink': helpers.getNested(dictionary, ['properties', 'identifier', 'permalink']),
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
        searchSites = [
            'google.com',
            'crunchbase.com'
        ]

        # can only search by location using the built-in search
        if get(inputRow, 'search type') == 'location' or not self.options['useGoogle']:
            searchSites = [
                'crunchbase.com'
            ]

        afterId = ''

        for searchSite in searchSites:
            success = self.searchUsingSite(inputRow, searchSite, afterId)

            if success:
                break

    def searchUsingSite(self, inputRow, searchSite, afterId):
        result = False
        
        self.api.proxies = self.internet.getRandomProxy()
        
        keyword = get(inputRow, 'keyword')
        
        if not keyword:
            return result

        searchResults = []

        if searchSite == 'google.com':
            self.google.captcha = False
            searchResults = self.google.search(f'site:crunchbase.com "{keyword}"', 5)

            if self.google.captcha:
                return False
        elif searchSite == 'crunchbase.com':
            # use crunchbase search as a backup
            self.api.setHeadersFromHarFile('program/resources/headers-search.json', '')

            if get(inputRow, 'search type') == 'location':
                toSend = helpers.getJsonFile('program/resources/body-search.json')
                
                toSend['query'][0]['values'][0] = keyword

                if afterId:
                    toSend["after_id"] = afterId

                toSend = json.dumps(toSend)

                searchResults = self.api.post(f'/v4/data/searches/organizations?source=slug', toSend)
            else:
                searchResults = self.api.get(f'/v4/data/autocompletes?query={keyword}&collection_ids=organizations&limit=25&source=topSearch')
            
            self.waitBetweenRequests()

            searchResults = get(searchResults, 'entities')

            self.log.info(f'Found {len(searchResults)} results')

            if get(inputRow, 'search type') == 'company':
                if searchResults and len(searchResults) > 0:
                    searchResults = searchResults[0:1]

        for searchResult in searchResults:
            if get(inputRow, 'search type') == 'location':
                searchResult = get(searchResult, 'properties')

            if not self.passesFilters(searchResult, searchSite):
                if get(inputRow, 'search type') == 'location':
                    continue
                else:
                    break

            url = ''
            
            if searchSite == 'google.com':
                if not '/organization/' in searchResult:
                    continue 

                url = '/organization/' + self.getProfileId(searchResult)
            elif searchSite == 'crunchbase.com':
                url = helpers.getNested(searchResult, ['identifier', 'permalink'])
                url = '/organization/' + url

            profile = self.getProfile(url)

            if not profile:
                if get(inputRow, 'search type') == 'location':
                    continue
                else:
                    break

            self.output(inputRow, profile)

            self.newSearchResultsCount += 1            
            self.log.info(f'New results: {self.newSearchResultsCount}. Result: {get(profile, "name")}.')

            result = True

            if not get(inputRow, 'search type') == 'location':
                # only want first result
                break
        
        return result

    def getProfileUrl(self, url):
        return 'https://www.crunchbase.com/organization/' + self.getProfileId(url)

    def getProfileId(self, url):
        return helpers.findBetween(url, '/organization/', '/')

    def passesFilters(self, searchResult, searchSite):
        result = True

        key = 'id'
        id = ''

        if searchSite == 'google.com':
            key = 'permalink'
            id = self.getProfileId(searchResult)
        elif searchSite == 'crunchbase.com':
            id = helpers.getNested(searchResult, ['identifier', 'uuid'])

        if self.inDatabase(key, id):
            result = False
        elif f',{id},' in helpers.getFile(self.options['outputFile']):
            self.log.debug('Skipping. Already in the output file.')
            result = False

        return result

    def inDatabase(self, key, value):
        result = False

        row = self.database.getFirst('result', 'id', f"{key} = '{value}'")

        if row:
            self.log.debug(f'Skipping. Already in the database.')
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
            helpers.removeFile(rotatedFileName)
            os.rename(self.options['outputFile'], rotatedFileName)

    def waitBetweenRequests(self):
        if get(self.options, 'secondsBetweenRequests'):
            time.sleep(self.options['secondsBetweenRequests'])

    def getDocument(self, url):
        response = self.api.get(url, None, False, True)
        self.waitBetweenRequests()

        if response and 'verify you are a human' in response.text:
            self.log.error('There is a captcha')
            return None
        
        document = lh.fromstring(response.content)
        return document

    def isDone(self):
        result = False

        if not self.options['resumeSearch']:
            return result

        minimumDate = helpers.getDateStringSecondsAgo(self.options['hoursBetweenRuns'] * 3600, True)

        row = self.database.getFirst('history', '*', f"gmDateCompleted > '{minimumDate}'", 'gmDateCompleted', 'desc')

        if row:
            self.log.info(f'Waiting until next run. Finished less than {self.options["hoursBetweenRuns"]} hours ago.')
            result = True

        return result

    def markDone(self):
        history = {
            'gmDate': str(self.gmDateStarted),
            'gmDateCompleted': str(datetime.utcnow())
        }

        self.database.insert('history', history)

    def waitForNextRun(self):
        # added a few seconds for a margin of error
        nextDay = self.gmDateStarted + timedelta(hours=self.options['hoursBetweenRuns'], seconds=10)

        self.log.info('Done this run')
        
        helpers.waitUntil(nextDay)

        if '--debug' in sys.argv:
            time.sleep(3)

    def __init__(self, options, credentials):
        self.options = options
        self.log = logging.getLogger(get(self.options, 'loggerName'))

        self.database = Database('program/resources/tables.json')

        self.api = Api('https://www.crunchbase.com', self.options)
        self.api.timeout = 15
        self.internet = Internet(self.options)
        self.website = Website(self.options)
        self.google = Google(self.options)
        self.google.internet = self.internet

        if '--debug' in sys.argv:
            pass #debug self.database.execute(f'delete from result')