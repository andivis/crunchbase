import os
import sys
import logging
import json
import time
import random

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
        self.inputRows = inputRows

        while True:
            self.run(inputRows)
            
            shouldRepeat = self.waitForNextRun()

            if not shouldRepeat:
                break

    def run(self, inputRows):
        self.gmDateStarted = datetime.utcnow()

        self.getReady()

        if self.options['refreshOnly']:
            # doesn't matter when last run was
            self.refreshOnly()

            self.log.info(f'Adding companies founded since {self.options["dateForNewCompaniesSearch"]}')

        if self.isDone():
            return

        for self.inputRowIndex, self.inputRow in enumerate(inputRows):
            try:
                self.search(self.inputRow)
            except Exception as e:
                helpers.handleException(e)

        self.markDone()

    def output(self, inputRow, newResult):
        if not get(newResult, 'id'):
            return

        id = get(newResult, 'id')

        self.log.debug('Writing to csv file')

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

        inFile = False
        
        if f',{id},' in helpers.getFile(self.options['outputFile']):
            inFile = True
        
        # remove it so can update it
        if inFile and self.options['refreshOnly']:
            self.removeFromCsvFile(id, outputFile)

        if not inFile or self.options['refreshOnly']:
            # this quotes fields that contain commas
            helpers.appendCsvFile(values, outputFile)
        else:
            self.log.debug(f'Not writing {get(newResult, "permalink")} to output file. Already in the output file.')

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
            
            helpers.toFile(','.join(printableFields) + '\n', outputFile)

    def getProfile(self, url):
        self.api.proxies = self.internet.getRandomProxy()

        self.checkProxy()
        
        self.api.setHeadersFromHarFile('program/resources/headers.json', '')

        result = {}

        self.log.info(f'Getting profile for {self.getProfileId(url)}')
        
        document = self.getDocument(url)

        if document == None:
            return result
        
        jsonElements = self.website.getXpath('', "//script[@type = 'application/ld+json' or @type = 'application/json']", False, None, document)

        for jsonElement in jsonElements:
            text = jsonElement.text_content()

            isMain = False

            if '&q;' in text:
                text = text.replace('&q;', '"')

                isMain = True
            
            dictionary = json.loads(text)

            if isMain:
                result = self.getMainInformation(dictionary)
        
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

        if '--debug' in sys.argv:
            helpers.toFile(json.dumps(dictionary), 'user-data/logs/j.json')

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
            'keyword': get(self.inputRow, 'keyword'),
            'json': dictionary
        }

        self.log.info(f'Profile: {get(result, "name")}, {get(result, "legalName")}, {get(result, "city")}, {get(result, "description")[0:30]}...')

        return result

    def findByValue(self, array, keyToFind, valueMustMatch, keyToReturn):
        for dictionary in array:
            for key, value in dictionary.items():
                if key == keyToFind and value == valueMustMatch:
                    return get(dictionary, keyToReturn)

        return ''

    def search(self, inputRow):
        self.searchResultsCount = 0
    
        if not get(inputRow, 'keyword'):
            return

        searchSites = [
            'google.com',
            'crunchbase.com'
        ]

        # can only search by location using the built-in search
        if get(inputRow, 'search type') == 'location' or not self.options['useGoogle']:
            searchSites = [
                'crunchbase.com'
            ]

        self.totalSearchResults = 0

        self.filterIndex = 0
        self.maximumFilterValue = 100 * 1000
        self.filterStep = 10 * 1000

        if '--debug' in sys.argv:
            self.filterStep = 100
            self.maximumFilterValue = self.filterStep * 3

        if self.options['refreshOnly']:
            # only want to search locations
            if not get(inputRow, 'search type') == 'location':
                return
            # only want one step
            self.filterStep = self.maximumFilterValue

        self.totalSteps = self.maximumFilterValue // self.filterStep

        if not get(inputRow, 'search type') == 'location':
            self.totalSteps = 1

        self.minimumRank = 0
        self.maximumRank = 0

        status = ''

        self.log.info(f'On keyword {self.inputRowIndex + 1} of {len(self.inputRows)}: {get(self.inputRow, "keyword")}')

        for self.filterIndex in range(0, self.totalSteps):
            self.log.info(f'Searching for {get(inputRow, "keyword")}. Search type: {get(inputRow, "search type")}.')

            for searchSite in searchSites:
                status = self.getPages(inputRow, searchSite)
                
                if status == 'should stop':
                    break

            # because these don't use filters
            if not get(inputRow, 'search type') == 'location':
                break

            if self.reachedSearchLimit():
                break

    def getPages(self, inputRow, searchSite):
        status = 'should continue'

        self.pageIndex = 0
        self.afterId = ''

        while True:
            try:
                status = self.getSearchResultsPage(inputRow, searchSite)
            except Exception as e:
                helpers.handleException(e)

            self.pageIndex += 1

            # only want one result for google
            if searchSite == 'google.com':
                status = 'should stop'
                break

            if status == 'should stop':
                break
            
            # backup
            if self.pageIndex > 500:
                self.log.debug('Stopping. Too many pages.')
                break

        return status

    def getSearchResultsPage(self, inputRow, searchSite):
        result = 'should continue'

        self.setLogPrefix(inputRow)
        
        if self.pageIndex > 0:
            self.log.info(f'Getting page {self.pageIndex + 1}')

        if searchSite == 'google.com':
            self.api.proxies = self.internet.getRandomProxy()
        else:
            self.api.proxies = None
        
        keyword = get(inputRow, 'keyword')
        
        searchResults = []

        if searchSite == 'google.com':
            self.google.captcha = False
            searchResults = self.google.search(f'site:crunchbase.com "{keyword}"', 5)

            if self.google.captcha:
                return 'should stop'
        elif searchSite == 'crunchbase.com':
            # use crunchbase search as a backup
            if os.path.exists('user-data/credentials/www.crunchbase.com.har'):
                self.api.options['randomizeUserAgent'] = 0
                self.api.setHeadersFromHarFile('user-data/credentials/www.crunchbase.com.har', '/v4/data/searches/organizations?source=slug')
                self.api.options['randomizeUserAgent'] = 1
            else:
                self.api.setHeadersFromHarFile('program/resources/headers-search.json', '')

            if get(inputRow, 'search type') == 'location':
                toSend = helpers.getJsonFile('program/resources/body-search.json')
                
                if self.options['refreshOnly']:
                    datePredicate = helpers.getJsonFile('program/resources/recently-founded.json')
                    toSend['query'][1] = datePredicate
                    toSend['query'][1]['values'][0] = self.options['dateForNewCompaniesSearch']

                else:
                    toSend['query'][0]['values'][0] = keyword

                    self.minimumRank = 0 + (self.filterIndex * self.filterStep)
                    
                    self.maximumRank = 0 + ((self.filterIndex + 1) * self.filterStep)
                    self.maximumRank = self.maximumRank - 1

                    toSend['query'][1]['values'] = [self.minimumRank, self.maximumRank]

                if self.afterId:
                    toSend["after_id"] = self.afterId

                toSend = json.dumps(toSend)

                response = self.api.post(f'/v4/data/searches/organizations?source=slug', data=toSend, responseIsJson=False, returnResponseObject=True)
            else:
                response = self.api.get(f'/v4/data/autocompletes?query={keyword}&collection_ids=organizations&limit=25&source=topSearch', responseIsJson=False, returnResponseObject=True)

            self.handleCaptcha(response)

            if response and response.text:
                searchResults = json.loads(response.text)

            self.totalSearchResults = get(searchResults, 'count')
            
            self.waitBetweenRequests()

            searchResults = get(searchResults, 'entities')

            if self.pageIndex == 0:
                self.log.info(f'There are {self.totalSearchResults} results for this search')
            
            self.log.debug(f'Found {len(searchResults)} results on this page')

            if get(inputRow, 'search type') == 'company':
                if searchResults and len(searchResults) > 0:
                    searchResults = searchResults[0:1]
            else:
                if len(searchResults):
                    lastResult = searchResults[-1]
                    self.afterId = get(lastResult, 'uuid')
                else:
                    self.log.info('Reached end of search results')
                    self.afterId = ''
                    result = 'should stop'

        for searchResult in searchResults:
            try:
                if get(inputRow, 'search type') == 'location':
                    searchResult = get(searchResult, 'properties')
                    # it has at least one result
                    result = 'success'

                url = ''
                
                if searchSite == 'google.com':
                    if not '/organization/' in searchResult:
                        continue 

                    url = '/organization/' + self.getProfileId(searchResult)
                elif searchSite == 'crunchbase.com':
                    url = helpers.getNested(searchResult, ['identifier', 'permalink'])
                    url = '/organization/' + url

                result = self.handleSearchResult(inputRow, url, searchResult, searchSite)

                if not get(inputRow, 'search type') == 'location':
                    # only want first result
                    break

                if self.reachedSearchLimit():
                    self.afterId = ''
                    result = 'should stop'
                    self.log.info(f'Stopping. Reached limit of {self.options["searchResultLimit"]} results')
                    break
            except Exception as e:
                helpers.handleException(e)

        if get(inputRow, 'search type') == 'location' and self.searchResultsCount == self.totalSearchResults:
            self.log.info('Reached end of search results')
            self.afterId = ''
            result = 'should stop'

        return result

    def handleSearchResult(self, inputRow, url, searchResult, searchSite):
        result = 'should continue'

        self.searchResultsCount += 1            

        if not self.passesFilters(searchResult, url, searchSite):
            return result

        self.setLogPrefix(inputRow)

        profile = self.getProfile(url)

        self.output(inputRow, profile)

        return 'success'

    def reachedSearchLimit(self):
        result = False
        
        if self.options['searchResultLimit'] > 0 and self.searchResultsCount >= self.options['searchResultLimit']:
            result = True

        return result

    def refreshOnly(self):
        # find rows not update in a long time
        minimumDate = helpers.getDateStringSecondsAgo(self.options['hoursBetweenRuns'] * 3600, True)

        printableDate = helpers.findBetween(minimumDate, '', '.')
        
        self.log.info(f'Refreshing all results not checked since {printableDate}')

        rows = self.database.get('result', '*', f"gmDate < '{minimumDate}'")

        random.shuffle(rows)

        for i, row in enumerate(rows):
            try:
                self.log.info(f'Refreshing result {i + 1} of {len(rows)}: {get(row, "permalink")}')
                
                self.setLogPrefix(None, f'Refreshing {i + 1} of {len(rows)}: {get(row, "permalink")}')

                url = '/organization/' + get(row, 'permalink')

                self.inputRow = {
                    'keyword': get(row, 'keyword')
                }

                profile = self.getProfile(url)

                self.output(None, profile)
            except Exception as e:
                helpers.handleException(e)

        self.log.info(f'Done refreshing')

    def setLogPrefix(self, inputRow, line=''):
        if not line:
            line = f'Keyword {self.inputRowIndex + 1} of {len(self.inputRows)}: {get(self.inputRow, "keyword")}'
        
            if get(inputRow, 'search type') == 'location':
                line = f'{get(self.inputRow, "keyword")}. Filter {self.filterIndex + 1} of {self.totalSteps}: rank {self.minimumRank} - {self.maximumRank}. Page: {self.pageIndex + 1}. Results: {self.searchResultsCount}.'
        
        helpers.setLogPrefix(self.log, line)

    def getProfileUrl(self, url):
        return 'https://www.crunchbase.com/organization/' + self.getProfileId(url)

    def getProfileId(self, url):
        return helpers.findBetween(url, '/organization/', '/')

    def passesFilters(self, searchResult, url, searchSite):
        result = True

        key = 'id'
        id = ''

        if searchSite == 'google.com':
            key = 'permalink'
            id = self.getProfileId(searchResult)
        elif searchSite == 'crunchbase.com':
            id = helpers.getNested(searchResult, ['identifier', 'uuid'])

        name = self.getProfileId(url)

        if self.inDatabaseAndNewEnough(key, id, name):
            result = False

        return result

    def inDatabaseAndNewEnough(self, key, value, name):
        result = False

        # is it too old?
        minimumDate = helpers.getDateStringSecondsAgo(self.options['hoursBetweenRuns'] * 3600, True)

        row = self.database.getFirst('result', '*', f"{key} = '{value}' and gmDate >= '{minimumDate}'")

        if row:
            date = helpers.findBetween(get(row, 'gmDate'), '', '.')
            self.log.info(f'Skipping {name}. Already in the database and was updated less than {self.options["hoursBetweenRuns"]} hours ago. Updated: {date}.')
            result = True
        
        return result

    def storeToDatabase(self, inputRow, newResult):
        if get(newResult, 'json'):
            if inputRow:
                newResult['json']['inputRow'] = inputRow
            
            newResult['json'] = json.dumps(newResult['json'], indent=4)

        self.database.insert('result', newResult)

    def getReady(self):
        helpers.makeDirectory(os.path.dirname(self.options['outputFile']))

        # should restart or resume search from where left off?
        if os.path.exists(self.options['outputFile']):
            if not self.options['resumeSearch']:
                # move old output file
                rotatedFileName = self.options['outputFile'] + '.old'
                helpers.removeFile(rotatedFileName)
                os.rename(self.options['outputFile'], rotatedFileName)

    def waitBetweenRequests(self, type=None):
        if type == 'profile':
            if get(self.options, 'secondsBetweenProfiles'):
                time.sleep(self.options['secondsBetweenProfiles'])
        else:
            if get(self.options, 'secondsBetweenSearches'):
                time.sleep(self.options['secondsBetweenSearches'])

    def getDocument(self, url):
        response = self.api.get(url, None, False, True)
        self.waitBetweenRequests('profile')

        if response and 'verify you are a human' in response.text:
            self.log.error('There is a captcha')
            return None
        
        document = lh.fromstring(response.content)
        return document

    def isDone(self):
        result = False

        if not self.options['resumeSearch']:
            return result

        if '--debug' in sys.argv:
            return result

        minimumDate = helpers.getDateStringSecondsAgo(self.options['hoursBetweenRuns'] * 3600, True)

        row = self.database.getFirst('history', '*', f"gmDateCompleted > '{minimumDate}'", 'gmDateCompleted', 'desc')

        if row:
            self.log.info(f'Waiting until next run. Finished less than {self.options["hoursBetweenRuns"]} hours ago.')
            result = True

        return result

    def removeFromCsvFile(self, id, outputFile):
        lines = helpers.getFile(outputFile).splitlines()
        
        newFile = ''

        for line in lines:
            if f',{id},' in line:
                continue

            newFile += line + '\n'

        helpers.toFile(newFile, outputFile)

    def handleCaptcha(self, response):
        if response == '' or response == None:
            return

        hasCaptcha = False

        if response.status_code == 403:
            hasCaptcha = True
        elif response.text and 'verify you are a human' in response.text:
            hasCaptcha = True

        if hasCaptcha:
            self.log.error('There is a captcha')
            helpers.wait(random.randrange(60 * 60, 120 * 60))

    def checkProxy(self):
        if random.randrange(0, 100) == 0:
            original = self.api.urlPrefix
            self.api.urlPrefix = ''

            try:
                ipInformation = self.api.get('https://ipinfo.io/json')
            except Exception as e:
                helpers.handleException(e)

            self.log.debug(ipInformation)
            self.api.urlPrefix = original

    def markDone(self):
        history = {
            'gmDate': str(self.gmDateStarted),
            'gmDateCompleted': str(datetime.utcnow())
        }

        self.database.insert('history', history)

    def waitForNextRun(self):
        self.log.info('Done this run')

        if self.options['runRepeatedly'] == 0:
            self.log.info('Run repeatedly is 0. Not running again.')
            return False

        # added a few seconds for a margin of error
        nextDay = self.gmDateStarted + timedelta(hours=self.options['hoursBetweenRuns'], seconds=10)

        helpers.waitUntil(nextDay)

        return True

    def __init__(self, options, credentials):
        self.options = options
        self.log = logging.getLogger(get(self.options, 'loggerName'))

        self.database = Database('program/resources/tables.json')

        self.api = Api('https://www.crunchbase.com', self.options)
        self.api.timeout = 15
        self.api.cachePostRequests = True
        
        self.internet = Internet(self.options)
        self.website = Website(self.options)
        self.google = Google(self.options)
        self.google.internet = self.internet

        self.filterIndex = 0
        self.minimumRank = 0
        self.maximumRank = 0
        self.pageIndex = 0
        self.afterId = ''
        self.totalSearchResults = 0