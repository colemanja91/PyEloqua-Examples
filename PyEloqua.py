#!usr/bin/env python

## package dependencies
import requests, argparse, json, warnings, time
from datetime import datetime

class Eloqua(object):

    def __init__(self, username=None, password=None, company=None, pod=None):

        if all(arg is not None for arg in (username, password, company, pod)):
            testUrl = 'https://secure.p0' + str(pod) + '.eloqua.com/API/REST/2.0/system/timeZones'
            req = requests.get(testUrl, auth=(company + '\\' + username, password))
            if req.status_code==200:
                self.username = username
                self.password = password
                self.company = company
                self.pod = pod
                self.urlBase = 'https://secure.p0' + str(pod) + '.eloqua.com'
            else:
                raise TypeError('Invalid login credentials; please double check')
        else:
            raise TypeError('You must provide all login credentials: company, username, password, and pod')

    ## Method: getFieldStatements
    ## Description: given a CDO ID, returns available field names and their API statements.
    ##              Useful for creating import / export definitions
    ## Parameters:
    ##      company: Eloqua instance company name; string
    ##      username: Eloqua username; string
    ##      password: Eloqua password; string
    ##      pod: Eloqua instance pod (Settings -> Company Defaults -> POD: POD{#}); integer
    ##      cdoID: Custom Data Object ID; integer
    ##      fields: List of fields to retreive from Eloqua (optional; if not used, method returns all fields); array
    ##
    ## Output: an array of dictionaries containing information on each field
    ##
    ## Usage:
    ##  fields = ['']
    ##  fieldStatements = getFieldStatements('mycompany', 'myuser', 'mypassword', 1, 123, fields)

    def getFieldStatements(self, cdoID, fields=[]):

        url = self.urlBase + '/API/Bulk/2.0/customobjects/' + str(cdoID) + '/fields'

        req = requests.get(url, auth = (self.company + '\\' + self.username, self.password))

        fieldsReturn = []

        if req.status_code == 200:

            if len(fields)>0:

                for item in req.json()['items']:

                    if item['name'] in fields:

                        fieldsReturn.append(item)

                if (len(fieldsReturn)<len(fields)):

                    warnings.warn("Not all fields could be found")

            else:

                fieldsReturn = req.json()['items']

            return fieldsReturn

        else:

            return req.status_code

    ## Method: createDef
    ##
    ## Description: Given a set of fields and filters, create an import / export definition
    ##
    ## Parameters:
    ##      company: Eloqua instance company name; string
    ##      username: Eloqua username; string
    ##      password: Eloqua password; string
    ##      pod: Eloqua instance pod (Settings -> Company Defaults -> POD: POD{#}); integer
    ##      defType: type of definition; string ['imports', 'exports']
    ##      defName: Export definition name; string; default = datetime stamp
    ##      cdoID: Custom Data Object ID; integer
    ##      fields: list of field name statements; dictionary; create using getFieldStatements
    ##      filters: a filter statement; string
    ##      identifierFieldName: Name of unique identifier field (imports only); string
    ##      isSyncTriggeredOnImport: automatically sync when data is POST'ed to the definiton; boolean
    ##
    ## Output: On successful definition creation, a dictionary containing details of the definition; if unsuccessful, a warning containing the returned JSON

    def createDef(self, defType, cdoID, fields, filters='', defName=datetime.now(), identifierFieldName='', isSyncTriggeredOnImport=False):

        if (len(fields)>100):

            raise Exception("Eloqua Bulk API can only export 100 CDO fields at a time")

        if (defType not in ['imports', 'exports']):

            raise Exception("Please choose a defType value: 'imports', 'exports'")

        ##if (defType=='imports' & len(identifierFieldName) not in fields.keys()):

        ##    raise Exception("Imports must had an identifierFieldName which is included in the specified fields")

        url = self.urlBase + '/API/Bulk/2.0/customobjects/' + str(cdoID) + '/' + defType

        headers = {'Content-Type':'application/json'}

        if (defType=='exports'):

            if (len(filters)>0):

                data = {'name': defName, 'filter': filters, 'fields': fields}

            else:

                data = {'name': defName, 'fields': fields}

        else:

            data = {'name': defName, 'fields': fields, 'identifierFieldName': identifierFieldName, 'isSyncTriggeredOnImport': isSyncTriggeredOnImport}

        req = requests.post(url, data = json.dumps(data), headers = headers, auth = (self.company + '\\' + self.username, self.password))

        if req.status_code==201:

            return req.json()

        else:

            raise Exception(req.json()['failures'][0])

    ## Create a sync for an existing definition

    def createSync(self, defObject={}, defURI=''):

        if ('uri' not in defObject):

            if (len(defURI)==0):

                raise Exception("Must include a valid defObject or defURI")

            else:

                uri = defURI

        else:

            uri = defObject['uri']

        url = self.urlBase + '/API/Bulk/2.0/syncs'

        headers = {'Content-Type':'application/json'}

        data = {'syncedInstanceUri': uri}

        req = requests.post(url, data = json.dumps(data), headers = headers, auth = (self.company + '\\' + self.username, self.password))

        if req.status_code==201:

            return req.json()

        else:

            raise Exception("Could not create sync: " + uri)


    ## Check a sync status

    def checkSyncStatus(self, syncObject={}, syncURI='', timeout=500):
        if ('uri' not in syncObject):
            if (len(syncURI)==0):
                raise Exception("Must include a valid syncObject or syncURI")
            else:
                uri = syncURI
        else:
            uri = syncObject['uri']

        url = self.urlBase + '/API/Bulk/2.0' + uri

        waitTime = 0
        notSynced = True
        while notSynced:
            req = requests.get(url, auth = (self.company + '\\' + self.username, self.password))
            if req.status_code != 200:
                warnings.warn(req.json())
            status = req.json()['status']
            if (status == 'success'):
                return 'success'
            elif (status in ['warning', 'error']):
                raise Exception("Sync finished with status 'warning' or 'error': " + uri)
            elif (waitTime<timeout):
                waitTime += 10
                time.sleep(10)
            else:
                raise Exception("Export not finished syncing after " + str(waitTime) + " seconds: " + uri)


    ## Get synced data

    def getSyncedData(self, defObject={}, defURI='', limit=50000):
        if ('uri' not in defObject):
            if (len(defURI)==0):
                raise Exception("Must include a valid defObject or defURI")
            else:
                uri = defURI
        else:
            uri = defObject['uri']

        offset = 0

        url = self.urlBase + '/API/Bulk/2.0' + uri + '/data?'

        results = []

        hasMore = True

        while (hasMore):
            urlWhile = url + 'offset=' + str(offset) + '&limit=' + str(limit)
            req = requests.get(urlWhile, auth=(self.company + '\\' + self.username, self.password))
            if 'items' in req.json():
                items = req.json()['items']
                for item in items:
                    results.append(item)
                offset += limit
            hasMore = req.json()['hasMore']

        return results

    ## Import data

    def postSyncData(self, data, defObject={}, defURI='', maxPost=20000, syncCount=80000):
        if ('uri' not in defObject):
            if (len(defURI)==0):
                raise Exception("Must include a valid defObject or defURI")
            else:
                uri = defURI
        else:
            uri = defObject['uri']

        if (maxPost>20000):
            raise Exception("It is not recommended to POST more than 20,000 records at a time. Please indicate a different maxPost value")

        if (syncCount>80000):
            raise Exception("It is recommended to sync at least every 80,000 records. Please indicate a different syncCount value")

        if (len(data)==0):
            raise Exception("Input data length is 0")

        hasMore = True
        offset = 0
        syncOffset = 0
        sendSet = []
        dataLen = len(data)
        url = self.urlBase + '/API/Bulk/2.0' + uri + '/data'
        headers = {'Content-Type':'application/json'}

        while (hasMore):
            for x in range(offset, min(offset+maxPost, dataLen), 1):
                sendSet.append(data[x])

            req = requests.post(url, data = json.dumps(sendSet), headers = headers, auth = (self.company + '\\' + self.username, self.password))

            if req.status_code == 204:

                syncOffset += maxPost

                if (syncOffset >= syncCount or offset+maxPost>=dataLen):
                    syncOffset = 0
                    importSync = self.createSync(defObject=defObject, defURI=defURI)
                    syncStatus = self.checkSyncStatus(syncObject=importSync)

                if offset+maxPost>=dataLen:
                    hasMore = False
                    return 'success'
                else:
                    offset += maxPost
                    sendSet = []
            else:
                #raise Exception("Could not import data (" + uri + "): " + req.json()['failures'][0])
                raise Exception(req.json()['failures'][0])
