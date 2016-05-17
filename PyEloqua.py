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
    ##      entity: what type of fields to retrieve
    ##      cdoID: Custom Data Object ID; integer
    ##      fields: List of fields to retreive from Eloqua (optional; if not used, method returns all fields); array
    ##
    ## Output: an array of dictionaries containing information on each field
    ##
    ## Usage:
    ##  fields = ['']
    ##  fieldStatements = getFieldStatements(123, fields)

    def getFields(self, entity, cdoID=0, fields=[]):

        if entity not in ['contacts', 'customObjects', 'activity', 'accounts']:
            raise Exception("Please choose a valid 'entity' value: 'contacts', 'accounts'")

        if entity == 'customObjects':
            if cdoID==0:
                raise Exception("Please specify a cdoID")

            url = self.urlBase + '/API/Bulk/2.0/customobjects/' + str(cdoID) + '/fields'
        else:
            url = self.urlBase + '/API/Bulk/2.0/' + entity + '/fields'

        req = requests.get(url, auth = (self.company + '\\' + self.username, self.password))

        fieldsReturn = []

        if req.status_code == 200:

            if len(fields)>0:

                for item in req.json()['items']:

                    if item['name'] in fields:

                        fieldsReturn.append(item)
                    else:
                        if item['internalName'] in fields:
                            fieldsReturn.append(item)

                if (len(fieldsReturn)<len(fields)):

                    warnings.warn("Not all fields could be found")

            else:

                fieldsReturn = req.json()['items']

            return fieldsReturn

        else:

            raise Exception("Failure getting fields: " + str(req.status_code))

    ## create field statement for import / export definition
    def createFieldStatement(self, entity, fields = '', cdoID = 0, useInternalName=True, addSystemFields=[]):
        fieldSet = self.getFields(entity = entity, fields = fields, cdoID = cdoID)

        fieldStatement = {}

        if len(addSystemFields)>0:
            for field in addSystemFields:
                if field in self.contactSystemFields:
                    fieldStatement[field] = self.contactSystemFields[field]
                else:
                    raise Exception("System field not recognized: " + field)

        if len(fieldSet)>0:
            for field in fieldSet:
                if useInternalName:
                    fieldStatement[field['internalName']] = field['statement']
                else:
                    fieldStatement[field['name']] = field['statement']
            return fieldStatement
        else:
            raise Exception("No fields found")

    ## get CDO ID from name
    def getCdoId(self, cdoName):
        url = self.urlBase + '/API/Bulk/2.0/customobjects?q="name=' + str(cdoName) + '"'

        req = requests.get(url, auth = (self.company + '\\' + self.username, self.password))

        if req.json()['totalResults']==1:
            cdoUri = req.json()['items'][0]['uri']
            cdoId = int(cdoUri.replace('/customObjects/', ''))
            return cdoId
        elif req.json()['totalResults']>1:
            raise Exception("Multiple CDOs with matching name")
        else:
            raise Exception("No matching CDOs found")

    ## create filter criteria based on shared filters, lists, segments, or account lists

    def filterExists(self, name, existsType):

        if existsType=='ContactFilter':
            url = self.urlBase + '/API/Bulk/2.0/contacts/filters?q="name=' + name + '"'
        elif existsType=='ContactSegment':
            url = self.urlBase + '/API/Bulk/2.0/contacts/segments?q="name=' + name + '"'
        elif existsType=='ContactList':
            url = self.urlBase + '/API/Bulk/2.0/contacts/lists?q="name=' + name + '"'
        elif existsType=='AccountList':
            url = self.urlBase + '/API/Bulk/2.0/accounts/lists?q="name=' + name + '"'
        else:
            raise Exception("Please choose a valid 'existsType': 'ContactFilter', 'ContactSegment', 'ContactList', 'AccountList'")

        req = requests.get(url, auth = (self.company + '\\' + self.username, self.password))

        if req.json()['totalResults']==1:
            filterStatement = "EXISTS('" + req.json()['items'][0]['statement'] + "')"
            return filterStatement
        elif req.json()['totalResults']>1:
            raise Exception("Multiple " + existsType + "s found")
        else:
            raise Exception("No matching " + existsType + " found")

    # Create export definition

    def createDef(self, defType, entity, fields, cdoID=0, filters='', defName=str(datetime.now()), identifierFieldName='', isSyncTriggeredOnImport=False):

        if (defType not in ['imports', 'exports']):
            raise Exception("Please choose a defType value: 'imports', 'exports'")

        if len(fields)==0:
            raise Exception("Please specify at least one field to export")

        if (defType == 'imports' and len(fields)>100):
            raise Exception("Eloqua Bulk API only supports imports of up to 100 fields")

        if entity not in ['contacts', 'customObjects', 'accounts']:
            raise Exception("Please choose a valid 'entity' value: 'contacts', 'accounts', 'customObjects'")

        if entity == 'customObjects':
            if cdoID==0:
                raise Exception("Please specify a cdoID")
            if (len(fields)>100):
                raise Exception("Eloqua Bulk API can only export 100 CDO fields at a time")
            url = self.urlBase + '/API/Bulk/2.0/customobjects/' + str(cdoID) + '/' + defType

        if entity == 'contacts':
            if (len(fields)>250):
                raise Exception("Eloqua Bulk API can only export 250 contact fields at a time")
            url = self.urlBase + '/API/Bulk/2.0/contacts/' + defType

        if entity == 'accounts':
            if len(fields)>100:
                raise Exception("Eloqua Bulk API can only export 100 account fields at a time")
            url = self.urlBase + '/API/Bulk/2.0/accounts/' + defType

        ##if (defType=='imports' & len(identifierFieldName) not in fields.keys()):

        ##    raise Exception("Imports must had an identifierFieldName which is included in the specified fields")

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

    contactSystemFields = {
        "contactID": "{{Contact.Id}}",
        "createdAt": "{{Contact.CreatedAt}}",
        "updatedAt": "{{Contact.UpdatedAt}}",
        "isSubscribed": "{{Contact.Email.IsSubscribed}}",
        "isBounced": "{{Contact.Email.IsBounced}}"
    }
