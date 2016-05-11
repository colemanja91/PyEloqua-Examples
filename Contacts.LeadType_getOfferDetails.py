#!usr/bin/env python

from PyEloqua import Eloqua
from simple_salesforce import Salesforce
from datetime import datetime
import sys, os, logging

## Setup logging

logging.basicConfig(filename=os.environ['OPENSHIFT_LOG_DIR'] + '/Contacts.LeadType_getOfferDetails.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#### setup Salesforce session

sf = Salesforce(username=os.environ['SALESFORCE_USER'], password=os.environ['SALESFORCE_PASSWORD'], security_token=os.environ['SALESFORCE_TOKEN'])
logging.info("Salesforce session established")

#### Setup Eloqua session

elq = Eloqua(username=os.environ['ELOQUA_USER'], password=os.environ['ELOQUA_PASSWORD'], company=os.environ['ELOQUA_COMPANY'], pod=os.environ['ELOQUA_POD'])
logging.info("Eloqua session established")

#### Setup vars for creating import/export defs

myFilter = "'{{CustomObject[1269].Field[23345]}}' = 'GET CAMPAIGN DETAILS'"

fields = {
        "Contacts_LeadType_MostRecent_Offer_PrimarySolution": "{{CustomObject[1269].Field[23339]}}",
        "Contacts_LeadType_MostRecent_Offer_ProductService": "{{CustomObject[1269].Field[23340]}}",
        "Contacts_LeadType_MostRecent_OfferID": "{{CustomObject[1269].Field[23338]}}",
        "Contacts_LeadType_S_Data_Status": "{{CustomObject[1269].Field[23345]}}",
        "Contacts_LeadType_S_Last_Error": "{{CustomObject[1269].Field[23349]}}",
        "Email_Address": "{{CustomObject[1269].Field[23337]}}",
        "DataCardIDExt": "{{CustomObject[1269].ExternalId}}"
}

## Do the thing
logging.info("Begin checking for records with 'GET CAMPAIGN DETAILS TEST'")

exportDefName = 'Contacts.LeadType - Get Campaign Details ' + str(datetime.now())

cdoDef = elq.createDef(defType='exports', cdoID=1269, fields=fields, filters = myFilter, defName=exportDefName)
logging.info("export definition created: " + cdoDef['uri'])

cdoSync = elq.createSync(defObject=cdoDef)
logging.info("export sync started: " + cdoSync['uri'])

status = elq.checkSyncStatus(syncObject=cdoSync)
logging.info("sync successful; retreiving data")

data = elq.getSyncedData(defObject=cdoDef)
logging.info("# of records:" + str(len(data)))

if (len(data)>0):

    dataOut = data

    nullCount = 0

    sfdcCampaigns = sf.query_all("SELECT Id, Solution_Code_Family__c, Solution_Code__c FROM Campaign")
    sfdcCampaignsId = dict( (d['Id'], d) for d in sfdcCampaigns['records'])

    logging.info("Retreived SFDC Campaigns")

    for record in dataOut:
        try:
            ##thisCampaign = sf.Campaign.get(record['Contacts_LeadType_MostRecent_OfferID'])
            thisCampaign = sfdcCampaignsId[record['Contacts_LeadType_MostRecent_OfferID']]
        except:
            record['Contacts_LeadType_S_Last_Error'] = 'Error retreiving campaign details'
            record['Contacts_LeadType_S_Data_Status'] = 'CAMPAIGN DETAIL ERROR'
        else:
            record['Contacts_LeadType_MostRecent_Offer_PrimarySolution'] = thisCampaign['Solution_Code_Family__c']
            record['Contacts_LeadType_MostRecent_Offer_ProductService'] = thisCampaign['Solution_Code__c']
            record['Contacts_LeadType_S_Data_Status'] = 'CAMPAIGN DETAILS RETREIVED'
            if (thisCampaign['Solution_Code_Family__c']==None):
                nullCount += 1

    logging.info("Records with no Primary Solution: " + str(nullCount))


    importDefName = 'Contacts.LeadType - Get Campaign Details ' + str(datetime.now())

    cdoInDef = elq.createDef(defType='imports', cdoID=1269, fields=fields, defName=importDefName, identifierFieldName='Email_Address')
    logging.info("import definition created: " + cdoInDef['uri'])

    postInData = elq.postSyncData(data = dataOut, defObject=cdoInDef, maxPost=20000)
    logging.info("Data successfully imported, job finished: " + str(datetime.now()))
else:
    logging.info("No records, job finished")
