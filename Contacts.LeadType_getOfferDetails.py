#!usr/bin/env python

## Import necessary libraries
from pyeloqua import Eloqua
from simple_salesforce import Salesforce
from datetime import datetime
import sys, os, logging

## Setup logging which will write to the default Openshift log directory

logging.basicConfig(filename=os.environ['OPENSHIFT_LOG_DIR'] + '/Contacts.LeadType_getOfferDetails.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#### setup Salesforce session
## Keep login details stored as environment variables

sf = Salesforce(username=os.environ['SALESFORCE_USER'], password=os.environ['SALESFORCE_PASSWORD'], security_token=os.environ['SALESFORCE_TOKEN'])
logging.info("Salesforce session established")

#### Setup Eloqua session
## Keep login details stored as environment variables

elq = Eloqua(username=os.environ['ELOQUA_USER'], password=os.environ['ELOQUA_PASSWORD'], company=os.environ['ELOQUA_COMPANY'], pod=os.environ['ELOQUA_POD'])
logging.info("Eloqua session established")

#### Setup vars for creating import/export defs

# Filter string: one criteria which filters for the field "Contacts.LeadType.S_Data_Status" = "GET CAMPAIGN DETAILS"
myFilter = "'{{CustomObject[1269].Field[23345]}}' = 'GET CAMPAIGN DETAILS'"

# Define which fields we want to export and import
fields = {
        "Contacts_LeadType_MostRecent_Offer_PrimarySolution": "{{CustomObject[1269].Field[23339]}}",
        "Contacts_LeadType_MostRecent_Offer_ProductService": "{{CustomObject[1269].Field[23340]}}",
        "Contacts_LeadType_MostRecent_OfferID": "{{CustomObject[1269].Field[23338]}}",
        "Contacts_LeadType_S_Data_Status": "{{CustomObject[1269].Field[23345]}}",
        "Contacts_LeadType_S_Last_Error": "{{CustomObject[1269].Field[23349]}}",
        "Email_Address": "{{CustomObject[1269].Field[23337]}}",
        "DataCardIDExt": "{{CustomObject[1269].ExternalId}}"
}

## Begin export
logging.info("Begin checking for records with 'GET CAMPAIGN DETAILS TEST'")

exportDefName = 'Contacts.LeadType - Get Campaign Details ' + str(datetime.now())

# Create an export definition
cdoDef = elq.createDef(defType='exports', cdoID=1269, fields=fields, filters = myFilter, defName=exportDefName)
logging.info("export definition created: " + cdoDef['uri'])

# Begin syncing the export definition we just created
cdoSync = elq.createSync(defObject=cdoDef)
logging.info("export sync started: " + cdoSync['uri'])

# Check the status of the sync until it completes or errors
status = elq.checkSyncStatus(syncObject=cdoSync)
logging.info("sync successful; retreiving data")

# Retrieve all data from the export
data = elq.getSyncedData(defObject=cdoDef)
logging.info("# of records:" + str(len(data)))

# If there is data, then process
if (len(data)>0):

    dataOut = data

    nullCount = 0

    # SOQL query to get all campaigns
    sfdcCampaigns = sf.query_all("SELECT Id, Solution_Code_Family__c, Solution_Code__c FROM Campaign")
    sfdcCampaignsId = dict( (d['Id'], d) for d in sfdcCampaigns['records'])

    logging.info("Retreived SFDC Campaigns")

    # For each record, get the appropriate SFDC Campaign Details
    for record in dataOut:
        try:
            thisCampaign = sfdcCampaignsId[record['Contacts_LeadType_MostRecent_OfferID']]
        except:
            # If an error, then write to the S_Last_Error field
            record['Contacts_LeadType_S_Last_Error'] = 'Error retreiving campaign details'
            record['Contacts_LeadType_S_Data_Status'] = 'CAMPAIGN DETAIL ERROR'
        else:
            record['Contacts_LeadType_MostRecent_Offer_PrimarySolution'] = thisCampaign['Solution_Code_Family__c']
            record['Contacts_LeadType_MostRecent_Offer_ProductService'] = thisCampaign['Solution_Code__c']
            record['Contacts_LeadType_S_Data_Status'] = 'CAMPAIGN DETAILS RETREIVED'
            # Do this to keep track of how many times there is no Primary Solution value
            if (thisCampaign['Solution_Code_Family__c']==None):
                nullCount += 1

    logging.info("Records with no Primary Solution: " + str(nullCount))


    importDefName = 'Contacts.LeadType - Get Campaign Details ' + str(datetime.now())

    # Create an Import definition
    cdoInDef = elq.createDef(defType='imports', cdoID=1269, fields=fields, defName=importDefName, identifierFieldName='Email_Address')
    logging.info("import definition created: " + cdoInDef['uri'])

    # Post and sync the data back into Eloqua
    postInData = elq.postSyncData(data = dataOut, defObject=cdoInDef, maxPost=20000)
    logging.info("Data successfully imported, job finished: " + str(datetime.now()))
else:
    logging.info("No records, job finished")