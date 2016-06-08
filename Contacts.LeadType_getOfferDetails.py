#!usr/bin/env python

from pyeloqua import Eloqua
from simple_salesforce import Salesforce
from datetime import datetime
import sys, os, logging
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

## Setup logging

logging.basicConfig(filename=os.environ['OPENSHIFT_LOG_DIR'] + '/Contacts.LeadType_getOfferDetails.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#### setup Salesforce session

sf = Salesforce(username=os.environ['SALESFORCE_USER'], password=os.environ['SALESFORCE_PASSWORD'], security_token=os.environ['SALESFORCE_TOKEN'])
logging.info("Salesforce session established")

#### Setup Eloqua session

elq = Eloqua(username=os.environ['ELOQUA_USER'], password=os.environ['ELOQUA_PASSWORD'], company=os.environ['ELOQUA_COMPANY'])
logging.info("Eloqua session established")

#### Setup vars for creating import/export defs

cdoID = elq.GetCdoId(cdoName = 'Contacts.LeadType')

myFilter = "'{{CustomObject[1269].Field[23345]}}' = 'GET CAMPAIGN DETAILS'"

findFields = ["Contacts.LeadType.MostRecent_Offer_PrimarySolution", "Contacts.LeadType.MostRecent_Offer_ProductService",
              "Contacts.LeadType.MostRecent_OfferID", "Contacts.LeadType.S_Data_Status", "Contacts.LeadType.S_Last_Error",
              "Email_Address1"]

myFields = elq.CreateFieldStatement(entity='customObjects', cdoID=cdoID, fields=findFields)

## Do the thing
logging.info("Begin checking for records with 'GET CAMPAIGN DETAILS TEST'")

exportDefName = 'Contacts.LeadType - Get Campaign Details ' + str(datetime.now())

cdoDef = elq.CreateDef(defType='exports', entity='customObjects', cdoID=1269, fields=myFields, filters = myFilter, defName=exportDefName)
logging.info("export definition created: " + cdoDef['uri'])

cdoSync = elq.CreateSync(defObject=cdoDef)
logging.info("export sync started: " + cdoSync['uri'])

status = elq.CheckSyncStatus(syncObject=cdoSync)
logging.info("sync successful; retreiving data")

data = elq.GetSyncedData(defObject=cdoDef)
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
            thisCampaign = sfdcCampaignsId[record['Contacts_LeadType_MostRecent_OfferID1']]
        except:
            record['Contacts_LeadType_S_Last_Error1'] = 'Error retreiving campaign details'
            record['Contacts_LeadType_S_Data_Status1'] = 'CAMPAIGN DETAIL ERROR'
        else:
            record['Contacts_LeadType_MostRecent_Offer_PrimarySol1'] = thisCampaign['Solution_Code_Family__c']
            record['Contacts_LeadType_MostRecent_Offer_ProductSer1'] = thisCampaign['Solution_Code__c']
            record['Contacts_LeadType_S_Data_Status1'] = 'CAMPAIGN DETAILS RETREIVED'
            if (thisCampaign['Solution_Code_Family__c']==None):
                nullCount += 1

    logging.info("Records with no Primary Solution: " + str(nullCount))


    importDefName = 'Contacts.LeadType - Get Campaign Details ' + str(datetime.now())

    cdoInDef = elq.CreateDef(defType='imports', entity='customObjects', cdoID=1269, fields=myFields, defName=importDefName, identifierFieldName='Email_Address1')
    logging.info("import definition created: " + cdoInDef['uri'])

    postInData = elq.PostSyncData(data = dataOut, defObject=cdoInDef, maxPost=20000)
    logging.info("Data successfully imported, job finished: " + str(datetime.now()))
else:
    logging.info("No records, job finished")

### Logging for Prometheus

registry = CollectorRegistry()
g = Gauge('job_last_success_unixtime', 'Last time a batch job successfully finished', registry=registry)
g.set_to_current_time()
h = Gauge('job_total_records_success', 'Total number of records successfully processed in last batch', registry=registry)
h.set(len(data))

push_to_gateway(os.environ['PUSHGATEWAY'], job='Contacts.LeadType_getOfferDetails', registry=registry)
