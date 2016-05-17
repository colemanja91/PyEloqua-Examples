#!usr/bin/env python

## Import necessary libraries
from pyeloqua import Eloqua
from datetime import datetime
import psycopg2
import sys, os, logging

## Setup logging which will write to the default Openshift log directory
logging.basicConfig(filename=os.environ['OPENSHIFT_LOG_DIR'] + '/Contacts_getArchivedLeads.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#### Setup Eloqua session
## Keep login details stored as environment variables
elq = Eloqua(username=os.environ['ELOQUA_USER'], password=os.environ['ELOQUA_PASSWORD'], company=os.environ['ELOQUA_COMPANY'], pod=os.environ['ELOQUA_POD'])
logging.info("Eloqua session established")

#### Setup vars for creating import/export defs

## create filter to grab from Archived Leads shared list

filters = elq.filterExists(name = "Archived Leads", existsType = "ContactList")

## create field statements

fields = elq.getFieldStatements(entity = "contacts",
                                fields = ['ZZ - Last SFDC Lead ID', 'ZZ - SFDC ContactID', 'ZZ - SFDC AccountID',
                                          'Email Address', 'Log: Lead Last Archived Path',
                                          'Timestamp: Lead Last Archived Date'],
                                addSystemFields = ['createdAt'])

## create export definition

exportDefName = 'Contacts - Get Archived Leads ' + str(datetime.now())

exportDef = elq.createDef(defType='exports', entity = 'contacts', fields = fields, filters = filters, defName = exportDefName)
logging.info("Export definition created")

## begin syncing

exportSync = elq.createSync(defObject = exportDef)
logging.info("Export sync started")

## Check sync status

status = elq.checkSyncStatus(syncObject = exportSync)
logging.info("Export synced")

## get synced data

logging.info("Getting data...")
data = elq.getSyncedData(defObject = exportDef)
logging.info("Retrieved rows: " + str(len(data)))

if len(data)>0:
    ## create terminator DB connection
    connstring = ""
    connstring += " dbname='" + os.environ['TERMINATOR_DB'] + "' "
    connstring += " user='" + os.environ['TERMINATOR_DB_USERNAME'] + "' "
    connstring += " host='" + os.environ['TERMINATOR_DB_HOST'] + "' "
    connstring += " password='" + os.environ['TERMINATOR_DB_PASSWORD'] + "' "
    connstring += " port='" + os.environ['TERMINATOR_DB_PORT'] + "' "
    conn = psycopg2.connect(connstring)
    logging.info("Connected to Terminator DB")
    cur=conn.cursor()

    logging.info("Loop through data and insert to Terminator")
    for row in data:
        sql = ""
        sql += " INSERT INTO eloqua_leads_archived "
        sql += " (eloqua_contact_id, sfdc_lead_id, sfdc_contact_id, sfdc_account_id, date_archived, archived_path, email_address, date_created) "
        sql += " values "
        sql += " (%s, %s, %s, %s, %s, %s, %s, %s)"

        dataVals = [row[z] for z in ("contactID", "C_SFDCLeadID", "C_SFDCContactID", "C_SFDCAccountID", "C_Lead_Last_Archived_Date1", "C_Lead_Last_Archived_Path1", "C_EmailAddress", "createdAt")]

        cur.execute(sql, dataVals)
    logging.info("done")
else:
    logging.info("No records")

logging.info("Hasta la vista, baby")
