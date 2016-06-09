from pyeloqua import Eloqua
from datetime import datetime
import requests
import sys, os, logging

## Setup logging

# logging.basicConfig(filename=os.environ['OPENSHIFT_LOG_DIR'] + '/Bulk_CleanupDefinitions.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

## setup session

elq = Eloqua(username=os.environ['ELOQUA_USER'], password=os.environ['ELOQUA_PASSWORD'], company=os.environ['ELOQUA_COMPANY'])
print("Eloqua session established")

## Get all definitions
print("Getting exports/imports")

defs = []

exports = elq.GetDef(defType='exports')

defs.extend(exports)

imports = elq.GetDef(defType='imports')

defs.extend(imports)

print("Got all exports/imports: " + str(len(defs)))

## Create a set of defs to delete

dayLimit = 14

deleteCount = 0
deleteError = 0

for item in defs:
    createdAt = datetime.strptime(item['createdAt'][:-2], '%Y-%m-%dT%H:%M:%S.%f')

    delta = datetime.now() - createdAt

    if (delta.days > dayLimit):
        url = elq.bulkBase + item['uri']

        req = requests.delete(url, auth = elq.auth)

        if req.status_code==204:
            deleteCount += 1
        else:
            print("NOT deleted: " + item['uri'])
            deleteError += 1


print("Total # of defs deleted: " + str(deleteCount))
print("Total # of defs NOT deleted: " + str(deleteError))
