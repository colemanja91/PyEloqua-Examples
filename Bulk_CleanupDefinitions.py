from pyeloqua import Eloqua
from datetime import datetime
import requests
import sys, os, logging
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

## Setup logging

logging.basicConfig(filename=os.environ['OPENSHIFT_LOG_DIR'] + '/Bulk_CleanupDefinitions.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

## setup session

elq = Eloqua(username=os.environ['ELOQUA_USER'], password=os.environ['ELOQUA_PASSWORD'], company=os.environ['ELOQUA_COMPANY'])
logging.info("Eloqua session established")

## Get all definitions
logging.info("Getting exports/imports")

defs = []

exports = elq.GetDef(defType='exports')

defs.extend(exports)

imports = elq.GetDef(defType='imports')

defs.extend(imports)

logging.info("Got all exports/imports: " + str(len(defs)))

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
            logging.info("NOT deleted: " + item['uri'])
            deleteError += 1


logging.info("Total # of defs deleted: " + str(deleteCount))
logging.info("Total # of defs NOT deleted: " + str(deleteError))

# Prometheus logging

registry = CollectorRegistry()
g = Gauge('job_last_success_unixtime', 'Last time a batch job successfully finished', registry=registry)
g.set_to_current_time()
h = Gauge('job_total_records_success', 'Total number of records successfully processed in last batch', registry=registry)
h.set(deleteCount)
j = Gauge('job_total_records_failed', 'Total number of records failed processing in last batch')
j.set(deleteError)

push_to_gateway(os.environ['PUSHGATEWAY'], job='Bulk_CleanupDefinitions', registry=registry)
