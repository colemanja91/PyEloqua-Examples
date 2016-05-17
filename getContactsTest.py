#!usr/bin/env python

from pyeloqua import Eloqua

elq = Eloqua(company='redhat', username='jecolema', password='@Redpoint74', pod=1)

print("Eloqua session established")

fields = elq.createFieldStatement(entity = 'contacts', fields=['First Name', 'C_EmailAddress', 'ZZ - SFDC LeadID'],
                                  addSystemFields=['contactID', 'createdAt', 'updatedAt', 'isBounced', 'isSubscribed'])

## stuff

filters = elq.filterExists(name = '20160513_jecolema_test', existsType = 'ContactList')

exportDef = elq.createDef(defType='exports', entity='contacts', fields=fields, filters=filters, defName='20160517_jecolema_test')

print("export definition created")

exportSync = elq.createSync(defObject = exportDef)

print("sync created")

status = elq.checkSyncStatus(syncObject=exportSync)

print("sync finished")

data = elq.getSyncedData(defObject=exportDef)

print(data)
