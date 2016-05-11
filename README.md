# PyEloqua-Examples
Examples of using Python alongside Eloqua

# Setup
All of the examples are created to run on Openshift. To replicate, you need to create an account (https://www.openshift.com, a free account allows you to run up to three apps for free). Then install rhc (https://developers.openshift.com/managing-your-applications/client-tools.html). Now, from command line, you can create a Python gear to run these scripts:

```
rhc app create MYPYELOQUAAPP python-3.3 cron-1.7
```

You can find more details about creating and using your app here: https://developers.openshift.com/managing-your-applications/creating-applications.html

# PyEloqua.py
Functions for interacting with Eloqua's Bulk API. Currently, has only been tested with import/export of CDO record,s although more functionality is planned.

# Contacts.LeadType_getOfferDetails.py
A script which uses PyEloqua to export records from a CDO, find the corresponding campaign (offer) details in Salesforce, then re-import the records.
Note: This script uses an instance-specific CDO and the code cannot be directly re-used without modification.
