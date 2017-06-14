import os
import numpy as np
import pandas as pd
from functions import login_salesforce, get_SF_data

project_path = '/Users/Gabi/Documents/projects/reporting/'
data_path = '/Users/Gabi/Documents/projects/reporting/data'


conn_SF = login_salesforce()


stages = ['Prospect', 'Qualified', 'Buying Process id.', 'Short List', 'Chosen Vendor', 'Negotiation/Review',
          'PO In Progress', 'Closed Won']
lost = ['Closed Deferred', 'Closed Lost']
not_pipeline = ['Qualified','Qualification','Prospect','Re-Qualify']

# dict to order stages
stage_order = dict(zip(stages, xrange(0, len(stages))))

industry_map = {
    "Asset Management": "Financial Services",
    "Banking": "Financial Services",
    "Insurance": "Financial Services",
    "Financial Services: Other": "Financial Services",
    "Healthcare Payer": "Healthcare",
    "Healthcare Provider": "Healthcare",
    "Healthcare Payer and Provider": "Healthcare",
    "Technology": "Technology",
    "Government: Federal": "Federal",
    "Government": "Federal",
    "Government: State and Local": "Federal",
    "Life Sciences": "Life Sciences"}

'''
Opportunity Snapshot
'''

q_snap = "Select o.Report_Date__c, o.Stage__c, o.Amount__c, ARR_Delta_EURO__c, o.Type__c, o.Close_Date__c, o.Industry__c, "
q_snap += "Created_Date__c, CurrencyIsoCode, Date_Opp_Moved_40__c, Opportunity_ID__c "
q_snap += "from dbo.Opportunity_Snapshot__c o "
q_snap += "where o.Type__c in ('New Business','Existing Business') "
q_snap += "and o.Stage__c not in ('Closed Won','Closed Lost','Closed Deferred','Re-Qualify');"

# get Opportunity Snapshot - SF backup data
df_snap = get_SF_data(connection=conn_SF, query_string=q_snap)

# clean up industry
df_snap['grouped_industry'] = df_snap.Industry__c.map(lambda  x: industry_map.get(x,'Other'))
# clean up date columns
date_cols = ['Created_Date__c', 'Close_Date__c','Report_Date__c']
df_snap[date_cols] = df_snap[date_cols].applymap(lambda x: np.datetime64(x))
# df_snap[date_cols] = df_snap[date_cols].applymap(lambda x: x.date())
# clean up numeric columns
numeric_cols = ['Amount__c','ARR_Delta_EURO__c']
df_snap[numeric_cols] = df_snap[numeric_cols].fillna(0.0)
df_snap[numeric_cols] = df_snap[numeric_cols].applymap(lambda x: np.float(x))
# create periods
monthly_periods = pd.period_range(df_snap.Report_Date__c.min(),df_snap.Report_Date__c.max(), freq='M')
quarter_periods = pd.period_range(df_snap.Report_Date__c.min(),df_snap.Report_Date__c.max(), freq='Q-SEP')
# add in Fiscal Quarter and Month
df_snap['quarter'] = df_snap.Report_Date__c.dt.to_period('Q-SEP')
df_snap['month'] = df_snap.Report_Date__c.dt.to_period('M')

# Filter out Closed Won, Closed Lost and Closed Deferred
df_snap = df_snap[~(df_snap.Stage__c.isin(['Closed Won','Closed Lost','Closed Deferred']))]
# Clean up old stage data
df_snap.Stage__c.replace('Qualification', 'Qualified', inplace = True)
df_snap = df_snap[df_snap.Stage__c != 'Re-Qualify']

rpt_dates_1 = ['10-15-2015','1-14-2016','4-14-2016','7-14-2016','10-14-2016','1-13-2017','4-14-2017','7-14-2017']
rpt_dates_1 = [pd.to_datetime(x) for x in rpt_dates_1]
rpt_dates_1 = [x.date() for x in rpt_dates_1]
rpt_dates_2 =['10-1-2015','11-5-2015','12-3-2015','1-7-2016','2-4-2016','3-3-2016','4-7-2016','5-5-2016','6-2-2016',
             '7-7-2016','8-5-2016','9-2-2016','10-7-2016','11-4-2016','12-2-2016','1-6-2017','2-3-2017','3-3-2017','4-7-2017',
             '5-5-2017','6-2-2017','7-7-2017','8-4-2017','9-1-2017']
rpt_dates_2 = [pd.to_datetime(x) for x in rpt_dates_2]
rpt_dates_2 = [x.date() for x in rpt_dates_2]

qt_rpt_dates = {
    pd.Period('2017Q1', freq = 'Q-SEP'):pd.to_datetime('2016-10-14').date(),
    pd.Period('2017Q2', freq = 'Q-SEP'):pd.to_datetime('2017-01-13').date(),
    pd.Period('2017Q3', freq = 'Q-SEP'):pd.to_datetime('2017-04-14').date(),
    pd.Period('2017Q4', freq = 'Q-SEP'):pd.to_datetime('2017-07-14').date()
}
# Filter out dates
df_snap = df_snap[(df_snap.Report_Date__c.isin(rpt_dates_1)) | (df_snap.Report_Date__c.isin(rpt_dates_2))]
# add order for stage sorting
df_snap['stage_order'] = df_snap.Stage__c.map(lambda x : stage_order.get(x))

# write to csv
df_snap.to_csv(data_path + '/output/'+ 'snapshot.csv',encoding ='utf-8')


'''
Sales Sourced Leads
'''
q_leads =  "select l.leadSource, l.CreatedDate, l.title_Score__c, l.Managed_By__c from dbo.Lead as l"


lead_mapping = {'Rep List': 'Sales Rep',
                'Rep Sourced Social': 'SDR',
                'Field Event':'Collibra Event'}
lead_source_mapping ={
    'Content Syndication': 'Marketing',
    'Direct Mail': 'Marketing',
    'Email': 'Marketing',
    'Field Event' : 'Marketing',
    'Paid Search': 'Marketing',
    'Seminar/Conference': 'Marketing',
    'Social Media': 'Marketing',
    'TechTarget': 'Marketing',
    'Webinar': 'Marketing',
    'Website': 'Marketing',
    'Partner': 'Sales',
    'Sales Operations':'Sales',
    'Sales Rep':'Sales',
    'SDR':'Sales',
    'University':'tbd'}

# get Sales Sourced Leads - SF backup data
df_leads = get_SF_data(connection=conn_SF, query_string=q_leads)
# clean up Created date
df_leads.CreatedDate = df_leads.CreatedDate.map(lambda x: np.datetime64(x))
# add in Fiscal Quarter and Month based on Created Date
df_leads['quarter'] = df_leads.CreatedDate.dt.to_period('Q-SEP')
df_leads['month'] = df_leads.CreatedDate.dt.to_period('M')

df_leads['source'] = df_leads.leadSource.map(lambda x: lead_source_mapping.get(x,'Old'))
# this is the updated lead sources based on updates made by Erica
df_leads['updated_leadSource'] = df_leads.leadSource
df_leads.updated_leadSource = df_leads.updated_leadSource.replace({'Rep List':'Sales Rep',
                                    'Rep Sourced Social':'SDR','Field Event':'Collibra Event'}, axis=0)

# Sales Sourced Leads - Direct Sales
sales_leads = df_leads[df_leads.Managed_By__c=='Direct Sales']
# Sales Sourced Leads - Reseller
reseller_leads = df_leads[df_leads.Managed_By__c=='Reseller']
# Sales Sourced Leads - Partner Org
reseller_leads = df_leads[df_leads.Managed_By__c=='Partner Org']
# Sales Sourced Leads - Unmanaged
reseller_leads = df_leads[df_leads.Managed_By__c=='Unmanaged']


'''
Pipeline Generation
'''
q_pipe = "select u1.Name as CreatedBy, u2.Name as AccountOwner, t.ActivityDate as StartDate, "
q_pipe += "o.Name as OpportunityName,  t.AccountId, t.Attach_to_an_Existing_Opportunity__c, o.Id as OpportunityId, "
q_pipe += "o.LeadSource, o.Type, o.StageName, o.New_ARR__c, o.CreatedDate, o.CloseDate "
q_pipe += "from dbo.Task as t "
q_pipe += "inner join dbo.Opportunity as o "
q_pipe += "on o.Id = t.WhatID "
q_pipe += "inner join dbo.[User] as u1 "
q_pipe += "on t.CreatedById = u1.Id  " #--SDR Created
q_pipe += "inner join dbo.[User] as u2 "
q_pipe += "on o.OwnerId = u2.Id " #-- Acct Owner
q_pipe += "where t.Type = 'Product Intro' "
q_pipe += "and t.Created_by_Division__c = 'SDR' "
q_pipe += "and o.Name != '' "
q_pipe += "and t.Status != 'Deferred' "
q_pipe += "and t.ActivityDate >='2016-04-01' "
q_pipe += "and t.ActivityDate <= GetDate(); "

# get Pipeline Generation data - SF backup data
df_pipe = get_SF_data(connection=conn_SF, query_string=q_pipe)

# clean up date columns
date_cols = ['StartDate', 'CreatedDate','CloseDate']
df_pipe[date_cols] = df_pipe[date_cols].applymap(lambda x: np.datetime64(x))
df_pipe[date_cols] = df_pipe[date_cols].applymap(lambda x: x.date())



