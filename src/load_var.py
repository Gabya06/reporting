import pandas as pd

'''
Script used to load variables such as stages, reporting dates, quarter periods, industry mapping...
'''

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

# reporting dates for board reports - when opportunity Snapshot was run
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


