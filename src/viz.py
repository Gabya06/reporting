from load_var import *

import pandas as pd

import plotly.plotly as py
import plotly.graph_objs as go

# snapshot charts: New ARR by Stage in last 6 quarters

## TODO:  show summary tables and viz for each data
# snapshot -
#   ARR by stage and quarter
#   ARR by industry and quarter
project_path = '/Users/Gabi/Documents/projects/reporting/'
data_path = 'Users/Gabi/Documents/projects/reporting/data'

snap_data = pd.read_csv(data_path+'snapshot.csv')


# Quarterly summary
# excludes closed won

# DataFrame for qt ARR by stage
qt_stage_ARR = pd.DataFrame(columns = stages[:-1], index = sorted(qt_rpt_dates.keys()))
# DataFrame for qt opps by stage
qt_stage_opp = pd.DataFrame(columns= stages[:-1], index = sorted(qt_rpt_dates.keys()))

for q in sorted(qt_rpt_dates.keys()):
    for s in stages[:-1]:
        temp = snap_data[(snap_data.Report_Date__c == qt_rpt_dates.get(q)) & (snap_data.Stage__c == s)]
        arr = temp.ARR_Delta_EURO__c.sum()
        qt_stage_ARR.ix[q][s] = arr

        num_opps = temp.Opportunity_ID__c.count()
        qt_stage_opp.ix[q][s] = num_opps



stage_ARR = snap_data.sort_values(by=['stage_order','quarter']).groupby(['quarter','stage_order','Stage__c']).ARR_Delta_EURO__c.sum()

stage_ARR = stage_ARR.reset_index()



# sales and marketing SQLS