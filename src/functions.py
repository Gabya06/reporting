import beatbox
import pandas as pd
from pandas.tseries.offsets import BMonthEnd
import pymssql #  using this on mac not windows machine
import datetime as dt


def login_salesforce():
    '''

    :return: connection to SalesForce backup database using pymssql (mssql)
    '''
    server = "10.0.123.212"
    user = "dbamp"
    password = "ua4yCAnolhxV"
    conn = pymssql.connect(server, user, password, "salesforce backups")
    return conn

def get_column_headers(cursor):
    '''

    :param cursor: mySQL cursor with query that has been executed
    :return: column headers based on query
    '''
    col_headers = list(cursor.description)
    col_headers = [x[0] for x in col_headers]
    return col_headers

def login_beatbox():
    # login to SF via beatbox
    # SF user/pass
    sf_user = 'john.angerami@collibra.com'
    sf_token = 'TFAB49jVswWVquu75y9nAklhl'
    sf_pass = 'Fiske!418'
    sf_pass_token = sf_pass + sf_token

    # instantiate object & login
    sf = beatbox._tPartnerNS
    svc = beatbox.PythonClient()
    svc.login(sf_user, sf_pass_token)
    dg = svc.describeGlobal()
    return svc

def query_data_bb(svc, query_string):
    """
    Function using BeatBox to query data and return results for any query

    :param svc: beatbox connection
    :param query_string: query string
    :return: list with data
    """
    record_list = []

    query_res = svc.query(query_string)
    record_list += query_res['records']

    # if need to get more records
    while not query_res.done:
        print " ******** FETCHING MORE DATAS ********"
        query_res = svc.queryMore(query_res.queryLocator)
        record_list += query_res
    # df = pd.DataFrame(data=record_list)
    # df.columns = df.columns.map(lambda x: x.replace("__c", "").replace("_", ""))

    return record_list



def get_object_fields(svc, sf_object):
    '''
    function to get SalesForce object fields
    :param svc: beatbox connection
    :param sf_object: object to get description
    :return: list of column names

    ex: obj_desc = svc.describeSObjects("Conversion_Rate__c")
    '''

    obj_desc = svc.describeSObjects(sf_object)[0]
    names = [name for name in obj_desc.fields]
    return names


# query for exchange rates
def get_exchange_df(connection):
    cursor = connection.cursor()
    exchangeRates = "select dc.ConversionRate, dc.IsoCode, dc.StartDate, dc.NextStartDate "
    exchangeRates += "from dbo.DatedConversionRate dc"
    cursor.execute(exchangeRates)
    rates_res = cursor.fetchall()
    # put in DataFrame
    rates = pd.DataFrame(data=rates_res, columns=get_column_headers(cursor))
    return rates


def get_opportunity_df(connection):
    '''
    Function used to query opportunity data for opportunity snapshots
    Query all new & existing opportunities, join with User table
    :param connection: pymysql open connection to SalesForce
    :return: DataFrame with opportunity & owner data
    '''
    q = "SELECT o.Services_Total__c, o.CreatedDate as Created_Date__c, Maintenance_ARR_Delta__c, o.ARR_Delta__c, "
    q += "u.Name as Opportunity_Owner__c, Account_Industry__c as Industry__c, a.Name as Account_Name__c, "
    q += "o.Id as Opportunity_ID__c, o.Cloud_Total__c, o.Probability as Probability__c, o.CurrencyIsoCode, "
    q += "o.Subscription_Total__c, o.Maintenance_Total__c, o.StageName as Stage__c, o.Amount as Amount__c, "
    q += "NextStep Next_Step__c, o.Name as Opportunity_Name__c, "
    q += "Subscription_ARR_Delta__c, Cloud_ARR_Delta__c, o.Type as Type__c, "
    q += "o.CloseDate as Close_Date__c, o.Previous_Stage__c as Prev_Stage__c, o.Region__c, "
    q += "u1.Name as Created_By__c, License_Total__c, o.Date_Opp_Moved_40__c  "
    q += "from dbo.Opportunity o "
    q += "join dbo.[User] u "
    q += "on o.OwnerId = u.Id "
    q += "join dbo.Account a "
    q += "on o.AccountId = a.Id "
    q += "join dbo.[User] u1 "
    q += "on o.CreatedById = u1.Id "
    q += "where o.Type in ('New Business','Existing Business');"
    cursor = connection.cursor()
    # Execute query to obtain opportunity data
    cursor.execute(q)
    qr_res = cursor.fetchall()
    # get column headers
    headers = get_column_headers(cursor)
    # put in DataFrame
    df = pd.DataFrame(data=qr_res, columns=headers)
    return df


## TODO: merge based on startdate and enddate
def lookup_rates(cur, close_date, rates_df, rates_dict):
    from datetime import datetime as dt
    '''
    Function to lookup exchange rates, based on currency code & close date
    Need to pass in latest exchange rates (rates_df) and current rates (rates_dict)
        If opportunity close date is in current month or in the future, use current rates
        Else: use past currency rates

    :param cur: currency, string ('USD','GBP')
    :param close_date: datetime.date. Close date of opportunity
    :param rates_df: DataFrame with exchange rates (past rates)
    :param rates_dict: dictionary with today's rates {IsoCode: exchange rate}
    :return: exchange rate
    '''
    today = dt.today().date()
    offset = BMonthEnd()
    first_day = offset.rollback(today).date()
    last_day = offset.rollforward(today).date()

    # EUR is always 1.0
    if cur == 'EUR':
        return 1.0
    # if close date is in future: use current rates
    elif close_date > today:
        return float(rates_dict['ConversionRate'][cur])
    # if close date is in past:
        # find rate where the date is in between startdate & next start date
        # or use present rate
    elif close_date < today:
        rate = rates_df[(rates_df.IsoCode ==cur)&(rates_df.NextStartDate > close_date) & (rates_df.StartDate <= close_date)].ConversionRate
        if len(rate) ==0:
            return float(rates_dict['ConversionRate'][cur])
        else:
            return float(rate)

def opp_age(stage, createdDate, closeDate):
    '''
    Function to calculate the age of an opportunity
        If opp is closed, age = closed date - created date
        Else: age = today - created date
    :param stage: str, StageName
    :param createdDate: datetime
    :param closeDate: datetime
    :return: integer
    '''
    closed = 'Closed' in stage
    if closed:
        age = closeDate - createdDate
    else:
        age = dt.today().date() - createdDate
    return age.days




def get_SF_data(query_string, connection):
    cursor = connection.cursor()
    # Execute query to obtain opportunity data
    cursor.execute(query_string)
    qr_results = cursor.fetchall()
    # get column headers
    headers = get_column_headers(cursor)
    # put in DataFrame
    df = pd.DataFrame(data=qr_results, columns=headers)
    return df


