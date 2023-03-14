import time as time
import warnings as warnings
from datetime import date as date

import numpy as np
import pandas as pd

from config import SQL_Connect as sqlc

warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None

def get_date():
    run_date = date.today()
    run_date = str(run_date)
    run_date = run_date.replace("-",".")
    return run_date


def conditions(df):
    points = 0
    if df["EmCount"] == df["MatchTitle"]: points += 1
    else: points += 0
    if (df["Account_Status__c"] == "Inactive"): points += 1
    else: points += 0 
    if df["Title"] != "N/A": points += 1
    else: points += 0
    if (df["FirstName"] != "Unknown" or df["LastName"] != "Unknown") : points += 1
    else: points += 0 
    if (df["AccountZipCode"] == df["ContactZipCode"]): points += 2
    else: points += 0 
    if df["LastName"] != "N/A": points += 2
    else: points += 0 
    if df["FirstName"] != "N/A": points += 2
    else: points += 0 
    if df["Account_Status__c"] == "Prospect": points += 2
    else: points += 0 
    if df["Account_Status__c"] == "Cancelled": points += 4
    else: points += 0 
    if "?" not in df["ContactName"]: points += 4
    else: points += 0
    if df["contactemaildomain"] == df["accntemaildomain"]: points += 4
    else: points += 0
    if df["contactemaildomain"] == df["accntwebdomain"]: points += 4
    else: points += 0
    if df["EmCount"] == df["MatchPhone"]: points += 4
    else: points += 0
    if df["OppCount"] >= 1: points += 4
    else: points += 0
    if df["OppCount"] >= 5: points += 4
    else: points += 0
    if df["OppCount"] >= 10: points += 4
    else: points += 0
    if df["OppCount"] >= 20: points += 4
    else: points += 0
    if df["OppCount"] >= 50: points += 4
    else: points += 0
    if df["OppCount"] >= 100: points += 4
    else: points += 0
    if df["OppCount"] >= 200: points += 4
    else: points += 0
    if df["OppCount"] >= 300: points += 4
    else: points += 0
    return points

def conditions_phase2 (df):
    points2 = 0
    if df["Account_Status__c"] == "Suspended": points2 += 8
    else: points2 += 0 
    #Phone Scrubber
    if df["ContactPhone"][:10] == df["AccountPhone"][:10]: points2 += 16
    else: points2 += 0
    #Max Created date over email groups
    if df["CreatedDate"] == df["MaxDate"]: points2 += 16
    else: points2 += 0
    if df["Account_Status__c"] == "Active": points2 += 32
    else: points2 += 0 
    #If stripped contact email domain is in Account Name or Vice Versa
    if df["EmDomInAcName"] == True | df["AcNameInEmDom"] == True : points2 += 64
    else: points2 += 0
    #If UpperLetterExtractor of Account name equals contact email domain.
    if df["A_Upper"] == df["contactemaildomain"]: points2 += 64
    else: points2 += 0 
    #Is stripped Account Name in Contact email domain
    if df["A_Name_Match_C_Em_Dom"] == True: points2 += 128
    else: points2 += 0
    if df["Type"] == "Decision Maker": points2 += 256
    else: points2 += 0
    if df["UsingPartnerCommunity"] == "Yes": points2 += 512
    else: points2 += 0  
    return points2

time_connect_start = time.time()

query = "exec StoredProcedure"
try:
    df = sqlc.sql_to_pandas(sqlc.serverMSP,sqlc.db_Analytics,query)
except sqlc.pyodbc.OperationalError:
    print("Connection Failed. Did you connect to the VPN?")
time_connect_end = time.time()
time_connect_delta = (time_connect_end - time_connect_start)
print("Took {:.2f} seconds".format(time_connect_delta))

df.fillna("N/A", inplace=True)

time_clean_start = time.time()
#Create first 3 of name column
df["First3Name"] = df["FirstName"].str[:3]
#Clean Phone Numbers
# df["AccountPhone"] = df["AccountPhone"].str.replace('(\D+)','') 
# df["ContactPhone"] = df["ContactPhone"].str.replace('(\D+)','')
#Clean Email Domain
df["em_dom"] = df["contactemaildomain"].apply(lambda x: x.split(".",1)[0])
df["am_dom"] = df["accntwebdomain"].apply(lambda x: x.split(".",1)[0])
df["ae_dom"] = df["accntemaildomain"].apply(lambda x: x.split(".",1)[0])
#Creates Account Name Upper Letter Extractor
df["A_Upper"] = df["AccountName"].str.findall(r'[A-Z]')
df["A_Upper"] = df["A_Upper"].str.join('')
time_clean_end = time.time()
time_clean_delta = time_clean_end - time_clean_start

print("Data Cleaned. Took {:.2f} seconds".format(time_clean_delta))

time_help_start = time.time()
#Column to match if account name is in contact email domain
df["A_Name_Match_C_Em_Dom"] = df.apply(lambda x: x.AccountName.replace(" ","").lower() in x.contactemaildomain, axis=1, result_type="expand").to_frame()
#Max Date
df["MaxDate"] = df.groupby("email")["CreatedDate"].transform(lambda x :x.max())
df["EmDomInAcName"] = df.apply(lambda x: x.em_dom in x.AccountName.lower(), axis=1)
df["AcNameInEmDom"] = df.apply(lambda x: x.AccountName.lower() in x.em_dom, axis=1)
#COUNT(*) OVER () WINDOW FUNCTION
df["MatchLnFn"] = df.groupby(["email","LastName","FirstName"])["contactid"].transform(lambda x : x.size)
df["MatchLN"] = df.groupby(["email", "LastName"])["contactid"].transform(lambda x :x.size)
df["MatchLn3Fn"] = df.groupby(["email","LastName","First3Name"])["contactid"].transform(lambda x :x.size)
df["MatchTitle"] = df.groupby(["email","Title"])["contactid"].transform(lambda x: x.size)
df["MatchPhone"] = df.groupby(["email","ContactPhone"])["contactid"].transform(lambda x: x.size)
time_help_end = time.time()
time_help_delta =  time_help_end - time_help_start

print("Helper Columns Created. Took {:.2f} seconds.".format(time_help_delta))

time_eval_start = time.time()
dupeEval = [
df["EmCount"].eq(2) & (df["MatchLnFn"].eq(1) & df["MatchLN"].eq(1) & df["MatchLn3Fn"].eq(1)),
df["EmCount"].eq(3) & (df["MatchLnFn"].eq(1) & df["MatchLN"].eq(1) & df["MatchLn3Fn"].eq(1)),
df["EmCount"].gt(df["MatchLnFn"]) & df["EmCount"].gt(df["MatchLn3Fn"]) & df["EmCount"].gt(df["MatchLN"]),
df["MatchLn3Fn"].gt(df["MatchLnFn"]) | df["MatchLn3Fn"].gt(df["MatchLN"]),
df["MatchLN"].gt(df["MatchLnFn"]) | df["MatchLN"].gt(df["MatchLn3Fn"]),
df["EmCount"].eq(df["MatchLn3Fn"])
]

choices = [1,5,25,50,75,100]

df["Dupes"] = np.select(dupeEval, choices, default=0)
# print("Duplicate likeliness evaluated.")


df["points"] = df.apply(conditions, axis = 1)
# print("conditions phase 1: completed")

df["points2"] = df.apply(conditions_phase2, axis=1)
# print("conditions phase 2: completed")

df["points_total"] = df["points"] + df["points2"]

df.sort_values(["EmCount","email","points"], inplace=True)
df["Rank"] = df.groupby("email")["points_total"].rank(ascending=False,method="average").to_frame()


df["eval_result"] = df.groupby("email")["points_total"].transform(lambda x: x.sum()/df["EmCount"])
df["Review_Groups"] = df["email"].map(df.groupby("email").apply(lambda x: x["Rank"].ne(1).all())) #Returns True if all rows in an email group that does not have a Rank of 1
df["Merge_Groups"] = df["email"].map(df.groupby("email").apply(lambda x: x["Rank"].eq(1).any())) #Returns True if any row in an email group has a Rank of 1
master_conditions = [
    df["Rank"].eq(1) & df["eval_result"].ne(df["points_total"]),
    df["Rank"].ne(1) & df["Merge_Groups"].eq(True),
    df["eval_result"].eq(df["points_total"]) | (df["Rank"] % 1).ne(0) | df["Review_Groups"].eq(True)  
]

master_choices = ["Primary","Merge","Review"]
df["MasterPicker"] = np.select(master_conditions,master_choices, default="Merge")
time_eval_end = time.time()
time_eval_delta = time_eval_end - time_eval_start
print("Duplicate Evalution Compelted. Took {:.2f} seconds.".format(time_eval_delta))

time_export_start = time.time()
#Add Researcher Columns
df["Data Load Status"] = ""
df["Data Load Date"] = ""
df["Notes"] = ""
df["Researcher"] = ""
df["Research Status"] = ""
df["Research Date"] = ""
df["Not a Duplicate?"] = ""
df["Master Record?"] = ""
df["Include?"] = ""
df["Missing Master?"] = ""

df.drop(['First3Name','em_dom','am_dom','ae_dom','A_Upper','A_Name_Match_C_Em_Dom',
'MaxDate','EmDomInAcName','AcNameInEmDom','MatchLnFn','MatchLN','MatchLn3Fn','MatchTitle',
'MatchPhone','points','points2','eval_result',"Review_Groups","Merge_Groups"],axis = 1,inplace=True)


df  = df.reindex(columns=['Data Load Status','Data Load Date','Dupes','points_total','Rank','MasterPicker','Notes','Researcher','Research Status','Research Date','Not a Duplicate?',
'Master Record?','Include?','Missing Master?','OppCount','Type','email','contactemaildomain','AccountName','ParentName','UltParentName','accntwebdomain','accntemaildomain','ContactName',
'ContactPhone','AccountPhone','Account_Status__c','Contact_Status__c','CreatedDate','UsingPartnerCommunity','ContactZipCode','AccountNumber','AccountZipCode','UPSameAsAccount','EmCount',
'FirstName','LastName','Title','accountid','ParentID','UltParentID','contactid'])


path = (r"~\Desktop\ContactExports")
df.to_csv(path+"\\"+"OneContactToManyAccounts_Export_{}.csv".format(get_date()),index=False)
time_export_end = time.time()
time_export_delta = time_export_end - time_export_start
print("Export Completed. Took {:.2f} seconds.".format(time_export_delta))

time_total = time_connect_delta + time_clean_delta + time_eval_delta + time_export_delta 
time.sleep(1)
print("Total time: {:.2f} seconds.".format(time_total))