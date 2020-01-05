import time
import datetime
import pandas as pd
from io import BytesIO, StringIO
from google.cloud import storage
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import snowflake.connector

#To run locally, comment out "load_dotenv" and manually enter the necessary paths/values in below variables
load_dotenv("{PATH}")
service_account_path = os.getenv("SERV_ACCT_KEY_PROD")
scope = os.getenv("SCOPE")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PW")
snowflake_region = os.getenv("SNOWFLAKE_ACCT")

#snowflake connection
ctx = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_region
    )

#initializing the query
query = ctx.cursor()

#query for top site list
query_data = query.execute("""
SELECT * FROM
(SELECT xyz.account_name AS sf_account_name, 
xyz.account_id, 
CASE WHEN a.domain like 'www.%' THEN regexp_replace(a.domain, 'www.', '') ELSE a.domain END, --a.domain,
Sum(CASE WHEN a.is_mkt THEN a.tot_usd_a_spend ELSE 0 end) AS spend,
ROW_NUMBER() OVER (PARTITION BY xyz.account_id ORDER BY spend DESC) AS RANK_BY_SPEND
FROM   mstr_datamart.ox_transaction_sum_daily_fact AS a 
LEFT JOIN mstr_datamart.dim_sites_to_owners AS xyz 
ON a.site_nk = xyz.site_nk 
WHERE a.utc_rollup_date >= current_date - 30
AND a.utc_rollup_date < current_date 
AND a.domain IS NOT NULL
GROUP  BY 1, 2, 3
HAVING spend > 0)
WHERE RANK_BY_SPEND = 1 -- change to < x to show Top X domains 
ORDER BY SPEND DESC
LIMIT 100;
""")

#fetches query data and putting into dataframe
sites_data = pd.DataFrame(query_data.fetchall())

#close snowflake connection
query.close()

#column names
sites_data.columns = ["sf_account_name", "sf_account_id", "domain", "spend", "rank_by_spend"]

#apply https to start of domain names for 
sites_data["domain"] = sites_data["domain"].apply(lambda x: "https://"+ x)

#list of sites to check, will be updated to a query later
urls = sites_data["domain"].tolist()

#define function for getting bidder info
def getBidderInfo(x):
    import time
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from xvfbwrapper import Xvfb #enables us to create a fake screen so JS will render
    chrome_driver_path = "/home/john.sattari/dump/pointless/chromedriver"
    window = Xvfb(width=1920, height=945) #set Xvfb window size.. HYUUUUUGE
    window.start() #open Xvfb window
    options = webdriver.ChromeOptions() #setting up chromedriver options
    options.add_argument("--profile-directory=Default"); #Use default profile from chrome path
    options.add_argument("--window-size=1440,728");
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36");
    cow = webdriver.Chrome(executable_path=chrome_driver_path, options=options); #setting up chromedriver
    var1 = ('''\
    var dfpDivs = googletag.pubads().getSlots().map(slot => slot.getSlotElementId());
    var DFPKV = [];
    window.googletag.pubads().getSlots().forEach(function(slot) {
        var tmap = slot.getTargetingMap();
        var name = slot.getAdUnitPath();
        var div = slot.getSlotElementId();
        //console.log(tmap)
        var tmapArray = Object.entries(tmap)
        for (x in tmapArray){
            //console.log(tmapArray[x][0])
        }
        DFPKV.push({div: div, name: name, targeting: tmap});
    })
    //console.log(DFPKV)
    if(window.pbjs){
        var prebidBids = []
        var prebidBids2 = []
        var prebidBids3 = []
        dfpDivs.forEach(slotCode => {
            //console.log(slotCode) 
            if(pbjs.getBidResponsesForAdUnitCode(slotCode).bids.length > 0){
              prebidBids[slotCode] = pbjs.getBidResponsesForAdUnitCode(slotCode)
              prebidBids2.push(pbjs.getBidResponsesForAdUnitCode(slotCode).bids)
            }
        })   
    //     console.log(prebidBids)
    //     console.log(prebidBids2)
        prebidBids3 = [].concat.apply([],prebidBids2);
       return JSON.parse(JSON.stringify(prebidBids3))
    }''') #JS script to be executed on page
    site = x #updating url to new variable
    try:
        cow.get(site)
        time.sleep(10) #wait this amount of seconds for site to load, then execute script
        result = cow.execute_script(var1) #execute JS script on current page
        print("site checked: " + site)
        return result #result of JS script store as variable
    except Exception:
        pass #move on if JS script fails to execute or site can't be reached
    finally:
        cow.close() #close browser
        cow.quit() #close driver
        window.stop() #close Xvfb display window

#empty list where results will be added after formatting
all_prebid = []

#loop that grabs and organizes bidder info. Inserts into all_prebid list
for url in urls:
    try:
        dog = getBidderInfo(url) #using our function
        for row in dog:
            row.update(domain = url) #insert in domain column
            row.update(crawl_timestamp = time.strftime("%Y-%m-%d %T")) #insert crawl timestamp
    except Exception:
        pass #skip if try results in fail (means url did not have ads or JS script returned null)
    else:
        all_prebid.extend(dog) #insert result into list

all_prebid_df = pd.DataFrame(all_prebid) #create dataframe from list

#create list of colum names
column_list = ('ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode', 'cpm', 'creativeId', 'currency',
               'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm','originalCurrency', 'requestId', 'requestTimestamp', 'responseTimestamp',
               'size', 'source', 'statusMessage', 'timeToRespond', 'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon',
               'rubiconTargeting', 'sendStandardTargeting', 'dealId')

#confirm that all_prebid_df col names exist in column list.
for col in column_list:
    if col not in all_prebid_df.columns:
        all_prebid_df.insert(0,col,"") #if col does not exist, insert new col with blanks

#put cols in proper order
all_prebid_df = all_prebid_df[['ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode', 'cpm', 'creativeId', 'currency',
                               'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm','originalCurrency', 'requestId', 'requestTimestamp', 'responseTimestamp',
                               'size', 'source', 'statusMessage', 'timeToRespond', 'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon',
                               'rubiconTargeting', 'sendStandardTargeting', 'dealId']]

#formatting cols timestamps
all_prebid_df["requestTimestamp"] = all_prebid_df["requestTimestamp"].apply(lambda x:datetime.datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d %T"))
all_prebid_df["responseTimestamp"] = all_prebid_df["responseTimestamp"].apply(lambda x:datetime.datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d %T"))

#fill in blank cells in dataframe
all_prebid_df["meta"] = all_prebid_df["meta"].fillna("")

#round cpms for readability
all_prebid_df["cpm"] = all_prebid_df["cpm"].round(4)

#-------GCS AUTH-------

credentials = service_account.Credentials.from_service_account_file(service_account_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
project_id = "{PROJECT_NAME}"
client = storage.Client(project=project_id, credentials=credentials)
bucket = client.get_bucket("{BUCKET_NAME}")

#----PULL FROM GCS----
blob = bucket.blob("{FILENAME}") #filename in gcs bucket
old_data = BytesIO(blob.download_as_string()) #download as string to avoid formatting issues!
all_prebid_df_exist = pd.read_csv(old_data, sep=",") #pretend it is a csv and read it into a dataframe

#set column order
all_prebid_df_exist = all_prebid_df_exist[['ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode', 'cpm', 'creativeId', 'currency',
                               'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm','originalCurrency', 'requestId', 'requestTimestamp', 'responseTimestamp',
                               'size', 'source', 'statusMessage', 'timeToRespond', 'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon',
                               'rubiconTargeting', 'sendStandardTargeting', 'dealId']]

#concat new data to existing data
all_prebid_up = pd.concat((all_prebid_df_exist, all_prebid_df), axis=0, sort=False)

#----PUSH TO GCS----

#create filestream using StringIO()
output = StringIO()

#dump to csv format
all_prebid_up.to_csv(output, sep = ",", index=False)

#apply .seek(0) to move cursor to start of string buffer 
output.seek(0)

#name of file once uploaded to GCP
blob = bucket.blob("{FILENAME}")

#upload as string, use output.getvalue() to read values of csv into buffer
blob.upload_from_string(output.getvalue(), content_type="text/csv")
print('File {} uploaded to {}.'.format(blob, bucket))

#close buffer to release memory
output.close()

#END