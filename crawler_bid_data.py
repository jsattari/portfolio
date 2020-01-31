import time
import datetime
import pandas as pd  # LifeOfDesiigner
from io import BytesIO, StringIO  # IOs
from google.cloud import storage  # gcs
from google.oauth2 import service_account  # gcs
import os  # use commands
import re  # regex
from dotenv import load_dotenv  # get env variables
import snowflake.connector  # query


def main():

    # To run locally, comment out "load_dotenv" and manually
    # enter the necessary paths/values in below variables
    load_dotenv("{PATH}")
    # load_dotenv("/Users/john.sattari/Desktop/"+".env")
    service_account_path = os.getenv("SERV_ACCT_KEY_PROD")
    scope = os.getenv("SCOPE")
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv("SNOWFLAKE_PW")
    snowflake_region = os.getenv("SNOWFLAKE_ACCT")

    # snowflake connection
    ctx = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_region
    )

    # initializing the query
    query = ctx.cursor()

    # query for top site list
    query_data = query.execute("""
    {QUERY}
    """)

    # fetches query data and putting into dataframe
    sites_data = pd.DataFrame(query_data.fetchall())

    # close snowflake connection
    query.close()

    # column names
    sites_data.columns = ["sf_account_name",
                          "sf_account_id", "domain", "spend", "rank_by_spend"]

    # apply https to start of domain names for
    sites_data["domain"] = sites_data["domain"].apply(lambda x: "https://" + x)

    # list of sites to check, will be updated to a query later
    urls = sites_data["domain"].tolist()

    # define function for getting bidder info
    def getBidderInfo(x):
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from xvfbwrapper import Xvfb  # enables us to create a fake screen so JS will render
        chrome_driver_path = "{PATH}"
        # set Xvfb window size.. HYUUUUUGE
        window = Xvfb(width=1920, height=945)
        window.start()  # open Xvfb window
        options = webdriver.ChromeOptions()  # setting up chromedriver options
        # Use default profile from chrome path
        options.add_argument("--profile-directory=Default")
        options.add_argument("--window-size=1440,728")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36")
        # setting up chromedriver
        cow = webdriver.Chrome(
            executable_path=chrome_driver_path, options=options)
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
        }''')  # JS script to be executed on page
        site = x  # updating url to new variable
        try:
            cow.get(site)
            # wait this amount of seconds for site to load, then execute script
            time.sleep(10)
            # execute JS script on current page
            result = cow.execute_script(var1)
            print("site checked: " + site)
            return result  # result of JS script store as variable
        except Exception:
            pass  # move on if JS script fails to execute or site can't be reached
        finally:
            cow.close()  # close browser
            cow.quit()  # close driver
            window.stop()  # close Xvfb display window

    # empty list where results will be added after formatting
    all_prebid = []

    # loop that grabs and organizes bidder info. Inserts into all_prebid list
    for url in urls:
        try:
            dog = getBidderInfo(url)  # using our function
            for row in dog:
                row.update(domain=url)  # insert in domain column
                row.update(crawl_timestamp=time.strftime(
                    "%Y-%m-%d %T"))  # insert crawl timestamp
        except Exception:
            # skip if try results in fail (means url did not have ads or JS script returned null)
            pass
        else:
            all_prebid.extend(dog)  # insert result into list

    all_prebid_df = pd.DataFrame(all_prebid)  # create dataframe from list

    # create list of colum names
    column_list = ('ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode', 'cpm', 'creativeId', 'currency',
                   'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm', 'originalCurrency', 'requestId', 'requestTimestamp', 'responseTimestamp',
                   'size', 'source', 'statusMessage', 'timeToRespond', 'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon',
                   'rubiconTargeting', 'sendStandardTargeting', 'dealId')

    # confirm that all_prebid_df col names exist in column list.
    for col in column_list:
        if col not in all_prebid_df.columns:
            # if col does not exist, insert new col with blanks
            all_prebid_df.insert(0, col, "")

    # put cols in proper order
    all_prebid_df = all_prebid_df[['ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode', 'cpm', 'creativeId', 'currency',
                                   'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm', 'originalCurrency', 'requestId', 'requestTimestamp', 'responseTimestamp',
                                   'size', 'source', 'statusMessage', 'timeToRespond', 'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon',
                                   'rubiconTargeting', 'sendStandardTargeting', 'dealId']]

    # formatting cols timestamps
    all_prebid_df["requestTimestamp"] = all_prebid_df["requestTimestamp"].apply(
        lambda x: datetime.datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d %T"))
    all_prebid_df["responseTimestamp"] = all_prebid_df["responseTimestamp"].apply(
        lambda x: datetime.datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d %T"))

    # fill in blank cells in dataframe
    all_prebid_df["meta"] = all_prebid_df["meta"].fillna("")

    # round cpms for readability
    all_prebid_df["cpm"] = all_prebid_df["cpm"].round(4)

    # create dataframe of adID and meta cols
    df_changed = pd.DataFrame(all_prebid_df, columns=['adId', 'meta'])

    # format meta col to something usable
    df_changed['meta'] = df_changed['meta'].astype(str).apply(lambda x: x.replace("'", "")).apply(lambda x: x.replace("{", "")).apply(lambda x:
                                                                                                                                      x.replace("}", "")).apply(lambda x: x.replace(" ", "")).apply(lambda x: x.replace(",", " "))

    # create new cols to fit new values
    df_changed['AdvertiserID'] = ""
    df_changed['BrandID'] = ""
    df_changed['BrandName'] = ""
    df_changed['DSPID'] = ""
    df_changed['NetworkID'] = ""

    # function to find dynamically find IDs within meta col
    def regex_id(col_name, string):
        match = re.compile(col_name+':'+r'[0-9]+')
        found = match.findall(string)
        return found

    # function to find names within meta col
    def regex_name(col_name, string):
        match = re.compile(col_name+':'+r'\s*\S+\S?')
        found = match.findall(string)
        return found

    # function to find dsp IDs
    def regex_dsp(col_name, string):
        match = re.compile(col_name+':'+r'\w+?(.*)')
        found = match.findall(string)
        return found

    # applying functions to each column
    df_changed['AdvertiserID'] = df_changed['meta'].apply(
        lambda x: regex_id('advertiserId', x) if 'advertiserId' in x else "")
    df_changed['BrandID'] = df_changed['meta'].apply(
        lambda x: regex_id('brandId', x) if 'brandId' in x else "")
    df_changed['BrandName'] = df_changed['meta'].apply(
        lambda x: regex_name('brandName', x) if 'brandName' in x else "")
    df_changed['DSPID'] = df_changed['meta'].apply(
        lambda x: regex_dsp('dspid', x) if 'dspid' in x else "")
    df_changed['NetworkID'] = df_changed['meta'].apply(
        lambda x: regex_id('networkId', x) if 'networkId' in x else "")

    # function to remove characters
    def formatter(string):
        bad_list = r"\[|\]|\:|\'"
        steak = re.sub(bad_list, "", string)
        return steak

    # apply above function to each col
    df_changed['AdvertiserID'] = df_changed['AdvertiserID'].astype(str).apply(
        lambda x: formatter(x)).apply(lambda x: x.replace('advertiserId', ""))
    df_changed['BrandID'] = df_changed['BrandID'].astype(str).apply(
        lambda x: formatter(x)).apply(lambda x: x.replace('brandId', ""))
    df_changed['BrandName'] = df_changed['BrandName'].astype(str).apply(
        lambda x: formatter(x)).apply(lambda x: x.replace('brandName', ""))
    df_changed['DSPID'] = df_changed['DSPID'].astype(str).apply(
        lambda x: formatter(x)).apply(lambda x: x.replace('dspid', ""))
    df_changed['NetworkID'] = df_changed['NetworkID'].astype(str).apply(
        lambda x: formatter(x)).apply(lambda x: x.replace('networkId', ""))

    # merge df_change to existing data
    df_merged = pd.merge(all_prebid_df, df_changed[['AdvertiserID', 'BrandID', 'BrandName', 'DSPID', 'NetworkID']],
                         left_on=all_prebid_df['adId'], right_on=df_changed['adId'], how='left')

    # select columns to keep from df_merged
    df_merged = df_merged[['ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode', 'cpm', 'creativeId', 'currency',
                           'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm', 'originalCurrency', 'requestId', 'requestTimestamp', 'responseTimestamp',
                           'size', 'source', 'statusMessage', 'timeToRespond', 'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon',
                           'rubiconTargeting', 'sendStandardTargeting', 'dealId', 'AdvertiserID', 'BrandID', 'BrandName', 'DSPID', 'NetworkID']]

    # -------GCS AUTH-------

    credentials = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    project_id = "{PROJECT_ID}"
    client = storage.Client(project=project_id, credentials=credentials)
    bucket = client.get_bucket("{BUCKET}")

    # ----PULL FROM GCS----
    blob = bucket.blob("bid_response_crawl_VM.csv")  # filename in gcs bucket
    # download as string to avoid formatting issues!
    old_data = BytesIO(blob.download_as_string())
    # pretend it is a csv and read it into a dataframe
    all_prebid_df_exist = pd.read_csv(old_data, sep=",")

    # set column order
    all_prebid_df_exist = all_prebid_df_exist[['ad', 'adId', 'adUnitCode', 'adserverTargeting', 'appnexus', 'auctionId', 'bidder', 'bidderCode',
                                               'cpm', 'creativeId', 'currency', 'height', 'mediaType', 'meta', 'netRevenue', 'originalCpm', 'originalCurrency',
                                               'requestId', 'requestTimestamp', 'responseTimestamp', 'size', 'source', 'statusMessage', 'timeToRespond',
                                               'ttl', 'width', 'domain', 'crawl_timestamp', 'placementId', 'status', 'ts', 'rubicon', 'rubiconTargeting',
                                               'sendStandardTargeting', 'dealId', 'AdvertiserID', 'BrandID', 'BrandName', 'DSPID', 'NetworkID']]

    # concat new data to existing data
    all_prebid_up = pd.concat(
        (all_prebid_df_exist, df_merged), axis=0, sort=False)

    # ----PUSH TO GCS----

    # create filestream using StringIO()
    output = StringIO()

    # dump to csv format
    all_prebid_up.to_csv(output, sep=",", index=False)

    # apply .seek(0) to move cursor to start of string buffer
    output.seek(0)

    # name of file once uploaded to GCP
    blob = bucket.blob("bid_response_crawl_VM.csv")

    # upload as string, use output.getvalue() to read values of csv into buffer
    blob.upload_from_string(output.getvalue(), content_type="text/csv")
    print('File {} uploaded to {}.'.format(blob, bucket))

    # close buffer to release memory
    output.close()


if __name__ == "__main__":
    main()
# END
