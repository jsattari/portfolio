from michaels_secret_stuff import *
import pandas as pd
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#from email.MIMEImage import MIMEImage
from email.mime.application import MIMEApplication
import smtplib
import time
from google.cloud import storage
from google.oauth2 import service_account



#today's date, saving for later
dt_local = time.strftime("%m-%d-%Y")

#initializing the query
query = ctx.cursor()

#query for brand data (date, brand, spend)
query.execute("""
{QUERY}
""")

#fetches query data and putting into dataframe
d_brand = pd.DataFrame(query.fetchall())

#close snowflake connection
query.close()

#adding column names to d_metrics
d_brand.columns = ["date", "brand", "spend","revenue"]
d_brand = d_brand.sort_values(by="spend",ascending=False)

#initializing the query
query = ctx.cursor()

#query for transaction data (bid req, bids, imps, spend, revenue)
query.execute("""
{QUERY}
""")

#fetches query data and putting into dataframe
d_metrics = pd.DataFrame(query.fetchall())

#close snowflake connection
query.close()

#adding column names to d_metrics
d_metrics.columns = ["int_deal_id","ext_deal_id","date","bid_req","bids","impressions","spend","revenue"]

#sorting d_metrics by date value... nitpicky (sorry)
d_metrics = d_metrics.sort_values(by="date",ascending=False)

#Filter by max date of d_metrics (should be yesterday's date) and then creating new dataframe with yesterdays data
yesterday = d_metrics["date"].max()
d_metrics_y = d_metrics[d_metrics["date"] == yesterday]
d_metrics_y.columns = ["int_deal_id_y","ext_deal_id_y","date_y","bid_req_y","bids_y","impressions_y","spend_y","revenue_y"]

#Filter by min date of d_metrics (should be 7 days prior to yesterday) and then creating new dataframe with last weeks data
last_week = d_metrics["date"].min()
d_metrics_lw = d_metrics[d_metrics["date"] == last_week]
d_metrics_lw.columns = ["int_deal_id_lw","ext_deal_id_lw","date_lw","bid_req_lw","bids_lw","impressions_lw","spend_lw","revenue_lw"]

#initializing the query
query = ctx.cursor()

#query for deal dimensions
query.execute("""
{QUERY}
""")

#fetches query data and putting into dataframe
d_deals = pd.DataFrame(query.fetchall())

#close snowflake connection
query.close()

#adding column names to d_metrics
d_deals.columns = ["instance","deal_name","int_deal_id","ext_deal_id","status","start_date","end_date","price","package_id","deleted"]

#initializing the query
query = ctx.cursor()

#query for sf opportunity info
query.execute("""
{QUERY}
""")

#fetches query data and putting into dataframe
d_sf = pd.DataFrame(query.fetchall())

#close snowflake connection
query.close()

#adding column names to d_sf
d_sf.columns = ["opp_link","opp_name","opp_owner","pd_type","ext_deal_id","created_date"]
d_sf = d_sf.drop_duplicates(keep=False)

#begin formatting - scrub vs sf opportinity info (deals should only be included if they have an opp in SF)
deal_list = pd.merge(d_deals,d_sf[["opp_name","opp_owner"]],
                   left_on=d_deals["ext_deal_id"], right_on=d_sf["ext_deal_id"],how="left")

#arrage columns
deal_list = deal_list[["opp_name","opp_owner","instance","deal_name","int_deal_id","ext_deal_id","status","start_date",
                       "end_date","price","package_id","deleted"]]

#removing deals that do not have an opportunity in SF
deal_list2 = deal_list[deal_list.opp_name.notnull()]

#merging deal_list with yesterdays data to create new dataframe (df)
df = pd.merge(deal_list2, d_metrics_y[["int_deal_id_y","ext_deal_id_y","date_y","bid_req_y","bids_y","impressions_y","spend_y"]],
              left_on="int_deal_id", right_on="int_deal_id_y", how="left")

#merging last weeks data with df to create new dataframe
df_all = pd.merge(df, d_metrics_lw[["int_deal_id_lw","ext_deal_id_lw","date_lw","bid_req_lw","bids_lw","impressions_lw","spend_lw"]],
                  left_on="int_deal_id", right_on="int_deal_id_lw", how="left")

df_all = df_all[["opp_name","opp_owner","instance","deal_name","int_deal_id","ext_deal_id","status","start_date",
                       "end_date","price","package_id","deleted","bid_req_y","bids_y","impressions_y","spend_y","bid_req_lw","bids_lw","impressions_lw","spend_lw"]]

#reformatting data types by column
df_all["deleted"] = df_all["deleted"].astype(int)
df_all["bid_req_y"] =df_all["bid_req_y"].fillna(0).astype(int)
df_all["bids_y"] = df_all["bids_y"].fillna(0).astype(int)
df_all["impressions_y"] = df_all["impressions_y"].fillna(0).astype(int)
df_all["spend_y"] = df_all["spend_y"].fillna(0).astype(float)
df_all["bid_req_lw"] =df_all["bid_req_lw"].fillna(0).astype(int)
df_all["bids_lw"] = df_all["bids_lw"].fillna(0).astype(int)
df_all["impressions_lw"] = df_all["impressions_lw"].fillna(0).astype(int)
df_all["spend_lw"] = df_all["spend_lw"].fillna(0).astype(float)

#add general reason column
df_all.insert(16,"General_reason","")

#dhc logic to determine reason of issues based on data 
def dhc_logic(df_all):
    if df_all["deleted"] == 1:
        x = "Deal deleted"
    elif df_all["status"] == "Expired":
        x = "Deal expired"
    elif df_all["status"] == "Paused":
        x = "Deal paused"
    elif df_all["bid_req_y"] == 0:
        x = "No bid requests"
    elif df_all["bid_req_y"] < 5000 and df_all["bids_y"] == 0:
        x = "Low bid requests"
    elif df_all["bid_req_y"] >= 5000 and df_all["bids_y"] == 0 and df_all["impressions_y"] == 0:
        x = "No bids, the buyer may not be targeting the deal ID yet"
    elif df_all["bid_req_y"] >= 5000 and df_all["bids_y"] >= 1 and df_all["impressions_y"] == 0:
        x = "Bids, but no impressions"
    elif df_all["bid_req_y"] >= 5000 and df_all["bids_y"] >= 1 | df_all["impressions_y"] >= 1 and df_all["spend_y"] < 10:
        x =  "Active deal, low spend (<$10)"
    else:
        x = "Active deal"
    return x

#applying that sweet function, bro
df_all["General_reason"] = df_all.apply(dhc_logic, axis=1)

#dump df_all into local csv
df_all.to_csv("{PATH}", index=False)
d_metrics.to_csv("{PATH}", index=False)
d_brand.to_csv("{PATH}", index=False)

print("csv files saved successfully")

#-------PUSH TO GCS BUCKET-------
key_path = "{PATH}"

credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)

project_id = '{PROJECT}'

client = storage.Client(project=project_id, credentials=credentials)
bucket = client.get_bucket('{BUCKET}')
blob = bucket.blob('Demand_DHC_dump.csv')
blob.upload_from_filename('{PATH}')
print('File {} uploaded to {}.'.format(blob, bucket))

blob = bucket.blob('Demand_DHC_dump_metrics.csv')
blob.upload_from_filename('{PATH}')
print('File {} uploaded to {}.'.format(blob, bucket))

blob = bucket.blob('Brand_metrics.csv')
blob.upload_from_filename('{PATH}')
print('File {} uploaded to {}.'.format(blob, bucket))


#creating raw_spend_change column
df_all.insert(21,"raw_spend_change","")
df_all["raw_spend_change"] = df_all["spend_y"] - df_all["spend_lw"]
df_all["raw_spend_change"] = df_all["raw_spend_change"].round(2)

#get top 5 gainers
df_gainz = df_all.sort_values(by="raw_spend_change",ascending=False).head()
df_gainz = df_gainz[["instance","deal_name","int_deal_id","ext_deal_id", "status","start_date","raw_spend_change"]]

#get top 5 losers
df_lozers = df_all.sort_values(by="raw_spend_change",ascending=True).head()
df_lozers = df_lozers[["instance","deal_name","int_deal_id","ext_deal_id", "status","start_date","raw_spend_change"]]

#top 5 brand spenders
d_brand_g = d_brand.groupby(["brand"]).agg('sum').reset_index()
d_brand_g["spend"] = d_brand_g["spend"].astype(float).round(2)
brand_5 = d_brand_g.sort_values('spend', ascending=False)
brand_list = brand_5[['brand','spend']]
brand_list = brand_list.head()


#---------CREATE/SEND EMAIL-----------------
contacts = ["{REDACTED}"]

# create message object instance
msg = MIMEMultipart()

# setup the parameters of the message
password = emailpw
msg['From'] = emailu
msg['To'] = ", ".join(contacts)
msg['Subject'] = "Demand Deal Check // " + dt_local

# html email body (need to "attach" msg body)... omfg
#body = "Hola! This is a test message" #plain text formatting, just input desired text between the quotes and change
body = '''\
<html>
<head>
<style type="text/css">
body * {margin: 0; padding: 0;}
div {width: 900px; background-color: #ffffff; align: center; margin: 0; padding: 0}
#snapshot1 {width:800px; font-family: Arial;}
#snapshot2 {width: 800px; font-family: Arial; text-align: center; align: center}
#gainz {color: #ffffff; border:1px solid #000000; background-color: #6a863b; font-family: Arial; font-size: 24pt; align: center}
#loserz {color: #ffffff; border:1px solid #000000; background-color: #af002a; font-family: Arial; font-size: 24pt; align: center}
#brandz {color: #ffffff; border:1px solid #000000; background-color: #feca1d; font-family: Arial; font-size: 24pt; align: center;}
#df {background-color: #ffffff; width: 800px; align: center;}
</style>
</head>
<body>
<p>Hola!<br><br>Please see below for a daily snapshot:<br></p>
<div>
  <table id="snapshot1" width="100%" cellpadding="0" align="center" bgcolor="ffffff">
    <caption><h1 id="gainz" align="center">Biggest Gainers (Yesterday vs LW)</h1></caption>
    <tr>
      <td>
        <td id="df" width="100%" cellpadding="0" align="center" bgcolor="ffffff">'''+df_gainz.to_html(index=False)+'''</td>
    </td>
  </tr>
  </table>
  <table id="snapshot2" width="100%" cellpadding="0" align="center" bgcolor="ffffff">
    <caption><h1 id="loserz" align="center">Biggest Losers (Yesterday vs LW)</h1></caption>
    <tr>
      <td>
        <td id="df" width="100%" cellpadding="0" align="center" bgcolor="ffffff">'''+df_lozers.to_html(index=False)+'''</td>
      </td>
    </tr>
</table>
<table id="snapshot1" width="100%" cellpadding="0" align="center" bgcolor="ffffff">
    <caption><h1 id="brandz" align="center">Top 5 Brands (prev 7 days)</h1></caption>
    <tr>
      <td>
        <td id="df" width="100%" cellpadding="0" align="center" bgcolor="ffffff">'''+brand_list.to_html(index=False)+'''</td>
    </td>
  </tr>
  </table>
</div>
<div>
  <table id="image" width="100%" cellpadding="0" align="center" bgcolor="ffffff">
  </table>
</div>
<p><br>A more in-depth look can be found <a href="https://datastudio.google.com/open/1MyW3GnAbG8m4Jz2aH2aInDUSr_xwEiZL">HERE</a>.<br>
<br><br>Thank you!</p>
</body>
</html>
'''

#attach HTML formatting
msg.attach(MIMEText(body,"html")) 

#attaching csv
file = MIMEApplication(open("{PATH}", encoding="utf-8").read())
file.add_header("Content-Disposition",
                '''attachment; filename="Demand_DHC_dump.csv"''')
msg.attach(file)

#creating email server
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(emailu, emailpw)

# send the message via the smtp server
#server.sendmail(msg['From'], msg['To'], msg.as_string())
server.send_message(msg)

#end server session
server.quit()

print("successfully sent email to: " + msg['To'])

#END
