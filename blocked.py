#general modules
from michaels_secret_stuff import *
import pandas as pd
#time modules
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
#storage auth
from google.cloud import storage
from google.oauth2 import service_account
#file buffer
from io import BytesIO
#email modules
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders
import smtplib


#-----START------
start = datetime.now()
print(start)

#last month and year
last_month = datetime.now() - relativedelta(months=1)
good_date = format(last_month, '%B_%Y')

#today's date, saving for later
dt_local = time.strftime("%Y-%m-%d")

#initializing the query
query = ctx.cursor()

#query for data
sql1 = """
{QUERY}
"""

#execute query
query.execute(sql1)

#fetches query data and putting into dataframe
dump = pd.DataFrame(query.fetchall())
#dump = pd.read_csv('/Users/john.sattari/Desktop/blocked_adv_monthly.csv')

#naming columns
dump.columns = ['publisher_account', 'pub_name', 'site', 'brand_id', 'brand_name', 'brand_industry', 'discard_reason',
                'currency_code', 'instance', 'supply_am', 'supply_bd', 'sf_account', 'base_rev', 'blocked_bids', 'WJXBFS3', 'WJXBFS4']

#adding new columns
dump.insert(16, 'Incremental Revenue (Estimated)', '')
dump.insert(17, 'Revenue Lift (Estimated)', '')

#tracking script execution time
print(datetime.now()- start)

#function to help with 0 values in base_rev col
def magic_div(dump):
    if dump['base_rev'] == 0:
        x = 0
    else:
        x = dump['Incremental Revenue (Estimated)'] / dump['base_rev']
    return x

#calculations for new columns
dump['Incremental Revenue (Estimated)'] = dump['{COL_NAME}'] + dump['{COL_NAME}']
dump['Revenue Lift (Estimated)'] = dump.apply(magic_div, axis = 1)

#sorting by revenue column
dump = dump.sort_values('Incremental Revenue (Estimated)', ascending = False)

#picking which columns to keep
dump = dump[['publisher_account', 'pub_name', 'site', 'brand_id', 'brand_name', 'brand_industry', 'discard_reason',
             'currency_code', 'instance', 'supply_am', 'supply_bd', 'sf_account', 'blocked_bids', 'Incremental Revenue (Estimated)', 'Revenue Lift (Estimated)']]

#tracking duration of script
print(datetime.now() - start)

#data stream into excel file
output = BytesIO()
writer = pd.ExcelWriter(output, engine='xlsxwriter', options={'strings_to_numbers': True})
dump.to_excel(writer, sheet_name=good_date, index=False, header = False)

#workbook objects
workbook = writer.book
worksheet = writer.sheets[good_date]

#format variables
numbers = workbook.add_format({'num_format': '#,###'})
percents = workbook.add_format({'num_format': '0%'})
dollas = workbook.add_format({'num_format': '$#,##0.00'})
bold = workbook.add_format({'bold': True})

# Add a header format.
header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
    'valign': 'top',
    'fg_color': '#FDFF00',
    'border': 0,
    'font_size': 12})

# Write the column headers with the defined format.
for col_num, value in enumerate(dump.columns.values):
    worksheet.write(0, col_num, value, header_format)

worksheet.set_column('M:M', None, numbers)
worksheet.set_column('N:N', None, dollas)
worksheet.set_column('O:O', None, percents)
worksheet.set_row(0, None, bold)
worksheet.set_column(0, 15, 30)

writer.save()

output.seek(0)

#-------GCS AUTH-------
key_path = "{PATH}"
credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
project_id = "{PROJECT}"
client = storage.Client(project=project_id, credentials=credentials)
bucket = client.get_bucket("{BUCKET}")

#name of file once uploaded to GCP
blob = bucket.blob("blocked_advertisers_monthly.xlsx")

#upload as file so you can use buffer to stream data into gcs
blob.upload_from_file(output, content_type="application/octet-stream")
print('File {} uploaded to {}.'.format(blob, bucket))

#---------CREATE/SEND EMAIL-----------------
contacts = ["{REDACTED}"]

# create message object instance
msg = MIMEMultipart()

# setup the parameters of the message
password = emailpw
msg['From'] = emailu
msg['To'] = ", ".join(contacts)
msg['Subject'] = "Blocked Advertisers Monthly Report // " + good_date

# html email body (need to "attach" msg body)... omfg
body ='''\
<html>
Hola,
<br><br>
Please use the below link to download your report:
<br><br>
<a href="{LINK_TO_CLOUD}">
blocked_advertisers_monthly.xlsx</a>
<br><br>
Thank you!
</html>
'''

#attach HTML formatting
msg.attach(MIMEText(body,"html")) 

#creating email server
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(emailu, emailpw)

# send the message via the smtp server
#server.sendmail(msg['From'], msg['To'], msg.as_string())
server.send_message(msg)

#end server session
server.quit()
output.close()
print("successfully sent email to: " + msg['To'])

#END