import pandas as pd
from pymongo import MongoClient
from datetime import date,timedelta,datetime

client            = MongoClient('mongodb://marketingUser:dPQrNuvREeJ8X8qbPaUrwysr767Qprwe@139.59.71.134:27017/rapidoProd?')
db                = client['rapidoProd']
ordersCollection  = db["orders"]
usersCollection   = db["users"]


client2           = MongoClient("mongodb://apiUser:MCk#al$hslk2a4hd@35.200.183.21:27017/admin")
db2               = client2['matrix']
sourceusers       = db2["userAttributionNew"]
citywise          = db2["cityWisePerformance"]

city=['Ahmedabad','Aurangabad','Bangalore','Bhopal','Bhubaneswar','Coimbatore','Gurgaon','Hyderabad','India','Indore','Jaipur','Madurai','Mysore','Nagpur','Patna','Trichy','Vijayawada','Vishakapatnam']

yesterday = str(date.today()-timedelta(1))
monthStart = str(date.today().replace(day=1))

dfu=pd.io.json.json_normalize(list(sourceusers.find(
    {"registeredDate":{"$gte":monthStart,"$lte":yesterday},"registeredCity":{"$in":city}},
    {"mobile":1,"source":1,"registeredCity":1,"_id":0})))

mobStr  = dfu['mobile'].astype(str).tolist()
# mobStr       = [str(x) for x in customerMob]

dfo=pd.io.json.json_normalize(list(ordersCollection.find(
    {"orderDate":{"$gte":monthStart,"$lte":yesterday},"customerObj.mobile":{"$in":mobStr},
    "serviceObj.city":{"$in":city}},
    {"customerObj.mobile":1,"status":1,"_id":0,"subTotal":1,"serviceObj.city":1})))
dfo=dfo.rename(columns={"serviceObj.city":'City'})
dfref=dfu.query('source=="referral"')
dfon =dfu.query('source=="online"')
dfoff=dfu.query('source=="offline"')
dlist = [dfon,dfref,dfoff,dfu]
source=["online","referral","offline","overall"]
df_final=pd.DataFrame()
for i in range(len(dlist)):
    df=dlist[i]
    R=df.groupby('registeredCity')['mobile'].count()
    R=R.reset_index()
    R=R.rename(columns={'registeredCity': 'City', 'mobile': 'Registrations'})  
    dfo['mobile']=dfo['customerObj.mobile'].apply(str)
    temp=df.merge(dfo,how='inner',on=['mobile'])
    g=temp.groupby('City')['mobile'].nunique()
    g=g.reset_index()
    g=g.rename(columns={ 'mobile': 'Gross'})
    s=(temp.query('status=="dropped"')).groupby('City', as_index=False).agg({"mobile":"nunique","status":"count","subTotal":"sum"})
    s= s.rename(columns={ 'mobile': 'net','status':'Rides','subTotal':'M0 Revenue'})
    s0=R.merge(g,how='left',on=['City'])
    s1=s0.merge(s,how='left',on=['City'])
    s2=s1.fillna(0)
    s2['gross%']=round((s2['Gross']/s2['Registrations'])*100, 2)    
    s2['net%']  =round((s2['net']/s2['Registrations'])*100 ,2)
    s2['G2N%']  =round((s2['net']/s2['Gross'])*100 ,2) 
    s2['RPC']   =round(s2['Rides']/s2['net'],1) 
    s2['source']=source[i]
    df_final=df_final.append(s2)

df_final['date'] =yesterday
df_final = df_final.fillna('')
# print(df_final.head())
try:
    bulk = citywise.initialize_unordered_bulk_op()

    for index, ids in df_final.iterrows():
        a=bulk.find({ 'date': ids['date'],'city':ids['City'],'source':ids['source']}).upsert().update(
            {
                '$set': {
                    'city' : ids['City'],
                    'registrations' : ids['Registrations'],
                    'gross' : ids['Gross'],
                    'net' : ids['net'],
                    'rides' : ids['Rides'],
                    'M0Revenue':ids['gross%'],
                    'netPercent':ids['net%'],
                    'netToGross': ids['G2N%'], 
                    'RPC'     : ids['RPC'],
                    'source' : ids['source'],
                    'date' : ids['date']
                }
            })
    bulk.execute()
    print("yayyyyyy executed bulk")
except Exception as e:
    # logging.info("createdOn: %s, exception: %s", createdOn, e)
    # pass
    print(e)