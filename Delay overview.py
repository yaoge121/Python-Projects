import pandas as pd
import numpy as np
from datetime import timedelta

#Base as 551
zsd0551_order=pd.read_excel(r"\\GE01A0001\Operations\02 Sales Operations and Planning\02 Planning and Allocation\03 Report and Presentations\POWER BI\Base data and structure\Data\ZSD0551.xlsx")
#zsd0551_order=pd.read_excel("Mapping-test.xlsx", "ZSD0551")
zsd0551_order["SAP ID & Item No"]= zsd0551_order["Sales Order No"].map(str) + "-" + zsd0551_order["Sales Order Item"].map(str)

#merge ATA/ETA log/ETA HQ in one column

order_conditions = [
    (pd.notnull(zsd0551_order["ETA Date(Actual)"])),
    (pd.isnull(zsd0551_order["ETA Date(Actual)"])) & (pd.notnull(zsd0551_order["ETA Date"])),
    (pd.isnull(zsd0551_order["ETA Date(Actual)"])) & (pd.isnull(zsd0551_order["ETA Date"])) & (pd.notnull(zsd0551_order["ETA Date(HQ)"])),
   ]

order_ETA_values1 = ["ATA", "ETA Logistic", "ETA HQ"]
order_ETA_values2 = [
    zsd0551_order["ETA Date(Actual)"].map(str),
    zsd0551_order["ETA Date"].map(str),
    zsd0551_order["ETA Date(HQ)"].map(str),
]

zsd0551_order["SAP ETA category"] = np.select(order_conditions, order_ETA_values1, default=np.nan)
zsd0551_order["SAP ETA on port"] = np.select(order_conditions, order_ETA_values2, default=np.nan)

zsd0551_order["SAP ETA on port"] =pd.to_datetime(zsd0551_order["SAP ETA on port"].map(str), errors='coerce')
zsd0551_order["SAP ETA on port"]=zsd0551_order["SAP ETA on port"].dt.date
zsd0551_order["WH ETA"]= zsd0551_order["SAP ETA on port"] + timedelta(days=10)
zsd0551_order["PO Require Date"] =pd.to_datetime(zsd0551_order["PO Require Date"].map(str), errors='coerce')
zsd0551_order["PO Require Date"]=zsd0551_order["PO Require Date"].dt.date


#ETD Setup
order_conditions2 = [
    (pd.notnull(zsd0551_order["ETD Date(Actual)"])),
    (pd.isnull(zsd0551_order["ETD Date(Actual)"])) & (pd.notnull(zsd0551_order["ETD Date"])),
    (pd.isnull(zsd0551_order["ETD Date(Actual)"])) & (pd.isnull(zsd0551_order["ETD Date"])) & (pd.notnull(zsd0551_order["ETD Date(HQ)"])),
   ]

order_ETA_values2_1 = ["ATD", "ETD Logistic", "ETD HQ"]
order_ETA_values2_2 = [
    zsd0551_order["ETD Date(Actual)"].map(str),
    zsd0551_order["ETD Date"].map(str),
    zsd0551_order["ETD Date(HQ)"].map(str),
]

zsd0551_order["SAP ETD category"] = np.select(order_conditions2, order_ETA_values2_1, default=np.nan)
zsd0551_order["SAP ETD on port"] = np.select(order_conditions2, order_ETA_values2_2, default=np.nan)
zsd0551_order["SAP ETD on port"] =pd.to_datetime(zsd0551_order["SAP ETD on port"].map(str), errors='coerce')
zsd0551_order["SAP ETD on port"]=zsd0551_order["SAP ETD on port"].dt.date

#Datediff set up
datediff_conditions = [
    (zsd0551_order["Inco Terms"]=="CIF"),
    (zsd0551_order["Inco Terms"]=="FOB"),
    (zsd0551_order["Inco Terms"]=="DDP"),
    (zsd0551_order["Inco Terms"]!="FOB") & (zsd0551_order["Inco Terms"]!="CIF") & (zsd0551_order["Inco Terms"]!="DDP")
   ]

datediff_values = [
    (zsd0551_order["SAP ETA on port"]-zsd0551_order["PO Require Date"]).apply(lambda x: x.days),
    (zsd0551_order["SAP ETD on port"]-zsd0551_order["PO Require Date"]).apply(lambda x: x.days),
    (zsd0551_order["WH ETA"]+timedelta(days=7)-zsd0551_order["PO Require Date"]).apply(lambda x: x.days),
    (zsd0551_order["WH ETA"]-zsd0551_order["PO Require Date"]).apply(lambda x: x.days)
]

zsd0551_order["Datediff"] = np.select(datediff_conditions, datediff_values, default=np.nan)

#delay category
delay_conditions = [
    (zsd0551_order["Datediff"]<=3),
    (zsd0551_order["Datediff"]>3)&(zsd0551_order["Datediff"]<=7),
    (zsd0551_order["Datediff"]>7)&(zsd0551_order["Datediff"]<=14),
    (zsd0551_order["Datediff"]>14)&(zsd0551_order["Datediff"]<=21),
    (zsd0551_order["Datediff"]>21)&(zsd0551_order["Datediff"]<=28),
    (zsd0551_order["Datediff"]>28)
   ]

delay_values = ["No delay","Delay 1 week less","Delay 1 week+","Delay 2 weeks+","Delay 3 weeks+","Delay 4 weeks+"]

zsd0551_order["Delay Category"] = np.select(delay_conditions, delay_values, default=np.nan)


#551 filters

order_filter1 = zsd0551_order[zsd0551_order["HQ Fulfill Flag"].notnull() & zsd0551_order["Reject Reason Text"].isnull()]
order_filter2 =order_filter1.query("Status=='approved' or Status.isnull()")
zsd0551_order_filter=order_filter2.query("`Sales Type`=='Module'")


zsd0551_open_order =zsd0551_order_filter.query("`PGI Done`=='NO'")


open_order_overview=zsd0551_open_order[["Sales Team","SAP ID & Item No", "Pallet Group ID", "Sales Person Name","Customer Name", "Module Type", "Material", "Material Desc.", "PO PCS", "MW",
                 "PO Status","SAP ETD category","SAP ETD on port","SAP ETA category","SAP ETA on port","WH ETA","PO Require Date","Datediff","Delay Category","Inco Terms","Incoterms Location","Fulfilled By"]]


open_order_overview.to_excel("HQ order prep-test.xlsx", index=False)


