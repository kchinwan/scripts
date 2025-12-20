import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from leaputils import Security
from leap.utils.Utilities import Utilities
import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry
import mysql.connector
from sqlalchemy import create_engine
import sys
import json
from urllib.parse import urlparse, quote_plus
import asyncio
# -------------------- DB Connection --------------------
def get_db_connection(args):
    
    db_cfg = json.loads(args['mysqlDS'])
    db_pwd = Utilities.decrypt(db_cfg['password'], db_cfg['salt'])
    parsed = urlparse(db_cfg['url'][5:])  # strip leading "mysql://"
    DB_USER = db_cfg['userName']
    DB_PWD  = db_pwd
    DB_HOST = parsed.hostname
    DB_PORT = parsed.port
    DB_NAME = parsed.path.lstrip('/')
    try:
        connection = mysql.connector.connect(
            user=DB_USER, password=DB_PWD,
            host=DB_HOST, port=DB_PORT, database=DB_NAME
        )
        engine = create_engine(
            f"mysql+mysqlconnector://{DB_USER}:%s@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            % quote_plus(DB_PWD)
        )
        print("Connected to MySQL")
    except Exception as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)
    return connection, engine

arguments = sys.argv
dict1 = {}
for arg in arguments:
        try:
            dict1[arg.split(':')[0]] = (':').join(arg.split(':')[1:])
        except:
            a = 'error'
       
connection, engine = get_db_connection(dict1)

query = '''
SELECT host, timestamp, metricType, MAX(value) AS value
FROM dynatrace_selnav_db
WHERE timestamp LIKE '2025-09-16%' 
  AND host = 'HOST-F52B15BB4B4D209D'
GROUP BY host, timestamp, metricType
 '''
df = pd.read_sql(query, engine)

import pandas as pd
import numpy as np
import asyncio
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from semantic_kernel.connectors.ai.open_ai import AzureTextEmbedding
from semantic_kernel.connectors.memory.azure_cognitive_search import AzureCognitiveSearchMemoryStore
from semantic_kernel.memory.semantic_text_memory import SemanticTextMemory

azure_openai_api_key = "null"
azure_openai_endpoint = ""
azure_api_version = "2024-02-01"
azure_deployment = "text-embedding-3-large-1"
azure_subscription_key = ""
search_endpoint = ""
search_admin_key = ""
vector_size = 3072
collection_name = "server-metrics"

CPU_METRIC = "CPU Usage %"
MEM_METRIC = "Memory Usage %"
DISK_UTIL_METRIC = "Disk Usage %"
READ_OPS_METRIC = "Read Ops"
WRITE_OPS_METRIC = "Write Ops"
ALL_METRICS = [CPU_METRIC, MEM_METRIC, DISK_UTIL_METRIC, READ_OPS_METRIC, WRITE_OPS_METRIC]

def slope_per_window(values):
    x = np.arange(len(values))
    mask = ~np.isnan(values)
    if mask.sum() < 2:
        return np.nan
    return np.polyfit(x[mask], values[mask], 1)[0]

def hourly_aggregate(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.floor("h")
    agg_rows = []
    for (hr, metric), g in df.groupby(["hour", "metricType"]):
        vals = g["value"].to_numpy()
        agg_rows.append({
            "hour": hr,
            "metricType": metric,
            "mean": np.nanmean(vals),
            "min": np.nanmin(vals),
            "max": np.nanmax(vals),
            "std": np.nanstd(vals, ddof=0),
            "slope_per_10min": slope_per_window(vals) * 10
        })
    return pd.DataFrame(agg_rows)

def add_zscores(agg):
    out = []
    for metric, g in agg.groupby("metricType"):
        mu = g["mean"].mean()
        sigma = g["mean"].std(ddof=0)
        z = (g["mean"] - mu) / sigma if sigma else 0
        gg = g.copy()
        gg["z_mean"] = z
        out.append(gg)
    return pd.concat(out, ignore_index=True)

def time_band_label(hr_ts):
    start = hr_ts
    end = hr_ts + pd.Timedelta(hours=1)
    fmt = lambda t: t.strftime("%I").lstrip("0") or "12"
    return f"{fmt(start)}-{fmt(end)} {'AM' if start.hour < 12 else 'PM'}"

def serialize_hour_record(host, day, hour_ts, metric_rows):
    dow = hour_ts.strftime("%a")
    band = time_band_label(hour_ts)
    month_end_flag = "yes" if (day + pd.offsets.MonthEnd(0) == day) else "no"
    order = {m: i for i, m in enumerate(ALL_METRICS)}
    metric_rows = metric_rows.sort_values(by=["metricType"], key=lambda s: s.map(order))
    lines = [f"Server={host}, {dow}, {band}, month_end={month_end_flag}."]
    for _, r in metric_rows.iterrows():
        unit = "%" if r["metricType"] not in {READ_OPS_METRIC, WRITE_OPS_METRIC} else ""
        lines.append(
            f"{r['metricType']} mean={r['mean']:.4f}{unit} "
            f"(z={r['z_mean']:+.3f}, slope={r['slope_per_10min']:+.4f}/10min). "
            f"min={r['min']:.4f}{unit}, max={r['max']:.4f}{unit}, std={r['std']:.4f}"
        )
    return " ".join(lines)

def build_hourly_texts(agg_z, host, day):
    rows = []
    for hr in sorted(agg_z["hour"].unique()):
        mr = agg_z[agg_z["hour"] == hr]
        text = serialize_hour_record(host, day, hr, mr)
        rows.append({"host": host, "day": str(day.date()), "hour": hr, "text_record": text})
    return pd.DataFrame(rows)

async def store_hourly_embeddings(df):
    memory_store = AzureCognitiveSearchMemoryStore(
        vector_size=vector_size,
        search_endpoint=search_endpoint,
        admin_key=search_admin_key
    )
    embedding_service = AzureTextEmbedding(
        deployment_name=azure_deployment,
        endpoint=azure_openai_endpoint,
        api_key=azure_openai_api_key,
        api_version=azure_api_version,
        default_headers={"Ocp-Apim-Subscription-Key": azure_subscription_key}
    )
    memory = SemanticTextMemory(storage=memory_store, embeddings_generator=embedding_service)
    for idx, row in df.iterrows():
        await memory.save_information(
            collection=collection_name,
            id=f"{row['host']}_{row['hour']}",
            text=row['text_record']
        )

if __name__ == "__main__":
    # df should be loaded from your SQL query before this point
    host = df["host"].iloc[0]
    day = pd.to_datetime(df["timestamp"].iloc[0]).normalize()
    agg = hourly_aggregate(df)
    agg_z = add_zscores(agg)
    hourly_texts_df = build_hourly_texts(agg_z, host, day)
    asyncio.run(store_hourly_embeddings(hourly_texts_df))
    search_client = SearchClient(
	    endpoint="",
	    index_name="server-metrics",
    	credential=AzureKeyCredential(""))
	
    results = search_client.search(search_text="*", top=5)
    for r in results:
	    print(r)

