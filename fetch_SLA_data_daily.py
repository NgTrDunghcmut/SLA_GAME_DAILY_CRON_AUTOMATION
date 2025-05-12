from elasticsearch import Elasticsearch
import elasticsearch
from datetime import datetime, timedelta
import pandas as pd
from action import filebrowseraction
import smtplib,ssl
from load_dotenv import load_dotenv
import os
load_dotenv()
url=os.getenv("URL")
usr=os.getenv("USR")
pwd=os.getenv("PASS")
def fetch_data(time_point: datetime = datetime.now(), time_delta: int = 1, type: str = "normal"):
    
    client = Elasticsearch(
        url,
        request_timeout=30,
        verify_certs=False,
        basic_auth=(usr, pwd),
        # opaque_id=None,
        # transport_params={"headers": {"accept": "application/json", "content-type": "application/json"}}
    )

    # if not (isinstance(time_delta, int) and isinstance(time_point, datetime)):
    #     raise KeyError("Missing either time_period or time_point, please specify for time period")

    if isinstance(time_point, str):
        end_date_obj = datetime.strptime(time_point, "%Y-%m-%d")
    else:
        end_date_obj = time_point
    start_date_obj = end_date_obj - timedelta(days=time_delta)

    # Split query if dates are in different months
    if start_date_obj.month != end_date_obj.month:
        end_of_start_month = datetime(start_date_obj.year, start_date_obj.month + 1, 1) - timedelta(days=1)
        start_of_end_month = datetime(end_date_obj.year, end_date_obj.month, 1)

        date_ranges = [
            (start_date_obj.strftime("%Y-%m-%d"), end_of_start_month.strftime("%Y-%m-%d")),
            (start_of_end_month.strftime("%Y-%m-%d"), end_date_obj.strftime("%Y-%m-%d"))
        ]
    else:
        date_ranges = [(start_date_obj.strftime("%Y-%m-%d"), end_date_obj.strftime("%Y-%m-%d"))]

    all_data = []

    for gte, lte in date_ranges:
        query = {
            "query": {
                "range": {
                    "date": {
                        "gte": gte,
                        "lte": lte,
                        "format": "yyyy-MM-dd"
                    }
                }
            },
            "_source": ["service", "ip", "region", "date", "packet_loss_avg", "latency_avg","source_ip"],
            "size": 10000
        }

        # Determine index pattern
        idx_suffix = datetime.strptime(gte, "%Y-%m-%d").strftime("%m-%Y")
        if type == "vip":
            idx = f"sla-ftel-v2-probe-baseline-{idx_suffix}"
        else:
            idx = f"sla-ftel-v2-probe-baseline-{type}-{idx_suffix}"

        try:
            # Initial scroll search
            resp = client.search(index=idx, body=query, scroll="2m")
            scroll_id = resp['_scroll_id']
            hits = resp['hits']['hits']

            while hits:
                for doc in hits:
                    if '_source' in doc:
                        all_data.append(doc['_source'])

                resp = client.scroll(scroll_id=scroll_id, scroll="2m")
                scroll_id = resp['_scroll_id']
                hits = resp['hits']['hits']

            client.clear_scroll(scroll_id=scroll_id)

        except Exception as e:
            print(f"❌ Error fetching data for {gte} to {lte}: {e}")

    all_data_df = pd.DataFrame(all_data)

    if not all_data_df.empty:
        # output_file = f"./data_game_fpt_{type}_{start_date_obj.strftime('%Y-%m-%d')}.xlsx"
        # all_data_df.to_excel(output_file, index=False)
        print(f"✅ Data successfully fetched")
    else:
        print("⚠️ No data returned for the given date range.")

    return all_data_df


def main(mode,day):
    file=f"./result/daily_average_data_for_game_{datetime.now().strftime("%m-%d")}.xlsx"
    z=1
    ip_list=pd.read_csv("./ip_game.csv")["IP"].to_list()
    
    datavip = fetch_data(time_delta=z, type="vip")
    data=fetch_data(time_delta=z, type="normal")
    data2 = fetch_data(time_delta=z, type="rush")

    data=data[~(data["source_ip"]=="118.71.46.254")]
    data2=data2[~(data2["source_ip"]=="118.71.46.254")]
    datavip=datavip[~(datavip["source_ip"]=="118.71.46.254")]

    if ("118.71.46.254" in data["source_ip"].unique())|("118.71.46.254" in data2["source_ip"].unique())|("118.71.46.254" in datavip["source_ip"].unique()):
            print("still affected by 118.71.46.254")
    else:
        print("cleared from 118.71.46.254")
    list_data = {"vip":datavip.drop(columns="source_ip"), "normal":data.drop(columns="source_ip"), "rush":data2.drop(columns="source_ip")}

    print(list_data)

    for idx, i in list_data.items():
        list_data[idx] = i.groupby(['ip', 'date',"region"]).agg(
            service=('service', 'first'),  # Keep the first value of 'service' for each group
            latency_avg=('latency_avg', 'mean'),
            packet_loss_avg=('packet_loss_avg', 'mean')
        ).reset_index()


    group2 = list_data["rush"].groupby(["service","ip", "region"])[["packet_loss_avg","latency_avg"]]
    group1 = list_data["normal"].groupby(["service","ip","region"])[["packet_loss_avg","latency_avg"]] 
    service = [g[0][0] for g in group2]
    ip = [g[0][1] for g in group2]
    region = [g[0][2] for g in group2]
    latency_avg_per_week = [round((g[1].sum() / len(g[1]))[1], 2) for g in group2]
    packetloss_avg_per_week = [round((g[1].sum() / len(g[1]))[0], 2) for g in group2]
    type_list = ["rush"] * len(service)

    # From group1 (normal)
    service += [g[0][0] for g in group1]
    ip += [g[0][1] for g in group1]
    region += [g[0][2] for g in group1]
    latency_avg_per_week += [round((g[1].sum() / len(g[1]))[1], 2) for g in group1]
    packetloss_avg_per_week += [round((g[1].sum() / len(g[1]))[0], 2) for g in group1]
    type_list += ["normal"] * len(group1)
    final_df = pd.DataFrame()
    final_df["service"]=service
    final_df["ip"]=ip
    final_df["latency_avg_per_week"]=latency_avg_per_week
    final_df["packetloss_avg_per_week"]=packetloss_avg_per_week
    final_df["type"]=type_list
    final_df["region"]=region
    desire_order=["service","ip","region","date","packet_loss_avg","latency_avg"]
    data=list_data["normal"][desire_order]
    datavip=list_data["vip"][desire_order]
    data2=list_data["rush"][desire_order]
    final_df=final_df[final_df["ip"].isin(ip_list)]
    data=data[data["ip"].isin(ip_list)]
    data2=data2[data2["ip"].isin(ip_list)]
    datavip=datavip[datavip["ip"].isin(ip_list)]

    with pd.ExcelWriter(file, engine="openpyxl", mode="w") as writer:
        final_df.to_excel(writer, sheet_name="Averages", index=False)
        data.to_excel(writer, sheet_name="Normal", index=False)
        data2.to_excel(writer, sheet_name="Rush", index=False)
        datavip.to_excel(writer, sheet_name="VIP", index=False)

    if day=="weekday":
        filebrowseraction(file=file, args=mode)
        print("DONE")
    else:
        print("IN PROCESS")

if __name__ == "__main__":
    print("Running main.py as script")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", choices=["weekday", "weekend"], required=True)
    parser.add_argument("-m", choices=["up", "del"], required=True)
    args = parser.parse_args()
    print("Mode selected:", args.m)
    main(args.m, args.d)
    