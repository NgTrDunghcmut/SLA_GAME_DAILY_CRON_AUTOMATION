from elasticsearch import Elasticsearch
import elasticsearch
from datetime import datetime, timedelta
import pandas as pd
from action import filebrowseraction
from load_dotenv import load_dotenv
import os
load_dotenv()
url=os.getenv("URL")
usr=os.getenv("USR")
pwd=os.getenv("PASS")
def fetch_data(time_point: datetime = datetime.now(), time_delta: int = 7, type: str = "normal"):
    """
    Fetch data from Elasticsearch between a time range.
    If the range spans two different months, it will split the query accordingly.
    """
    print(Elasticsearch.__version__)
    client = Elasticsearch(
        url,
        request_timeout=30,
        verify_certs=False,
        basic_auth=(usr, pwd),
        # opaque_id=None,
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
            "_source": ["service", "ip", "region", "date", "packet_loss", "latency","source_ip","time"],
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


def main(mode):
    file=f"./result/all_source_ip_data_for_game_{datetime.now().strftime("%m-%d")}_v3.xlsx"
    if mode =="up":
       
        
        data=fetch_data(time_delta=2, type="samples")
   
        data=data[data["packet_loss"]>0]
        data=data[~(data["source_ip"]=="118.71.46.254")]

        # data2=data[(data["packet_loss"]>0)&(data["service"]=="FreeFire")]

        
        with pd.ExcelWriter(file, engine="openpyxl", mode="w") as writer:
            # final_df.to_excel(writer, sheet_name="Averages", index=False)
            data.to_excel(writer, sheet_name="all", index=False)
            # data2.to_excel(writer, sheet_name="lua chua", index=False)
            # datavip.to_excel(writer, sheet_name="VIP", index=False)

    print(f"FILE BROWSER MODE: {mode}")
    filebrowseraction(file=file, args=mode)
    print("DONE")

if __name__ == "__main__":
    print("Running main.py as script")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", choices=["up", "del"], required=True)
    args = parser.parse_args()
    print("Mode selected:", args.m)
    main(args.m)
    