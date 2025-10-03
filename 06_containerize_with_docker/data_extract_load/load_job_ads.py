# Pipeline 

import dlt
import requests 
import json 
from pathlib import Path 
import os 

db_path = str(Path(__file__).parents[1] / "data_warehouse/job_ads.duckdb")

# Extract 
def get_ads(url_search, params):
    headers = {"accept": "application/json"}
    response = requests.get(url_search, headers=headers, params=params)
    response.raise_for_status()
    return json.loads(response.content.decode("utf8"))


@dlt.resource(write_disposition="replace")
def jobads_resourse(params):
    url = "https://jobsearch.api.jobtechdev.se"
    url_search = f"{url}/search"
    for ad in get_ads(url_search, params)["hits"]:
        yield ad
        

# Load        
def run_pipeline(table_name):
    pipeline = dlt.pipeline(
        pipeline_name="jobsearch",
        destination=dlt.destinations.duckdb(db_path), #updated destination
        dataset_name="staging"
    )
    params = {"limit": 100, "occupation-field": "6Hq3_tKo_V57"}
    load_info = pipeline.run(jobads_resourse(params=params), table_name=table_name)
    print(load_info)
    
    
if __name__ == "__main__":
    working_directory = Path(__file__).parent
    os.chdir(working_directory)
    run_pipeline(table_name="technical_field_job_ads")