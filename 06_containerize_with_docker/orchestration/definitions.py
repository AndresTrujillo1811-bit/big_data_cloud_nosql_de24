from pathlib import Path
import os
import dlt
import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# to import dlt script
import sys
sys.path.insert(0, '../data_extract_load')
from load_job_ads import jobads_source

# Paths
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")



# dlt Asset 
dlt_resource = DagsterDltResource() 
@dlt_assets(
    dlt_source = jobads_source(),
    dlt_pipeline = dlt.pipeline(
        pipeline_name="jobsearch",
        dataset_name="staging",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
    ),
)
def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource): 
    yield from dlt.run(context=context)
    
    

# dbt Asset
dbt_project_directory = Path(__file__).parents[1] / "data_transformation"
dbt_project = DbtProject(project_dir=dbt_project_directory, profiles_dir=Path(DBT_PROFILES_DIR))
dbt_resource = DbtCliResource(project_dir=dbt_project)
dbt_project.prepare_if_dev()

# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path,) #access metadata of dbt project so that dagster understand structure of the dbt project
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream() #compile the project and collect all results    