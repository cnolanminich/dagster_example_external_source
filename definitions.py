from dagster import AssetKey, AutomationCondition, asset, op, job, ScheduleDefinition, Definitions, AssetSpec, AssetExecutionContext, sensor, SensorEvaluationContext, SensorResult, AssetMaterialization, AssetSelection, build_asset_context
import duckdb 
import random 
import time
from pathlib import Path as p
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets, DagsterDbtTranslator
from typing import List, Mapping, Tuple
import json
import pandas as pd

DUCKDB_PATH = p("dbt_project/example.duckdb")

def add_or_update_table(table_name):

    data = pd.DataFrame({
        "offices": random.choices(["scranton", "talahasse", "buffalo", "new york"], k=5),
        "sales": [random.randint(1,10000) for i in range(0,5)], 
        "last_updated": time.time()
    })

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data")
    conn.close()

# Create sample duckdb warehouse with two tables
add_or_update_table("rarely_updated")
add_or_update_table("regularly_updated")

# Utility helpers for interacting with duckdb
def get_duckdb_tables():
    con = duckdb.connect(DUCKDB_PATH)
    tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main';
        """
    tables = con.execute(tables_query).fetchall()
    table_names = [table[0] for table in tables]
    return table_names

def get_max_date_from_duckdb_table(table_name):
    con = duckdb.connect(DUCKDB_PATH)
    max_date_query = f"""
        SELECT MAX(last_updated)
        FROM {table_name}
        """
    max_date: List[Tuple[float, None]] = con.execute(max_date_query).fetchall() 
    return float(max_date[0][0])

# Create a job to update the two tables above. We're going to pretend these are the external tables under peloton control
# Ignore the "dagster" part of this if it's confusing
@op
def update_table():
    add_or_update_table("regularly_updated")

@job
def run_update_table():
    update_table()
# END fake dagster stuff just used to mock external table updates

# BEGIN the normal dagster stuff you'll need
# create an asset factory to identify the external assets
external_tables = get_duckdb_tables()
def create_external_table_asset(table_name):
    """ Asset Factory that creates _external assets_ """
    return AssetSpec(
        key=table_name,
        group_name="source_data",
        kinds={"snowflake"}
    )

external_sources = [create_external_table_asset(table) for table in external_tables]

# use an asset factory to create a regular "staged" assets downstream
# from the external source using an asset factory 
def create_asset(table_name):
    """ Asset factory that creates a regular asset """
    @asset(
            name=f"{table_name}_staged",
            kinds={"databricks"},
            deps=[table_name],
            automation_condition=AutomationCondition.eager() # use declarative automation to propagate upstream changes
    ) 
    def my_asset():
        # code that does something, eg invoke a databricks job
        ...

    return my_asset

staged = [create_asset(table) for table in external_tables]

# create a dbt asset also representing the silver layer 
# under the hood the dbt integration looks a lot like the factory above
 
my_dbt_project = DbtProject(
    project_dir=p(__file__).parent.joinpath("dbt_project").resolve()
)
my_dbt_project.prepare_if_dev()

class MyDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "model":
            return AutomationCondition.eager()
        
        return None
    
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

@dbt_assets(
        manifest=my_dbt_project.manifest_path,
        dagster_dbt_translator=MyDbtTranslator()
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()  


# create a sensor that will
# check the external sources for updates
# return observations which, when changing,
# will trigger the downstreams via the downstream's declarative automation policies
@sensor(target=AssetSelection.all())
def watch_externals(context: SensorEvaluationContext): 
    # sensors provide a scursor for managing state
    cursor_str: str | None = context.cursor

    # initialize this cursor (for the first time through)
    if cursor_str is None:
        tables = get_duckdb_tables()
        cursor = {
            "tables": tables,
            "last_updated": [time.time() for i in range(0, len(tables))] # or set this to 0 if you want all source tables and downstreams triggered when the sensor first runs
        }

    if type(cursor_str) is str: 
        cursor: Mapping[str, List] = json.loads(cursor_str)
    
    updated_tables = []
    tables: List[str] = cursor['tables']
    last_updates: List[float] = cursor['last_updated']

    for i in range(0, len(tables)): 
        # figure out if there are new rows
        table = tables[i]
        max_date = get_max_date_from_duckdb_table(table)
        last_update = last_updates[i]
        context.log.info(
            f"TABLE {table} has last update {last_update} and max update of {max_date}"
        )
        if max_date > last_update:
            # record an asset materialization for the external asset
            # this will log metadata and trigger the automation 
            # policies on the downstreams
            updated_tables.append(
                AssetMaterialization(
                    asset_key=table,
                    metadata={
                        "external_update": f"Found new rows with a max date of {max_date} which was greater than the last observation date of {last_update}"
                    }
                )
            )
            last_updates[i] = max_date
    
    cursor['last_updated'] = last_updates

    return SensorResult(
        asset_events=updated_tables,
        cursor=json.dumps(cursor)
    )

real_definitions = Definitions(
    assets=[*external_sources, *staged, my_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=my_dbt_project)
    },
    # safe to ignore, the following is part of the "mock" system
    jobs=[run_update_table],
    sensors=[watch_externals],
    schedules=[
        ScheduleDefinition(
            name="fake_external_system_updates",
            cron_schedule="*/3 * * * *",
            target=run_update_table
        ) 
    ]
)