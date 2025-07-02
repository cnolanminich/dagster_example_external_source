from dagster import (
    AssetKey,
    AutomationCondition,
    asset,
    op,
    job,
    ScheduleDefinition,
    Definitions,
    AssetSpec,
    AssetExecutionContext,
    sensor,
    SensorEvaluationContext,
    SensorResult,
    AssetMaterialization,
    AssetSelection,
)
import dagster as dg
import duckdb
import random
import time
from pathlib import Path as p
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets, DagsterDbtTranslator
from typing import List, Mapping, Tuple
import json
import pandas as pd

DUCKDB_PATH = p("dbt_project/example.duckdb")


def connect_with_retry(db_path, retries=5, delay=1):
    for attempt in range(retries):
        try:
            return duckdb.connect(db_path)
        except duckdb.IOException as e:
            if "Could not set lock on file" in str(e):
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise
            else:
                raise


def add_or_update_table(table_name):
    data = pd.DataFrame(
        {
            "offices": random.choices(
                ["scranton", "talahasse", "buffalo", "new york"], k=5
            ),
            "sales": [random.randint(1, 10000) for i in range(0, 5)],
            "last_updated": time.time(),
        }
    )

    conn = connect_with_retry(DUCKDB_PATH)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data")
    conn.close()


# Create sample duckdb warehouse with two tables
add_or_update_table("rarely_updated")
add_or_update_table("regularly_updated")


# Utility helpers for interacting with duckdb
def get_duckdb_tables():
    con = connect_with_retry(DUCKDB_PATH)
    tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main';
        """
    tables = con.execute(tables_query).fetchall()
    table_names = [table[0] for table in tables]
    return table_names


def get_duckdb_table_info():
    con = connect_with_retry(DUCKDB_PATH)
    query = """
        SELECT table_name,
        estimated_size as num_rows,
        -- duckdb doesn't have a last_modified column, but in Snowflake you could use this.
        -- last_modified
         FROM duckdb_tables()
        """
    results = con.execute(query).fetchall()
    # returns list of tuples: (table_name, num_rows, ...)
    return results


# in snowflake this isn't necessary, but in duckdb we need to
# get the last updated date from the table itself
def get_max_date_from_duckdb_table(table_name):
    con = connect_with_retry(DUCKDB_PATH)
    max_date_query = f"""
        SELECT MAX(last_updated)
        FROM {table_name}
        """
    max_date: List[Tuple[float, None]] = con.execute(max_date_query).fetchall()
    return float(max_date[0][0])


# Create a job to update the two tables above. We're going to pretend these are the external tables under control
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
    """Asset Factory that creates _external assets_"""
    return AssetSpec(key=f"main/{table_name}", group_name="source_data", kinds={"snowflake"})


external_sources = [create_external_table_asset(table) for table in external_tables]


# use an asset factory to create a regular "staged" assets downstream
# from the external source using an asset factory
def create_asset(table_name):
    """Asset factory that creates a regular asset"""

    @asset(
        name=f"{table_name}_staged",
        kinds={"databricks"},
        deps=[ dg.AssetKey(["main",table_name])],
        automation_condition=AutomationCondition.eager(),  # use declarative automation to propagate upstream changes
    )
    def my_asset():
        # code that does something, eg invoke a databricks job
        ...

        yield dg.MaterializeResult(metadata={"dagster/row_count": 0}) 
    return my_asset


staged = [create_asset(table) for table in external_tables]

# create a dbt asset also representing the silver layer
# under the hood the dbt integration looks a lot like the factory above

my_dbt_project = DbtProject(
    project_dir=p(__file__).parent.parent.parent.parent.joinpath("dbt_project").resolve()
)
my_dbt_project.prepare_if_dev()


class MyDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "model":
            return AutomationCondition.eager()

        return None

    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        return AssetKey([dbt_resource_props["schema"], dbt_resource_props["name"]])


@dbt_assets(
    manifest=my_dbt_project.manifest_path, dagster_dbt_translator=MyDbtTranslator()
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_row_counts()


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
            "last_updated": [
                time.time() for i in range(0, len(tables))
            ],  # or set this to 0 if you want all source tables and downstreams triggered when the sensor first runs
        }

    if type(cursor_str) is str:
        cursor: Mapping[str, List] = json.loads(cursor_str)

    updated_tables = []
    tables: List[str] = cursor["tables"]
    last_updates: List[float] = cursor["last_updated"]

    for i in range(0, len(tables)):
        table = tables[i]
        # Fetch all table info once
        if i == 0:
            duckdb_infos = get_duckdb_table_info()
            table_info_dict = {row[0]: row for row in duckdb_infos}
        duckdb_info = table_info_dict.get(table)
        if duckdb_info is None:
            continue

        num_rows = duckdb_info[1]
        max_date = get_max_date_from_duckdb_table(table)
        last_update = last_updates[i]
        context.log.info(
            f"TABLE {table} has last update {last_update} and current num_rows of {num_rows}"
        )
        if max_date > last_update:
            updated_tables.append(
                AssetMaterialization(
                    asset_key=dg.AssetKey(['main',table]),
                    description=f"Table {table} has been updated.",
                    metadata={
                        "dagster/row_count": num_rows,
                        "dagster/last_updated": max_date,
                        "external_update": f"Found new rows: num_rows is now {num_rows} (was {last_update})"
                    },
                )
            )
            last_updates[i] = max_date
    cursor["last_updated"] = last_updates
    context.log.info(f"updated tables: {updated_tables}")
    return SensorResult(asset_events=updated_tables, cursor=json.dumps(cursor))

@dg.asset_check(asset="rarely_updated_staged")
def check_row_count_comparison(context):
    """Check that compares row count between downstream asset and its upstream asset."""
    
    # Get the latest materialization events for both assets
    downstream_event = context.instance.get_latest_materialization_event(
        asset_key=dg.AssetKey("rarely_updated_staged")
    )
    upstream_event = context.instance.get_latest_materialization_event(
        asset_key=dg.AssetKey("rarely_updated")
    )
    
    # Check if both assets have been materialized
    if not downstream_event or not upstream_event:
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": "One or both assets have not been materialized"}
        )
    
    # Extract row count metadata
    downstream_materialization = downstream_event.asset_materialization
    upstream_materialization = upstream_event.asset_materialization
    
    downstream_row_count = downstream_materialization.metadata.get("dagster/row_count")
    upstream_row_count = upstream_materialization.metadata.get("dagster/row_count")
    
    # Check if row count metadata exists
    if not downstream_row_count or not upstream_row_count:
        return dg.AssetCheckResult(
            passed=False,
            metadata={"error": "Row count metadata missing from one or both assets"}
        )
    
    # Get the actual values
    downstream_count = downstream_row_count.value
    upstream_count = upstream_row_count.value
    
    # Compare row counts (example: downstream should have same rows)
    passed = downstream_count == upstream_count

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "downstream_row_count": downstream_count,
            "upstream_row_count": upstream_count,
            "difference": downstream_count - upstream_count
        }
    )


defs = Definitions(
    assets=[*external_sources, *staged, my_dbt_assets],
    asset_checks=[check_row_count_comparison],
    resources={"dbt": DbtCliResource(project_dir=my_dbt_project)},
    # safe to ignore, the following is part of the "mock" system
    jobs=[run_update_table],
    sensors=[watch_externals],
    schedules=[
        ScheduleDefinition(
            name="fake_external_system_updates",
            cron_schedule="*/3 * * * *",
            target=run_update_table,
        )
    ],
)
