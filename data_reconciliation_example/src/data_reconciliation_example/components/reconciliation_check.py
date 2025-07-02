import dagster as dg

class ReconciliationCheck(dg.Component, dg.Model, dg.Resolvable):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """

    # added fields here will define yaml schema via Model
    asset_key: list[str]
    upstream_asset_key: list[str]


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        
        asset_key = dg.AssetKey(self.asset_key)
        upstream_asset_key = dg.AssetKey(self.upstream_asset_key)
        # Add definition construction logic here.
        @dg.asset_check(asset=asset_key)
        def _check_row_count_comparison(context):
            """Check that compares row count between downstream asset and its upstream asset."""
            
            # Get the latest materialization events for both assets
            downstream_event = context.instance.get_latest_materialization_event(
                asset_key=asset_key
            )
            upstream_event = context.instance.get_latest_materialization_event(
                asset_key=upstream_asset_key
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

        return dg.Definitions(
            asset_checks=[_check_row_count_comparison]
        )