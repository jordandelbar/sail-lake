import dagster as dg

from lake.assets.aggregates import quakes_daily_region
from lake.assets.events import quakes_events, raw_quakes
from lake.assets.revisions import quakes_revisions
from lake.assets.seismicity import quakes_magnitude_frequency, quakes_region_seismicity
from lake.lakehouse import Lakehouse

assets = [
    quakes_events,
    quakes_daily_region,
    quakes_magnitude_frequency,
    quakes_region_seismicity,
    quakes_revisions,
]

quakes_job = dg.define_asset_job("quakes", selection=assets)

defs = dg.Definitions(
    assets=[raw_quakes, *assets],
    jobs=[quakes_job],
    schedules=[
        dg.ScheduleDefinition(
            name="quakes_15min",
            target=quakes_job,
            cron_schedule="5-59/15 * * * *",
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
    ],
    resources={"lakehouse": Lakehouse(spark_remote=dg.EnvVar("SPARK_REMOTE"))},
)
