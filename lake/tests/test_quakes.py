import math
import random
import subprocess
import sys

from conftest import raw_df, raw_row

from lake.quakes.events import events, merge_events
from lake.quakes.revisions import revisions
from lake.quakes.seismicity import magnitude_frequency, seismicity


def test_events_keep_the_latest_version_whatever_the_source(spark):
    raw = raw_df(
        spark,
        [
            raw_row("a", updated=1, mag=2.0),
            raw_row("a", updated=3, mag=2.4, source="backfill"),
            raw_row("a", updated=2, mag=2.2),
            raw_row("b", updated=1, mag=1.0),
        ],
    )
    out = {r.id: r.mag for r in events(raw).collect()}
    assert out == {"a": 2.4, "b": 1.0}


def test_merge_is_idempotent_and_only_newer_versions_replace(spark, tmp_path):
    table = "merge_test"
    v2 = events(raw_df(spark, [raw_row("a", updated=2, mag=2.2)]))
    v2.createOrReplaceTempView("_like")
    spark.sql(
        f"CREATE TABLE {table} USING iceberg LOCATION 'file://{tmp_path}/t' "
        "AS SELECT * FROM _like WHERE false"
    ).collect()

    def state():
        return sorted((r.id, r.mag) for r in spark.table(table).collect())

    merge_events(table, v2)
    merge_events(table, v2)
    assert state() == [("a", 2.2)]

    merge_events(table, events(raw_df(spark, [raw_row("a", updated=1, mag=9.9)])))
    assert state() == [("a", 2.2)], "an older version must not overwrite a newer one"

    merge_events(table, events(raw_df(spark, [raw_row("a", updated=3, mag=2.5)])))
    assert state() == [("a", 2.5)]
    spark.sql(f"DROP TABLE {table}").collect()


def test_backfill_copies_are_not_live_versions(spark):
    raw = raw_df(
        spark,
        [
            raw_row("live", updated=1, source="feed"),
            raw_row("live", updated=1, source="backfill"),
            raw_row("old-live", updated=1, source=None),
            raw_row("backfill-only", updated=5, source="backfill"),
        ],
    )
    out = {r.id: r.versions_captured for r in revisions(raw).collect()}
    assert out == {"live": 1, "old-live": 1}


def test_first_observed_is_unknown_when_any_capture_lacks_it(spark):
    raw = raw_df(
        spark,
        [
            raw_row("partial", updated=1, observed=None),
            raw_row("partial", updated=2, observed=1_600_000),
            raw_row("full", updated=1, observed=1_120_000),
            raw_row("full", updated=2, observed=1_600_000),
        ],
    )
    out = {r.id: r.minutes_to_first_observed for r in revisions(raw).collect()}
    assert out == {"partial": None, "full": 2.0}  # (1_120_000 - 1_000_000) ms = 2 min


def test_changes_are_net_between_earliest_and_latest_captured(spark):
    raw = raw_df(
        spark,
        [
            raw_row("x", updated=1, mag=2.0, type="earthquake"),
            raw_row("x", updated=2, mag=3.0, type="quarry blast"),
            raw_row("x", updated=3, mag=2.0, type="earthquake"),
            raw_row(
                "x", updated=3, mag=2.0, type="earthquake"
            ),  # same version landed twice
        ],
    )
    [r] = revisions(raw).collect()
    assert (r.versions_captured, r.net_mag_change, r.type_differs) == (3, 0.0, False)


def test_magnitude_bins_floor_including_negatives_and_cumulate(spark):
    ev = events(
        raw_df(
            spark,
            [
                raw_row(f"e{i}", updated=1, mag=m)
                for i, m in enumerate([-0.15, -0.1, 2.3, 2.34, 3.0])
            ],
        )
    )
    rows = {
        (r.mag_bin, r.n, r.n_cum)
        for r in magnitude_frequency(ev).collect()
        if r.region == "Alaska"
    }
    assert rows == {(-0.2, 1, 5), (-0.1, 1, 4), (2.3, 2, 3), (3.0, 1, 1)}


def test_b_value_recovers_a_known_gutenberg_richter_distribution(spark):
    rng, b = random.Random(42), 1.0
    # complete catalog from M1.0 with b = 1, magnitudes rounded to 0.1 like real catalogs
    mags = [round(0.95 - math.log10(rng.random()) / b, 1) for _ in range(20_000)]
    rows = [raw_row(f"r{i}", updated=1, mag=m) for i, m in enumerate(mags)]
    rows += [raw_row(f"s{i}", updated=1, mag=2.0, place="X, Sparse") for i in range(50)]
    ev = events(raw_df(spark, rows))
    out = {r.region: r for r in seismicity(magnitude_frequency(ev), ev, 200).collect()}
    assert out["Alaska"].mc == 1.2  # busiest bin 1.0, plus the 0.2 correction
    assert abs(out["Alaska"].b_value - b) < 0.05
    assert out["Sparse"].b_value is None, "too few events above Mc for a b-value"
    assert "ALL" not in out


def test_transformations_do_not_import_dagster():
    code = (
        "import sys, lake.quakes.events, lake.quakes.revisions, lake.quakes.seismicity; "
        "sys.exit('dagster' in sys.modules)"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0
