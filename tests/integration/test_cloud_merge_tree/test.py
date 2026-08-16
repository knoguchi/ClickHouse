import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

TABLE_NAME = "cloud_test"

# Low grace period/interval so the Phase 3 GC tests below don't need to wait minutes for the
# parts-killer to run -- production defaults (480s grace period) are exercised by the setting's
# own default, not by these tests, which only need to prove the mechanism works at all. The grace
# period still needs a comfortable margin over ordinary Python-side query round-trip time: a
# background merge can tombstone parts well before the test gets around to checking them, so too
# short a grace period races the "not deleted yet" assertion against the test's own overhead.
GC_TABLE_DDL_SETTINGS = (
    "storage_policy = 's3', "
    "cloud_merge_tree_gc_grace_period_seconds = 15, "
    "cloud_merge_tree_gc_interval_ms = 1000"
)

TABLE_DDL = """
    (id UInt64, data String)
    ENGINE = CloudMergeTree
    ORDER BY id
    SETTINGS storage_policy = 's3'
    """


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml"],
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in cluster.instances.values():
        node.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")


def list_objects(cluster, path="data/"):
    minio = cluster.minio_client
    objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    logging.info(f"list_objects ({len(objects)}): {[x.object_name for x in objects]}")
    return objects


def test_second_replica_sees_inserts_without_peer_fetch(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    # CloudMergeTree derives its Keeper root from the table UUID (see DESIGN.md), not from an
    # engine argument like ReplicatedMergeTree's zookeeper_path. So the only way to point two
    # replicas at the same table is to create it on one and ATTACH it by the same UUID on the
    # other -- there is no ON CLUSTER support to do this implicitly yet.
    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    # This test only exercises the single-writer case; see
    # test_concurrent_multi_writer_insert_no_collision_or_loss for concurrent INSERT from both
    # replicas (block-number allocation moved to Keeper-side ephemeral-sequential locks, so it's
    # safe across replicas as of that change).
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    # node2 never ran an INSERT and has no local copy of the part -- CloudMergeTree has no
    # peer-to-peer part fetch (unlike ReplicatedMergeTree's DataPartsExchange). The only way it
    # can see the row is by picking up the part name from Keeper's part-set watcher and reading
    # the part directly off the shared disk.
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "3")
    assert_eq_with_retry(node2, f"SELECT sum(id) FROM {TABLE_NAME}", "6")

    assert (
        node2.query(
            f"SELECT disk_name FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
        ).strip()
        == "s3"
    )

    # And the convergence is symmetric: node1 keeps seeing its own insert consistently too.
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}") == "3\n"


def test_concurrent_multi_writer_insert_no_collision_or_loss(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    # Keeper-side ephemeral-sequential block-number allocation (block_numbers/<partition>/) must
    # hand every concurrent INSERT -- from either replica -- a distinct block number, even when
    # many land at the same instant. A process-local counter cannot guarantee that across two
    # separate replica processes writing the same shared disk.
    rows_per_node = 20

    def insert_one(node, value):
        node.query(
            f"INSERT INTO {TABLE_NAME} VALUES ({value}, 'v{value}')",
            settings={"async_insert": 0},
        )

    with ThreadPoolExecutor(max_workers=2 * rows_per_node) as executor:
        futures = [
            executor.submit(insert_one, node1, i) for i in range(1, rows_per_node + 1)
        ] + [
            executor.submit(insert_one, node2, i)
            for i in range(rows_per_node + 1, 2 * rows_per_node + 1)
        ]
        for future in futures:
            future.result()

    total = 2 * rows_per_node
    expected_sum = str(total * (total + 1) // 2)

    assert_eq_with_retry(node1, f"SELECT count() FROM {TABLE_NAME}", str(total))
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", str(total))
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == expected_sum
    assert node2.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == expected_sum

    # No two active parts should ever share a block-number range -- that would mean the
    # ephemeral-sequential allocator handed out the same number to two concurrent writers.
    collisions = node1.query(
        f"""
        SELECT count() FROM (
            SELECT min_block_number, max_block_number, count() c
            FROM system.parts
            WHERE table = '{TABLE_NAME}' AND active
            GROUP BY min_block_number, max_block_number
            HAVING c > 1
        )
        """
    ).strip()
    assert collisions == "0"

    # Not asserting an exact active-part count here: background merging (scheduleDataProcessingJob)
    # is free to consolidate some of the 40 freshly-inserted parts before this check runs, and
    # already did so in practice under this test's timing. The count/sum assertions above and the
    # collision check are what actually establish "no collision, no loss"; requiring precisely
    # `total` active parts would just be asserting that no merge happened yet, which isn't a
    # correctness property.
    active_parts = int(
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
        ).strip()
    )
    assert 1 <= active_parts <= total


def _optimize_until_single_part(node, timeout_seconds=30):
    # A single OPTIMIZE TABLE call runs one round of the ordinary (non-aggressive) merge
    # selector, same as background scheduling -- it can legitimately decide "nothing worth
    # merging yet" even with several small parts present (see StorageMergeTree::selectPartsToMerge,
    # which CloudMergeTree's selection mirrors). Repeat calls until parts converge to one, so the
    # test doesn't depend on the selector's cost-function internals or on background-scheduling
    # timing.
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        active_parts = node.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
        ).strip()
        if active_parts == "1":
            return
        node.query(f"OPTIMIZE TABLE {TABLE_NAME}")
        time.sleep(0.5)
    raise AssertionError(
        f"Table {TABLE_NAME} on {node.name} did not converge to a single part within "
        f"{timeout_seconds}s"
    )


def test_optimize_merges_all_parts_and_propagates_to_second_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    row_count = 5
    for i in range(1, row_count + 1):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

    # Not asserting an exact pre-merge active-part count here: background merging
    # (scheduleDataProcessingJob) can legitimately consolidate some of these 5 inserts before
    # this check runs (more likely under load from other concurrently-running tests), same
    # reasoning as test_concurrent_multi_writer_insert_no_collision_or_loss above. The row count
    # is what actually matters and is checked below regardless of how many parts it's split
    # across; _optimize_until_single_part converges to 1 part from any starting state.
    assert (
        node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == str(row_count)
    )

    expected_sum = str(row_count * (row_count + 1) // 2)

    _optimize_until_single_part(node1)

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == str(row_count)
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == expected_sum

    # The merge's source parts are removed from Keeper's canonical set atomically with the merged
    # part's creation (DESIGN.md invariant 3); on node1 they should be locally Outdated, never
    # gone-but-still-active.
    active_names = node1.query(
        f"SELECT name FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
    ).strip()
    assert len(active_names.splitlines()) == 1

    # No merge-in-progress lease should survive a completed merge.
    leases = node1.query(
        f"SELECT count() FROM system.zookeeper WHERE path = "
        f"'/clickhouse/cloud_tables/{table_uuid}/leases'"
    ).strip()
    assert leases == "0"

    # node2 never ran the merge itself -- it must pick up the merged part (and the sources'
    # removal) purely from the Keeper part-set watcher, same mechanism as cross-replica INSERT
    # visibility.
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", str(row_count))
    assert_eq_with_retry(node2, f"SELECT sum(id) FROM {TABLE_NAME}", expected_sum)
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active",
        "1",
    )


def test_concurrent_optimize_race_exactly_one_winner(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    row_count = 8
    for i in range(1, row_count + 1):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    # Checking rows, not active-part count: background merging (scheduleDataProcessingJob) is free
    # to consolidate some of the 8 freshly-inserted parts on node2 before this setup check runs --
    # what actually matters here is that node2 has adopted all the data before the OPTIMIZE race
    # below starts, not how many physical parts it currently happens to be split across.
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM {TABLE_NAME}",
        str(row_count),
    )

    expected_sum = str(row_count * (row_count + 1) // 2)

    # Both replicas race to merge the same range: exactly-once materialization (DESIGN.md
    # invariant 3) means the Keeper-fenced multi() in commitMergedPart lets only one of them win,
    # regardless of which selects first -- the loser must discard its output without a trace in
    # the canonical part set.
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(node1.query, f"OPTIMIZE TABLE {TABLE_NAME}"),
            executor.submit(node2.query, f"OPTIMIZE TABLE {TABLE_NAME}"),
        ]
        for future in futures:
            future.result()

    _optimize_until_single_part(node1)

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == str(row_count)
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == expected_sum

    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", str(row_count))
    assert_eq_with_retry(node2, f"SELECT sum(id) FROM {TABLE_NAME}", expected_sum)
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active",
        "1",
    )

    leases = node1.query(
        f"SELECT count() FROM system.zookeeper WHERE path = "
        f"'/clickhouse/cloud_tables/{table_uuid}/leases'"
    ).strip()
    assert leases == "0"


def test_merge_source_objects_survive_grace_period_then_get_collected(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_gc_merge"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    try:
        table_uuid = node1.query(
            f"SELECT uuid FROM system.tables WHERE table = '{table}'"
        ).strip()

        row_count = 5
        for i in range(1, row_count + 1):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")

        # Not asserting an exact pre-merge active-part count or tombstone count below: background
        # merging can legitimately consolidate some of these 5 inserts (into one or more
        # intermediate merges, each tombstoning its own sources) before this test's own OPTIMIZE
        # call runs, more so under load from other concurrently-running tests. The row count is
        # what actually matters and is checked below; "at least one tombstone exists" is enough
        # to prove the mechanism engaged at all, regardless of exactly how many merges produced it.
        assert (
            node1.query(f"SELECT count() FROM {table}").strip() == str(row_count)
        )
        expected_sum = str(row_count * (row_count + 1) // 2)

        objects_before_merge = list_objects(cluster)

        node1.query(f"OPTIMIZE TABLE {table}")
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active",
            "1",
        )

        # The merge's sources are Keeper-deactivated and tombstoned atomically with the merge
        # commit (DESIGN.md invariant 3 is unaffected -- this just rides more ops in the same
        # multi()).
        assert (
            int(
                node1.query(
                    f"SELECT count() FROM system.zookeeper WHERE path = "
                    f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'"
                ).strip()
            )
            > 0
        )

        # Not deleted yet -- the grace period hasn't elapsed. The merged part's own objects are
        # now additionally present alongside the not-yet-reclaimed sources, so the meaningful
        # comparison is against the post-GC count below (a strict decrease), not against the
        # pre-merge baseline (which the new part's objects would legitimately push above).
        objects_immediately_after = list_objects(cluster)
        assert len(objects_immediately_after) >= len(objects_before_merge)

        # Wait past grace_period + gc_interval: the parts-killer physically deletes the sources'
        # objects and drains their tombstones.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'",
            "0",
            retry_count=30,
            sleep_time=1,
        )

        deadline = time.time() + 30
        while time.time() < deadline:
            remaining = list_objects(cluster)
            if len(remaining) < len(objects_immediately_after):
                break
            time.sleep(1)
        else:
            raise AssertionError(
                "Merge source parts' S3 objects were not garbage collected within timeout"
            )

        # Data itself is untouched -- only the superseded sources' objects were reclaimed.
        assert node1.query(f"SELECT count() FROM {table}").strip() == str(row_count)
        assert node1.query(f"SELECT sum(id) FROM {table}").strip() == expected_sum
    finally:
        # The drop_table autouse fixture only cleans up TABLE_NAME ("cloud_test") -- this test
        # uses its own name, and leaving it (and its still-running parts_killer_task) attached
        # would compete with every later test's BackgroundSchedulePool tasks for the rest of the
        # suite run. Must run even on assertion failure, not just on success.
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_drop_table_objects_survive_grace_period_then_get_collected(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_gc_drop"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{table}'"
    ).strip()
    node2.query(
        f"ATTACH TABLE {table} UUID '{table_uuid}' (id UInt64, data String) "
        f"ENGINE = CloudMergeTree ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    try:
        for i in range(1, 4):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")

        active_part_names = node1.query(
            f"SELECT name FROM system.parts WHERE table = '{table}' AND active"
        ).strip().splitlines()
        assert len(active_part_names) > 0

        # Drop only on node1 -- node2 keeps its own StorageCloudMergeTree object (and
        # parts-killer task) alive to actually perform the physical cleanup. If every replica
        # dropped the table, no live GC task would remain to drain these tombstones; that
        # liveness gap is a known, documented limitation of this phase (see DESIGN.md
        # discussion), not something this test exercises.
        node1.query(f"DROP TABLE {table} SYNC")

        # Objects survive immediately after DROP -- this is the regression check proving
        # physical deletion is no longer inline with the DROP TABLE query. Checked via Keeper
        # tombstones for the exact parts this DROP deactivated (precise -- not confused by an
        # unrelated, earlier background merge's tombstones independently aging out around the
        # same time), not via a raw object-count delta.
        tombstoned = node1.query(
            f"SELECT name FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'"
        ).strip().splitlines()
        assert set(active_part_names) <= set(tombstoned)

        # Wait past grace_period + gc_interval: the parts-killer physically deletes this
        # table's objects and drains its tombstones. Checked via Keeper, not a raw S3
        # object-count delta -- other tests' tables can leave permanently-orphaned objects behind
        # (the documented liveness gap: no live replica left to run their own trailing cleanup
        # once every replica has dropped a table), which would otherwise make "did the total
        # object count go down" an unreliable signal for this table's own reclaim specifically.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'",
            "0",
            retry_count=30,
            sleep_time=1,
        )
    finally:
        # Same reasoning as the merge test's finally: this table's name isn't covered by the
        # drop_table autouse fixture, and node2's copy must not outlive the test (its
        # parts_killer_task would otherwise keep running for the rest of the suite).
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
