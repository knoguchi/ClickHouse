import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.client import QueryRuntimeException
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

    # No merge-in-progress lease should survive a completed merge. releaseLease() is best-effort
    # and runs just after the winning multi() succeeds, not inside it -- a background merge that
    # produced the just-observed single-part state can still be a few instructions away from
    # calling it, so this needs a retry like every other post-merge check here, not a bare assert.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper WHERE path = "
        f"'/clickhouse/cloud_tables/{table_uuid}/leases'",
        "0",
    )

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

    # Same reasoning as test_optimize_merges_all_parts_and_propagates_to_second_replica's lease
    # check: releaseLease() is best-effort and runs just after the winning multi() succeeds, not
    # inside it, so this needs a retry rather than a bare assert.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper WHERE path = "
        f"'/clickhouse/cloud_tables/{table_uuid}/leases'",
        "0",
    )


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


def test_drop_partition_removes_only_target_partition_and_gets_collected(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    partitioned_ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree "
        f"PARTITION BY id % 2 ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    # TABLE_NAME here (not a dedicated name): the drop_table autouse fixture cleans up by name
    # regardless of which DDL created the table, so no try/finally is needed just for that -- same
    # as the plain-DDL tests above.
    node1.query(f"CREATE TABLE {TABLE_NAME} {partitioned_ddl}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {partitioned_ddl}")

    for i in range(1, 11):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "10")

    # system.parts has no per-row data (it's part-level metadata) -- look up the even group's
    # partition_id via its `partition` column (the formatted partition-key value, "0" for the
    # even-id group under `PARTITION BY id % 2`) rather than assuming CloudMergeTree/MergeTree's
    # partition-ID string hashing/formatting; DROP PARTITION ID takes that value directly.
    even_partition_id = node1.query(
        f"SELECT DISTINCT partition_id FROM system.parts "
        f"WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()
    assert even_partition_id != ""

    node1.query(f"ALTER TABLE {TABLE_NAME} DROP PARTITION ID '{even_partition_id}'")

    # Only the even-id group is gone; the odd-id group (sum = 1+3+5+7+9 = 25) is untouched.
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "5"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "25"
    assert (
        node1.query(f"SELECT count() FROM {TABLE_NAME} WHERE id % 2 = 0").strip()
        == "0"
    )

    # Keeper-driven visibility: node2 never ran the DROP PARTITION itself.
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "5")
    assert_eq_with_retry(node2, f"SELECT sum(id) FROM {TABLE_NAME}", "25")

    # The dropped partition's parts are tombstoned (not deleted inline -- same lazy-GC path as
    # DROP TABLE and merge sources), then physically reclaimed after the grace period.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.zookeeper WHERE path = "
        f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'",
        "0",
        retry_count=30,
        sleep_time=1,
    )


def test_drop_part_removes_single_part_and_no_such_part_throws(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    for i in range(1, 4):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "3")

    active_parts = node1.query(
        f"SELECT name FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
    ).strip().splitlines()
    assert len(active_parts) >= 1
    victim = active_parts[0]
    # Not assuming one row per part: background merging could have already consolidated some of
    # the 3 inserts into a single part by the time active_parts was read above (same reasoning as
    # test_concurrent_multi_writer_insert_no_collision_or_loss's active-part-count comment) -- read
    # the victim's own row count via the `_part` virtual column instead of assuming 1.
    victim_rows = int(
        node1.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE _part = '{victim}'"
        ).strip()
    )

    node1.query(f"ALTER TABLE {TABLE_NAME} DROP PART '{victim}'")

    assert (
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' "
            f"AND active AND name = '{victim}'"
        ).strip()
        == "0"
    )
    remaining = str(3 - victim_rows)
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == remaining
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", remaining)

    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(f"ALTER TABLE {TABLE_NAME} DROP PART 'all_9999_9999_0'")
    assert "NO_SUCH_DATA_PART" in str(exc.value)


def test_truncate_empties_table_and_allows_further_inserts(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    for i in range(1, 6):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "5")

    node1.query(f"TRUNCATE TABLE {TABLE_NAME}")

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "0"
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "0")

    # The table itself, and its Keeper root, must still be usable -- TRUNCATE is not DROP TABLE.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (100, 'z')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "100"


def test_concurrent_optimize_and_drop_partition_no_corruption(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")

    row_count = 8
    for i in range(1, row_count + 1):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

    partition_id = node1.query(
        f"SELECT DISTINCT partition_id FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
    ).strip()
    assert partition_id != ""

    # A merge consolidating the partition's parts races DROP PARTITION removing the whole
    # partition. Whichever wins, removeActivePartsMatching()'s retry loop always re-reads the live
    # active set before each attempt, so it either removes the original sources (if it beats the
    # merge) or the merged part that superseded them (if the merge won first) -- either way the
    # partition ends up empty with no exception escaping either side, mirroring how
    # commitMergedPart's lease-fenced multi() already fails closed against a concurrent DROP today.
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(node1.query, f"OPTIMIZE TABLE {TABLE_NAME}"),
            executor.submit(
                node1.query,
                f"ALTER TABLE {TABLE_NAME} DROP PARTITION ID '{partition_id}'",
            ),
        ]
        for future in futures:
            future.result()

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "0"
    assert (
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
        ).strip()
        == "0"
    )


def test_insert_deduplication_identical_content_is_noop(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"

    # insert_deduplicate defaults to 1: the exact same block content (same values) inserted again
    # must be a silent no-op, not a duplicate row -- matches ReplicatedMergeTree's own semantics
    # for a client retry after a timeout.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"

    # Different content is not deduplicated -- proves the hash is content-sensitive, not just
    # "any repeat insert is dropped".
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"

    # One dedup hash node per surviving distinct content, not per INSERT call.
    assert (
        node1.query(
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{table_uuid}/deduplication_hashes'"
        ).strip()
        == "2"
    )


def test_insert_deduplication_can_be_disabled(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")

    # deduplicate_insert defaults to 'enable', which overrides insert_deduplicate outright (see
    # its own doc in Core/Settings.cpp) -- that's the setting that actually has to be flipped to
    # genuinely disable dedup, not insert_deduplicate on its own.
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')",
        settings={"deduplicate_insert": "disable"},
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')",
        settings={"deduplicate_insert": "disable"},
    )

    # With dedup genuinely off (not just defaulting to on), both identical-content inserts land as
    # separate rows.
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"


def test_concurrent_identical_insert_from_both_replicas_deduplicates_to_one(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    # Both replicas race to insert the exact same content at once: the dedup path's Keeper CAS
    # (bundled into the same multi() as the part-znode create, see commitInsertedPart) must let
    # only one land, regardless of which replica's multi() commits first.
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                node1.query,
                f"INSERT INTO {TABLE_NAME} VALUES (42, 'same')",
                settings={"async_insert": 0},
            ),
            executor.submit(
                node2.query,
                f"INSERT INTO {TABLE_NAME} VALUES (42, 'same')",
                settings={"async_insert": 0},
            ),
        ]
        for future in futures:
            future.result()

    assert_eq_with_retry(node1, f"SELECT count() FROM {TABLE_NAME}", "1")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")


def test_alter_update_and_delete_apply_correctly_and_gc_source(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_mutations"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
    ddl = f"(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{table}'"
    ).strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        for i in range(1, 6):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "5")

        node1.query(f"ALTER TABLE {table} UPDATE data = 'updated' WHERE id = 3")
        assert_eq_with_retry(node1, f"SELECT data FROM {table} WHERE id = 3", "updated")
        # Keeper-driven visibility: node2 never ran the mutation itself.
        assert_eq_with_retry(node2, f"SELECT data FROM {table} WHERE id = 3", "updated")

        node1.query(f"ALTER TABLE {table} DELETE WHERE id = 2")
        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "4")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "4")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND is_done = 1",
            "2",
        )

        # Each mutation's source part is tombstoned (not deleted inline) and reclaimed after the
        # grace period, exactly the same lazy-GC path as merge sources and DROP TABLE/PARTITION.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'",
            "0",
            retry_count=30,
            sleep_time=1,
        )

        # Rows untouched by either mutation survive throughout.
        assert node1.query(f"SELECT data FROM {table} WHERE id = 1").strip() == "v1"
        assert node1.query(f"SELECT data FROM {table} WHERE id = 5").strip() == "v5"
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_mutation_does_not_affect_parts_inserted_after_snapshot(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_mutation_snapshot"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        for i in range(1, 4):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'orig')")

        # Submits a mutation whose predicate would match every row -- including one inserted right
        # after submission. The block-number snapshot CloudMergeTreeCoordination::snapshotBlockNumbers
        # takes at submission time is what must exclude that later insert, not query timing.
        node1.query(f"ALTER TABLE {table} UPDATE data = 'mutated' WHERE 1")
        node1.query(f"INSERT INTO {table} VALUES (100, 'orig')")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND is_done = 1",
            "1",
        )

        assert node1.query(f"SELECT data FROM {table} WHERE id = 100").strip() == "orig"
        assert (
            node1.query(f"SELECT count() FROM {table} WHERE data = 'mutated'").strip()
            == "3"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_two_sequential_mutations_both_apply(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_two_mutations"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        node1.query(f"INSERT INTO {table} VALUES (1, 'a')")

        # The second mutation targets a part that doesn't exist yet at submission time (the first
        # mutation hasn't run) -- selectPartsToMutate() must converge across multiple background
        # cycles, applying the lowest-id pending mutation to whatever part currently carries the
        # row each time, not just fire once.
        node1.query(f"ALTER TABLE {table} UPDATE data = 'b' WHERE id = 1")
        node1.query(f"ALTER TABLE {table} UPDATE data = 'c' WHERE id = 1")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND is_done = 1",
            "2",
            retry_count=40,
            sleep_time=1,
        )
        assert node1.query(f"SELECT data FROM {table} WHERE id = 1").strip() == "c"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_concurrent_mutation_and_optimize_no_corruption(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_mutation_race"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        row_count = 8
        for i in range(1, row_count + 1):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")

        # A background merge and a submitted mutation race over overlapping parts:
        # currently_merging_mutating_parts (shared between selectPartsToMerge and
        # selectPartsToMutate) makes them mutually exclusive on any single part, but not on which
        # one reaches a given row first -- either interleaving must still converge to the same
        # correct end state, with neither side throwing.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(node1.query, f"OPTIMIZE TABLE {table}"),
                executor.submit(node1.query, f"ALTER TABLE {table} DELETE WHERE id = 4"),
            ]
            for future in futures:
                future.result()

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND is_done = 1",
            "1",
            retry_count=40,
            sleep_time=1,
        )
        assert node1.query(f"SELECT count() FROM {table}").strip() == str(row_count - 1)
        assert node1.query(f"SELECT count() FROM {table} WHERE id = 4").strip() == "0"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_add_column_default_applies_to_old_and_new_parts_cross_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")

    node1.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra UInt64 DEFAULT 42")

    # The row written before the ALTER has no `extra` in its own part -- the shared reader-path
    # default-materialization machinery (IMergeTreeReader::fillMissingColumns(), unmodified for
    # CloudMergeTree) must produce the default on the fly, on both replicas.
    assert node1.query(f"SELECT extra FROM {TABLE_NAME} WHERE id = 1").strip() == "42"
    assert_eq_with_retry(node2, f"SELECT extra FROM {TABLE_NAME} WHERE id = 1", "42")

    # A row inserted after the ALTER is written under the new schema and carries its real value.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'b', 100)")
    assert node1.query(f"SELECT extra FROM {TABLE_NAME} WHERE id = 2").strip() == "100"
    assert_eq_with_retry(node2, f"SELECT extra FROM {TABLE_NAME} WHERE id = 2", "100")


def test_comment_column_cross_replica(cluster):
    # Not DROP COLUMN: upstream ClickHouse's own AlterCommand::getMutationStageDecision()
    # classifies DROP COLUMN (like RENAME COLUMN/DROP INDEX/DROP PROJECTION/DROP STATISTICS) as
    # unconditionally requiring a mutation -- it's not actually metadata-only even on ordinary
    # MergeTree, since existing parts' on-disk column data needs an eventual cleanup pass. That
    # correctly falls under this step's NOT_IMPLEMENTED scope cut (see
    # test_alter_requiring_data_rewrite_throws_not_implemented) rather than being a supported
    # cross-replica case. COMMENT COLUMN is genuinely metadata-only (AlterCommand::isCommentAlter()),
    # so it exercises the same cross-replica propagation this step actually promises.
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")

    node1.query(f"ALTER TABLE {TABLE_NAME} COMMENT COLUMN data 'a comment'")

    assert (
        node1.query(
            f"SELECT comment FROM system.columns WHERE table = '{TABLE_NAME}' AND name = 'data'"
        ).strip()
        == "a comment"
    )
    assert_eq_with_retry(
        node2,
        f"SELECT comment FROM system.columns WHERE table = '{TABLE_NAME}' AND name = 'data'",
        "a comment",
    )
    # The table itself and its data are untouched.
    assert node1.query(f"SELECT data FROM {TABLE_NAME}").strip() == "a"


def test_alter_issued_on_second_replica_is_picked_up_by_first(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    # Symmetry check: node1 never runs this ALTER itself -- only its own watcher-driven pickup
    # (piggybacked on part_set_updating_task, see StorageCloudMergeTree.h) can make it appear here.
    node2.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra UInt64 DEFAULT 7")

    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.columns WHERE table = '{TABLE_NAME}' AND name = 'extra'",
        "1",
    )


def test_concurrent_alter_add_column_from_both_replicas_both_land(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    # Both replicas race to ADD a different column at once: trySetMetadata()'s CAS-fenced retry
    # (reload the latest columns, reapply on top, retry) must let both land, not silently drop one.
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                node1.query, f"ALTER TABLE {TABLE_NAME} ADD COLUMN col_a UInt64 DEFAULT 1"
            ),
            executor.submit(
                node2.query, f"ALTER TABLE {TABLE_NAME} ADD COLUMN col_b UInt64 DEFAULT 2"
            ),
        ]
        for future in futures:
            future.result()

    # Whichever replica's CAS lost the race (ZBADVERSION) only reflects the merged result in its
    # own memory once its retry lands; the *other* replica (which may have already returned from
    # its own successful, non-retried alter() call before that retry even started) only picks up
    # the second column later, via its background watcher -- an extra hop beyond a plain single
    # ALTER, so a longer window than the default is warranted here, same reasoning as this file's
    # other background-propagation-driven waits (e.g. the GC tests' 30x1s windows).
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.columns WHERE table = '{TABLE_NAME}' AND name IN ('col_a', 'col_b')",
        "2",
        retry_count=40,
        sleep_time=1,
    )
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.columns WHERE table = '{TABLE_NAME}' AND name IN ('col_a', 'col_b')",
        "2",
        retry_count=40,
        sleep_time=1,
    )


def test_alter_requiring_data_rewrite_throws_not_implemented(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")

    # String -> UInt64 is a valid conversion (passes checkAlterIsPossible) but is not free -- it
    # requires reparsing and rewriting every existing part, so AlterCommands::getMutationCommands()
    # returns non-empty and StorageCloudMergeTree::alter() must reject it for now (Phase 4 Step D's
    # documented scope cut), not silently mishandle it.
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN data UInt64")
    assert "NOT_IMPLEMENTED" in str(exc.value)


def test_attach_with_mismatched_columns_throws_incompatible_columns(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()

    # A stale/wrong ATTACH statement (extra column not present in Keeper's canonical schema)
    # must be rejected at startup, mirroring StorageReplicatedMergeTree's own
    # checkTableStructureAttempt behavior, rather than silently taking over with the wrong schema.
    mismatched_ddl = """
        (id UInt64, data String, extra UInt64)
        ENGINE = CloudMergeTree
        ORDER BY id
        SETTINGS storage_policy = 's3'
        """
    with pytest.raises(QueryRuntimeException) as exc:
        node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {mismatched_ddl}")
    assert "INCOMPATIBLE_COLUMNS" in str(exc.value)


def test_attach_with_comment_only_difference_succeeds(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()

    # ColumnsDescription::operator== excludes comments from equality (matches
    # StorageReplicatedMergeTree's own comparison semantics), so an ATTACH whose columns differ
    # only in a COMMENT clause must be accepted, not rejected.
    commented_ddl = """
        (id UInt64, data String COMMENT 'some comment')
        ENGINE = CloudMergeTree
        ORDER BY id
        SETTINGS storage_policy = 's3'
        """
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {commented_ddl}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert_eq_with_retry(
        node2, f"SELECT count() FROM {TABLE_NAME}", "1"
    )


def test_attach_after_alter_adopts_new_schema_and_metadata_version(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra UInt64 DEFAULT 7")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()

    # node2's ATTACH uses the post-ALTER column list -- it must be accepted (matches Keeper's
    # canonical schema, which is already at metadata_version 1, not 0) and node2's in-memory
    # metadata_version must be stamped correctly so parts it writes/merges stamp
    # metadata_version.txt correctly and don't spuriously conflict with a later ALTER.
    altered_ddl = """
        (id UInt64, data String, extra UInt64 DEFAULT 7)
        ENGINE = CloudMergeTree
        ORDER BY id
        SETTINGS storage_policy = 's3'
        """
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {altered_ddl}")

    node2.query(f"INSERT INTO {TABLE_NAME} (id, data) VALUES (1, 'a')")
    assert_eq_with_retry(
        node1, f"SELECT extra FROM {TABLE_NAME} WHERE id = 1", "7"
    )

    # A further ALTER issued from node2 (the just-attached replica) must not conflict with its
    # own adopted version -- proves metadata_version was stamped from Keeper, not left at the
    # ATTACH statement's own default of 0.
    node2.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN another UInt64 DEFAULT 9")
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.columns WHERE table = '{TABLE_NAME}' AND name = 'another'",
        "1",
    )


def test_detach_and_attach_partition_roundtrip(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'b')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"

    # TABLE_DDL has no PARTITION BY, so the table's single partition id is the constant "all".
    node1.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION ID 'all'")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "0"

    node1.query(f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION ID 'all'")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "3"


def test_detach_and_attach_part_by_name(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"

    part_name = node1.query(
        f"SELECT name FROM system.parts WHERE table = '{TABLE_NAME}' AND active ORDER BY name LIMIT 1"
    ).strip()

    node1.query(f"ALTER TABLE {TABLE_NAME} DETACH PART '{part_name}'")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"

    node1.query(f"ALTER TABLE {TABLE_NAME} ATTACH PART '{part_name}'")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "3"


def test_detach_on_one_replica_visible_on_other(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")

    node1.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION ID 'all'")

    # No explicit action on node2 -- the watcher-driven part-set diff already treats "znode gone
    # from parts/" uniformly regardless of cause (DROP, merge-source removal, or DETACH), so
    # DETACH becomes visible here for free, with no DETACH-specific code in the watcher.
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "0")


def test_attach_from_replica_that_did_not_detach(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")

    node1.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION ID 'all'")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "0")

    # ATTACH issued from node2 -- the replica that did *not* run the DETACH -- proves the
    # detached-parts registry is Keeper-native state, not per-replica-local.
    node2.query(f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION ID 'all'")
    assert node2.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"
    assert_eq_with_retry(node1, f"SELECT count() FROM {TABLE_NAME}", "1")


def test_attach_without_detach_throws_no_such_data_part(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")

    # Nothing was ever detached -- there is no detached_parts/ entry for partition "all" to attach.
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION ID 'all'")
    assert "NO_SUCH_DATA_PART" in str(exc.value)


def test_detached_part_survives_grace_period_and_can_be_reattached(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_detach_gc"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    try:
        node1.query(f"INSERT INTO {table} VALUES (1, 'a')")
        table_uuid = node1.query(
            f"SELECT uuid FROM system.tables WHERE table = '{table}'"
        ).strip()

        objects_before = list_objects(cluster)
        assert len(objects_before) > 0

        node1.query(f"ALTER TABLE {table} DETACH PARTITION ID 'all'")

        # Well past grace_period_seconds + gc_interval_ms (see GC_TABLE_DDL_SETTINGS) and several
        # parts-killer cycles -- the detached part's objects must never be touched, since they're
        # recorded under detached_parts/, a namespace the GC scan never reads. This is the core
        # invariant this whole feature exists to protect.
        time.sleep(20)
        objects_after_grace_period = list_objects(cluster)
        assert len(objects_after_grace_period) == len(objects_before)

        # No tombstone was ever created for it either -- detach uses a separate namespace from drop.
        assert (
            int(
                node1.query(
                    f"SELECT count() FROM system.zookeeper WHERE path = "
                    f"'/clickhouse/cloud_tables/{table_uuid}/dropped_parts'"
                ).strip()
            )
            == 0
        )

        node1.query(f"ALTER TABLE {table} ATTACH PARTITION ID 'all'")
        assert node1.query(f"SELECT count() FROM {table}").strip() == "1"
        assert node1.query(f"SELECT data FROM {table} WHERE id = 1").strip() == "a"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
