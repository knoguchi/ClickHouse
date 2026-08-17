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
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml"],
            with_zookeeper=True,
            stay_alive=True,
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


def _optimize_query_until(node, optimize_query, condition, timeout_seconds=30):
    # Generalization of _optimize_until_single_part above for PARTITION/FINAL forms, whose
    # convergence condition isn't just "one active part total". Same underlying reason a single
    # call can legitimately no-op: selectPartsToMerge's parts-version freshness check
    # (StorageCloudMergeTree.cpp) bails out immediately -- not just for cost-based selection but
    # for selectAllPartsToMergeWithinPartition too -- if this replica's local watcher hasn't yet
    # caught up to Keeper's latest parts-version, e.g. right after a burst of INSERTs.
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        node.query(optimize_query)
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"{optimize_query!r} on {node.name} did not converge within {timeout_seconds}s"
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


def test_drop_partition_then_reinsert_identical_content_is_not_deduplicated(cluster):
    node1 = cluster.instances["node1"]
    ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree "
        "PARTITION BY id % 2 ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")

    # Same reasoning as test_truncate_then_reinsert_identical_content_is_not_deduplicated, scoped
    # to one partition: a real DROP PARTITION (not DETACH, which keeps the data recoverable) must
    # also clear that partition's deduplication_hashes/ entries, or re-inserting the exact same
    # content into the same partition afterwards is silently discarded.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'a')")
    even_partition_id = node1.query(
        f"SELECT DISTINCT partition_id FROM system.parts "
        f"WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()
    assert even_partition_id != ""

    node1.query(f"ALTER TABLE {TABLE_NAME} DROP PARTITION ID '{even_partition_id}'")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "0"

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'a')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "2"


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


def test_truncate_then_reinsert_identical_content_is_not_deduplicated(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")

    # insert_deduplicate defaults on: deduplication_hashes/ znodes are keyed by content hash and
    # are never cleared by TRUNCATE (unlike ReplicatedMergeTree's clearBlocksInPartition on every
    # drop-range path) -- so the same content inserted again after a TRUNCATE must still be
    # treated as new data, not silently discarded as a dedup hit against pre-TRUNCATE state. This
    # is the canonical staging-table reload workflow: TRUNCATE then re-INSERT the same batch.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"

    node1.query(f"TRUNCATE TABLE {TABLE_NAME}")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "0"

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "1"


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


def test_merge_rejects_combining_differently_mutated_parts(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_merge_mutation_version"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        # SYSTEM STOP MERGES during setup only -- without it, background merging consolidates most
        # of these rows' parts together *during* the insert loop itself (100 sequential INSERTs
        # take long enough for that), so few enough parts are left by the time the mutation below
        # is even submitted that it completes faster than any external polling could observe.
        node1.query(f"SYSTEM STOP MERGES {table}")

        row_count = 100
        for i in range(1, row_count + 1):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")

        node1.query(f"ALTER TABLE {table} UPDATE data = 'mutated' WHERE 1")

        # Merges and mutations both run via the same background scheduler and share one blocker
        # (SYSTEM STOP MERGES blocks mutation selection too, matching upstream), so both start
        # racing over these 100 fresh, still all-unmutated parts the moment this fires -- exactly
        # the interleaving canMergeParts must stay safe under.
        node1.query(f"SYSTEM START MERGES {table}")

        # Catch the transient window where the background mutation scheduler has applied the
        # mutation to some, but not all, of the several parts above -- canMergeParts must reject a
        # merge combining an already-mutated part with a not-yet-mutated one: the merge result
        # stamps mutation = max(sources), which would make partNeedsMutation() falsely report the
        # merged part as already covering data that was never actually transformed.
        deadline = time.time() + 30
        mutated_rows = 0
        while time.time() < deadline:
            mutated_rows = int(
                node1.query(f"SELECT count() FROM {table} WHERE data = 'mutated'").strip()
            )
            if 0 < mutated_rows < row_count:
                break
            time.sleep(0.05)
        assert 0 < mutated_rows < row_count, (
            "never observed a partially-mutated state to test the merge guard against "
            f"(mutated_rows={mutated_rows})"
        )

        node1.query(f"OPTIMIZE TABLE {table}")

        # Whatever OPTIMIZE did (merged some parts or not), every row must eventually show the
        # mutated value once the mutation itself finishes -- not silently skipped for some rows
        # because a merge stamped a part as "mutation done" without ever actually running the
        # mutation's commands on part of its data.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM {table} WHERE data = 'mutated'",
            str(row_count),
            retry_count=60,
            sleep_time=1,
        )
        assert node1.query(f"SELECT count() FROM {table}").strip() == str(row_count)
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
    # now goes through the mutation-carrying ALTER path (see
    # test_alter_modify_column_rewrites_existing_data and friends) rather than the purely
    # metadata-only, immediate-propagation case this test is actually about. COMMENT COLUMN is
    # genuinely metadata-only (AlterCommand::isCommentAlter()), so it exercises that promise
    # cleanly.
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


def test_alter_modify_order_by_propagates_to_second_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"

    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {ddl}")

    # A non-column ALTER (MODIFY ORDER BY here; also ADD INDEX/MODIFY TTL/MODIFY SETTING) only
    # serialized columns text into the Keeper metadata znode -- a non-issuing replica's watcher
    # bumped its own metadata_version but only ever re-parsed those (unchanged) columns, so it
    # never actually adopted the new sorting key. Two replicas would then register
    # differently-sorted parts in the same shared part set. is_in_sorting_key (not the sorting_key
    # expression string) so this doesn't depend on how the key gets formatted.
    node1.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra UInt64, MODIFY ORDER BY (id, extra)")

    assert_eq_with_retry(
        node2,
        f"SELECT is_in_sorting_key FROM system.columns "
        f"WHERE table = '{TABLE_NAME}' AND name = 'extra'",
        "1",
    )
    assert (
        node1.query(
            f"SELECT is_in_sorting_key FROM system.columns "
            f"WHERE table = '{TABLE_NAME}' AND name = 'extra'"
        ).strip()
        == "1"
    )


def test_alter_modify_order_by_survives_restart_on_second_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"

    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {ddl}")

    node1.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra UInt64, MODIFY ORDER BY (id, extra)")
    assert_eq_with_retry(
        node2,
        f"SELECT is_in_sorting_key FROM system.columns "
        f"WHERE table = '{TABLE_NAME}' AND name = 'extra'",
        "1",
    )

    # The watcher must persist the new schema to node2's own CREATE query (.sql file), not just
    # its in-memory metadata snapshot -- otherwise a restart re-parses the stale pre-ALTER schema,
    # which the constructor's own columns check throws INCOMPATIBLE_COLUMNS against (Keeper's
    # canonical metadata reflects the ALTER regardless of which replica issued it).
    node2.restart_clickhouse()

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a', 5)")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "1")
    assert (
        node2.query(
            f"SELECT is_in_sorting_key FROM system.columns "
            f"WHERE table = '{TABLE_NAME}' AND name = 'extra'"
        ).strip()
        == "1"
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


def test_alter_modify_column_rewrites_existing_data(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, '10'), (2, '20')")

    node1.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN data UInt64")

    # mutate() is fire-and-forget (no mutations_sync) -- the mutation that rewrites existing parts
    # to the new type runs asynchronously via the same background selectPartsToMutate() machinery
    # ALTER TABLE ... UPDATE/DELETE already uses, so give it a window to actually apply.
    assert_eq_with_retry(node1, f"SELECT sum(data) FROM {TABLE_NAME}", "30")
    assert node1.query(f"SELECT toTypeName(data) FROM {TABLE_NAME} LIMIT 1").strip() == "UInt64"

    # New inserts land in the new type, and are queried consistently alongside the rewritten rows.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (3, '5')")
    assert_eq_with_retry(node1, f"SELECT sum(data) FROM {TABLE_NAME}", "35")


def test_alter_combining_metadata_and_data_rewrite_commands(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, '10')")

    # A single ALTER statement mixing a metadata-only command (ADD COLUMN) and a data-rewrite
    # command (MODIFY COLUMN type change) -- both must land, committed via the same atomic
    # metadata+mutation Keeper multi() (the ADD COLUMN's default is metadata-only and applies
    # immediately; the MODIFY COLUMN's rewrite is mutation-driven and asynchronous).
    node1.query(
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra UInt64 DEFAULT 7, MODIFY COLUMN data UInt64"
    )

    assert node1.query(f"SELECT extra FROM {TABLE_NAME} WHERE id = 1").strip() == "7"
    assert_eq_with_retry(node1, f"SELECT sum(data) FROM {TABLE_NAME}", "10")
    assert node1.query(f"SELECT toTypeName(data) FROM {TABLE_NAME} LIMIT 1").strip() == "UInt64"


def test_alter_modify_column_data_rewrite_visible_on_other_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {TABLE_DDL}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, '10'), (2, '20')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "2")

    node1.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN data UInt64")

    # node2 never ran the ALTER itself -- it must pick up both the schema change (metadata watcher)
    # and the rewritten data (mutation execution, which any replica -- not just the ALTER's issuer
    # -- may end up running, same as a manually-submitted mutation).
    assert_eq_with_retry(
        node2, f"SELECT sum(data) FROM {TABLE_NAME}", "30", retry_count=40, sleep_time=1
    )
    assert node2.query(f"SELECT toTypeName(data) FROM {TABLE_NAME} LIMIT 1").strip() == "UInt64"


def test_concurrent_alter_modify_column_from_both_replicas_both_land(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_concurrent_modify"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, col_a String, col_b String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        table_uuid = node1.query(
            f"SELECT uuid FROM system.tables WHERE table = '{table}'"
        ).strip()
        node2.query(
            f"ATTACH TABLE {table} UUID '{table_uuid}' "
            f"(id UInt64, col_a String, col_b String) ENGINE = CloudMergeTree "
            f"ORDER BY id SETTINGS storage_policy = 's3'"
        )

        node1.query(f"INSERT INTO {table} VALUES (1, '10', '100')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "1")

        # Both replicas race to MODIFY a different column's type at once: the atomic
        # metadata+mutation multi()'s CAS-fenced retry (reload the latest columns, reapply,
        # retry) must let both metadata changes -- and both mutations -- land, not silently
        # drop one, same as the existing ADD COLUMN concurrent-ALTER test's intent.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    node1.query, f"ALTER TABLE {table} MODIFY COLUMN col_a UInt64"
                ),
                executor.submit(
                    node2.query, f"ALTER TABLE {table} MODIFY COLUMN col_b UInt64"
                ),
            ]
            for future in futures:
                future.result()

        assert_eq_with_retry(
            node1, f"SELECT toTypeName(col_a) FROM {table} LIMIT 1", "UInt64",
            retry_count=40, sleep_time=1,
        )
        assert_eq_with_retry(
            node1, f"SELECT toTypeName(col_b) FROM {table} LIMIT 1", "UInt64",
            retry_count=40, sleep_time=1,
        )
        assert_eq_with_retry(node1, f"SELECT col_a FROM {table} WHERE id = 1", "10")
        assert_eq_with_retry(node1, f"SELECT col_b FROM {table} WHERE id = 1", "100")
        assert_eq_with_retry(
            node2, f"SELECT toTypeName(col_a) FROM {table} LIMIT 1", "UInt64",
            retry_count=40, sleep_time=1,
        )
        assert_eq_with_retry(
            node2, f"SELECT toTypeName(col_b) FROM {table} LIMIT 1", "UInt64",
            retry_count=40, sleep_time=1,
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")

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


def test_mutation_applies_to_partition_reattached_after_submission(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'b')")

    # TABLE_DDL has no PARTITION BY, so DETACH PARTITION ID 'all' leaves the table's one and only
    # partition with zero active parts on this replica.
    node1.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION ID 'all'")

    # buildMutationEntry's table-wide affected-partition discovery must not silently exempt a
    # partition just because it happens to have no active parts on this replica right now -- the
    # mutation still needs to apply once the detached data comes back.
    node1.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 1")

    node1.query(f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION ID 'all'")

    assert_eq_with_retry(node1, f"SELECT count() FROM {TABLE_NAME} WHERE id = 1", "0")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.mutations WHERE table = '{TABLE_NAME}' AND is_done = 1",
        "1",
    )


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


def test_kill_mutation_prevents_data_rewrite(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_kill_mutation"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        node1.query(f"INSERT INTO {table} VALUES (1, 'orig')")

        # Pause background scheduling so the submitted mutation cannot start executing before we
        # kill it -- otherwise this test would race the background CloudMergeMutateTask.
        node1.query(f"SYSTEM STOP MERGES {table}")

        node1.query(f"ALTER TABLE {table} UPDATE data = 'mutated' WHERE 1")
        mutation_id = node1.query(
            f"SELECT mutation_id FROM system.mutations WHERE table = '{table}' AND NOT is_done"
        ).strip()
        assert mutation_id != ""

        node1.query(f"KILL MUTATION WHERE table = '{table}' AND mutation_id = '{mutation_id}'")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND mutation_id = '{mutation_id}'",
            "0",
        )

        node1.query(f"SYSTEM START MERGES {table}")

        # Give the (now mutation-less) background scheduler a moment to run, then confirm the kill
        # stuck -- the row was never rewritten.
        time.sleep(3)
        assert node1.query(f"SELECT data FROM {table} WHERE id = 1").strip() == "orig"
    finally:
        node1.query(f"SYSTEM START MERGES {table}")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_kill_mutation_on_finished_mutation_removes_it_from_system_mutations(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")

    node1.query(f"ALTER TABLE {TABLE_NAME} UPDATE data = 'b' WHERE 1")
    assert_eq_with_retry(node1, f"SELECT data FROM {TABLE_NAME} WHERE id = 1", "b")

    mutation_id = node1.query(
        f"SELECT mutation_id FROM system.mutations WHERE table = '{TABLE_NAME}' AND is_done = 1"
    ).strip()
    assert mutation_id != ""

    # CloudMergeTree never automatically removes a mutations/<id> znode once its work is done (a
    # documented, separate gap from this fix) -- KILL MUTATION on an already-finished mutation is
    # therefore also the only way to manually reclaim it today. Must succeed cleanly, not throw,
    # and actually remove the (now inert) Keeper entry.
    node1.query(f"KILL MUTATION WHERE table = '{TABLE_NAME}' AND mutation_id = '{mutation_id}'")
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.mutations WHERE table = '{TABLE_NAME}' AND mutation_id = '{mutation_id}'",
        "0",
    )
    # The already-applied data is untouched by killing the (already-finished) mutation entry.
    assert node1.query(f"SELECT data FROM {TABLE_NAME} WHERE id = 1").strip() == "b"


def test_kill_mutation_on_nonexistent_id_is_a_no_op(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")

    # KILL MUTATION targeting an id that was never submitted for this table at all must be a safe
    # no-op, not an error.
    node1.query(f"KILL MUTATION WHERE table = '{TABLE_NAME}' AND mutation_id = '99999999'")
    assert node1.query(f"SELECT data FROM {TABLE_NAME} WHERE id = 1").strip() == "a"


def test_covered_parts_are_reclaimed_from_local_memory_after_merge(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_covered_parts_reclaimed"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        for i in range(1, 4):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")

        node1.query(f"OPTIMIZE TABLE {table}")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active",
            "1",
        )

        # Before this fix, MergeTreeData::Transaction::commit()'s generic covered-part handling
        # demoted a merge's source parts to Outdated with a future remove_time, relying on the
        # generic old-parts cleanup thread to eventually erase them from data_parts_indexes --
        # a thread CloudMergeTree never runs. They must now be gone from system.parts entirely
        # (not just inactive), not lingering in local memory forever.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND NOT active",
            "0",
        )

        assert node1.query(f"SELECT count() FROM {table}").strip() == "3"
        assert node1.query(f"SELECT sum(id) FROM {table}").strip() == "6"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_optimize_deduplicate_removes_exact_duplicate_rows(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    # Two separate INSERTs land in two separate parts -- DEDUPLICATE only collapses duplicates
    # *within one merge*, not incrementally across the whole table's history, so this is the
    # minimum setup needed to actually exercise it. deduplicate_insert must be disabled for the
    # inserts themselves, or CloudMergeTree's own whole-block insert-time dedup (Phase 4 Step B)
    # would silently collapse the second identical INSERT before OPTIMIZE ever runs, defeating
    # the point of this test.
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')", settings={"deduplicate_insert": "disable"}
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')", settings={"deduplicate_insert": "disable"}
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')", settings={"deduplicate_insert": "disable"}
    )
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "3"

    node1.query(f"OPTIMIZE TABLE {TABLE_NAME} DEDUPLICATE")

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active",
        "1",
    )


def test_optimize_deduplicate_by_columns_dedups_on_subset(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    # Same id, different data -- a full-row DEDUPLICATE would NOT collapse these (rows differ),
    # but DEDUPLICATE BY id must, since it only compares the named column(s). Different data means
    # insert-time whole-block dedup never applies here regardless, unlike the test above.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'b')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"

    # No parens around the column list -- "DEDUPLICATE BY (id)" is a syntax error (confirmed via
    # upstream's own 01581_deduplicate_by_columns_local.sql test, which always uses a bare list).
    node1.query(f"OPTIMIZE TABLE {TABLE_NAME} DEDUPLICATE BY id")

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "1"


def test_optimize_cleanup_throws_cannot_assign_optimize(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")

    # CloudMergeTree's Phase 0 registration only supports MergingParams::Mode::Ordinary -- there
    # is no way to CREATE a Replacing-mode CloudMergeTree table yet, so CLEANUP must always
    # reject here, same as upstream would for any non-ReplacingMergeTree table.
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(f"OPTIMIZE TABLE {TABLE_NAME} CLEANUP")
    assert "CANNOT_ASSIGN_OPTIMIZE" in str(exc.value)


def test_optimize_plain_still_works_unchanged(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (3, 'c')")

    # Regression guard: a plain OPTIMIZE TABLE (no modifiers) -- the background scheduling path's
    # own CloudMergePlainMergeTreeTask construction -- must still work with the new required
    # deduplicate/deduplicate_by_columns/cleanup constructor arguments wired correctly.
    node1.query(f"OPTIMIZE TABLE {TABLE_NAME}")

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "3"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "6"
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active",
        "1",
    )


def test_replace_partition_from_replaces_destination_partition(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_replace_src"
    dst_table = "cloud_test_replace_dst"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id PARTITION BY id % 2 SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {ddl}")
    node1.query(f"CREATE TABLE {dst_table} {ddl}")
    try:
        # dst has old data in both partitions.
        node1.query(f"INSERT INTO {dst_table} VALUES (2, 'old0')")
        node1.query(f"INSERT INTO {dst_table} VALUES (3, 'old1')")

        # src has new data only for partition 0.
        node1.query(f"INSERT INTO {src_table} VALUES (10, 'new0a')")
        node1.query(f"INSERT INTO {src_table} VALUES (12, 'new0b')")

        node1.query(f"ALTER TABLE {dst_table} REPLACE PARTITION 0 FROM {src_table}")

        # Partition 0 in dst now holds exactly src's data, not the old row.
        result = sorted(
            node1.query(f"SELECT data FROM {dst_table} WHERE id % 2 = 0 ORDER BY id")
            .strip()
            .splitlines()
        )
        assert result == sorted(["new0a", "new0b"])

        # Partition 1 (untouched) still has its original data.
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 3").strip() == "old1"

        # This is a copy, not a move -- source is untouched.
        assert node1.query(f"SELECT count() FROM {src_table}").strip() == "2"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_replace_partition_then_reinsert_discarded_content_is_not_deduplicated(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_replace_dedup_src"
    dst_table = "cloud_test_replace_dedup_dst"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id PARTITION BY id % 2 SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {ddl}")
    node1.query(f"CREATE TABLE {dst_table} {ddl}")
    try:
        # dst's original row registers a deduplication_hashes/ entry for (2, 'old') in partition 0.
        node1.query(f"INSERT INTO {dst_table} VALUES (2, 'old')")

        # REPLACE discards that row -- src's differently-content'd row takes its place.
        node1.query(f"INSERT INTO {src_table} VALUES (2, 'new')")
        node1.query(f"ALTER TABLE {dst_table} REPLACE PARTITION 0 FROM {src_table}")
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 2").strip() == "new"

        # Re-inserting the exact content REPLACE just discarded must be treated as new data, not
        # silently dropped as a dedup hit against a row that no longer exists in dst.
        node1.query(f"INSERT INTO {dst_table} VALUES (2, 'old')")
        assert node1.query(f"SELECT count() FROM {dst_table}").strip() == "2"
        assert sorted(
            node1.query(f"SELECT data FROM {dst_table} ORDER BY data").strip().splitlines()
        ) == ["new", "old"]
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_attach_partition_from_adds_alongside_existing_data(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_attach_from_src"
    dst_table = "cloud_test_attach_from_dst"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id PARTITION BY id % 2 SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {ddl}")
    node1.query(f"CREATE TABLE {dst_table} {ddl}")
    try:
        node1.query(f"INSERT INTO {dst_table} VALUES (2, 'existing0')")
        node1.query(f"INSERT INTO {src_table} VALUES (10, 'new0')")

        # replace=false: src's partition-0 data joins dst's existing partition-0 data, nothing removed.
        node1.query(f"ALTER TABLE {dst_table} ATTACH PARTITION 0 FROM {src_table}")

        result = sorted(
            node1.query(f"SELECT data FROM {dst_table} WHERE id % 2 = 0 ORDER BY id")
            .strip()
            .splitlines()
        )
        assert result == sorted(["existing0", "new0"])
        assert node1.query(f"SELECT count() FROM {src_table}").strip() == "1"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_replace_partition_from_mismatched_schema_throws(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_replace_schema_src"
    dst_table = "cloud_test_replace_schema_dst"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(
        f"CREATE TABLE {src_table} (id UInt64, extra UInt64) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(f"CREATE TABLE {dst_table} {TABLE_DDL}")
    try:
        # checkStructureAndGetMergeTreeData() (generic, engine-agnostic) rejects a column-list
        # mismatch before any cloning is attempted.
        with pytest.raises(QueryRuntimeException) as exc:
            node1.query(f"ALTER TABLE {dst_table} REPLACE PARTITION ID 'all' FROM {src_table}")
        assert "INCOMPATIBLE_COLUMNS" in str(exc.value)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_replace_partition_visible_on_destination_other_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src_table = "cloud_test_replace_xreplica_src"
    dst_table = "cloud_test_replace_xreplica_dst"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {TABLE_DDL}")
    node1.query(f"CREATE TABLE {dst_table} {TABLE_DDL}")
    dst_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{dst_table}'"
    ).strip()
    node2.query(f"ATTACH TABLE {dst_table} UUID '{dst_uuid}' {TABLE_DDL}")
    try:
        node1.query(f"INSERT INTO {dst_table} VALUES (1, 'old')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {dst_table}", "1")

        node1.query(f"INSERT INTO {src_table} VALUES (2, 'new')")
        node1.query(f"ALTER TABLE {dst_table} REPLACE PARTITION ID 'all' FROM {src_table}")

        # node2 never ran the REPLACE PARTITION itself -- purely Keeper-watcher-driven, same
        # mechanism as any other cross-replica part-set change.
        assert_eq_with_retry(node2, f"SELECT count() FROM {dst_table}", "1")
        assert_eq_with_retry(node2, f"SELECT data FROM {dst_table} WHERE id = 2", "new")
    finally:
        node2.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")


def test_replace_partition_old_parts_survive_grace_period_then_reclaimed(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_replace_gc_src"
    dst_table = "cloud_test_replace_gc_dst"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(
        f"CREATE TABLE {src_table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(
        f"CREATE TABLE {dst_table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    try:
        dst_uuid = node1.query(
            f"SELECT uuid FROM system.tables WHERE table = '{dst_table}'"
        ).strip()
        node1.query(f"INSERT INTO {dst_table} VALUES (1, 'old')")
        node1.query(f"INSERT INTO {src_table} VALUES (2, 'new')")

        node1.query(f"ALTER TABLE {dst_table} REPLACE PARTITION ID 'all' FROM {src_table}")
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 2").strip() == "new"

        # The replaced-away old part is tombstoned (not deleted inline) -- same lazy-GC path as
        # DROP PARTITION and merge sources.
        assert (
            int(
                node1.query(
                    f"SELECT count() FROM system.zookeeper WHERE path = "
                    f"'/clickhouse/cloud_tables/{dst_uuid}/dropped_parts'"
                ).strip()
            )
            > 0
        )
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{dst_uuid}/dropped_parts'",
            "0",
            retry_count=30,
            sleep_time=1,
        )

        assert node1.query(f"SELECT count() FROM {dst_table}").strip() == "1"
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 2").strip() == "new"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_move_partition_to_table_moves_data(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_move_src"
    dst_table = "cloud_test_move_dst"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id PARTITION BY id % 2 SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {ddl}")
    node1.query(f"CREATE TABLE {dst_table} {ddl}")
    try:
        # src has data in both partitions; dst already has some data in partition 1.
        node1.query(f"INSERT INTO {src_table} VALUES (10, 'move0a')")
        node1.query(f"INSERT INTO {src_table} VALUES (12, 'move0b')")
        node1.query(f"INSERT INTO {src_table} VALUES (11, 'stay1')")
        node1.query(f"INSERT INTO {dst_table} VALUES (3, 'existing1')")

        node1.query(f"ALTER TABLE {src_table} MOVE PARTITION 0 TO TABLE {dst_table}")

        # Source: partition 0 is gone entirely, partition 1 untouched.
        assert node1.query(f"SELECT count() FROM {src_table} WHERE id % 2 = 0").strip() == "0"
        assert node1.query(f"SELECT data FROM {src_table} WHERE id = 11").strip() == "stay1"

        # Destination: gained partition 0's data, its own partition 1 data untouched.
        result = sorted(
            node1.query(f"SELECT data FROM {dst_table} WHERE id % 2 = 0 ORDER BY id")
            .strip()
            .splitlines()
        )
        assert result == sorted(["move0a", "move0b"])
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 3").strip() == "existing1"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_concurrent_insert_during_move_partition_is_not_lost(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src_table = "cloud_test_move_race_src"
    dst_table = "cloud_test_move_race_dst"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id PARTITION BY id % 2 SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {ddl}")
    node1.query(f"CREATE TABLE {dst_table} {ddl}")
    src_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{src_table}'").strip()
    node2.query(f"ATTACH TABLE {src_table} UUID '{src_uuid}' {ddl}")
    try:
        # SYSTEM STOP MERGES: isolate the concurrent-INSERT race under test from an unrelated one
        # -- a background merge opportunistically consolidating two of the several small parts
        # below while the MOVE is in flight hits the exact same "one of these parts is no longer
        # active" retry path for a completely different reason, unrelated to this test's actual
        # target.
        node1.query(f"SYSTEM STOP MERGES {src_table}")

        # Several parts in partition 0 -- movePartitionToTable's clone loop (one
        # cloneAndLoadDataPart() object-storage copy per part) takes long enough to give the
        # concurrent INSERT below a real chance to land inside the race window between that loop
        # and the commit multi().
        for i in (2, 4, 6, 8, 10):
            node1.query(f"INSERT INTO {src_table} VALUES ({i}, 'v{i}')")

        with ThreadPoolExecutor(max_workers=2) as executor:
            move_future = executor.submit(
                node1.query, f"ALTER TABLE {src_table} MOVE PARTITION 0 TO TABLE {dst_table}"
            )
            # node2, not node1: a genuinely different replica's INSERT, adopted through node1's
            # own watcher -- exactly CODE_REVIEW.md finding #4's failure scenario
            # (movePartitionToTable rebuilt its removal list from a fresh partition-wide scan on
            # every attempt, so a part that showed up in the meantime got tombstoned on the source
            # without ever having been cloned to the destination -- lost from both tables).
            insert_future = executor.submit(
                node2.query, f"INSERT INTO {src_table} VALUES (100, 'racing')"
            )
            move_future.result(timeout=60)
            insert_future.result(timeout=60)

        # The racing row must land in exactly one of the two tables -- never both (double-counted)
        # and never neither (lost). Which table depends on exactly where the race landed relative
        # to movePartitionToTable's internal snapshot, which this test deliberately does not pin
        # down -- only that the row isn't silently dropped.
        def total_for_id_100():
            src_count = int(node1.query(f"SELECT count() FROM {src_table} WHERE id = 100").strip())
            dst_count = int(node1.query(f"SELECT count() FROM {dst_table} WHERE id = 100").strip())
            return src_count + dst_count

        deadline = time.time() + 30
        total = total_for_id_100()
        while total != 1 and time.time() < deadline:
            time.sleep(0.5)
            total = total_for_id_100()
        assert total == 1, f"row id=100 present in {total} of {{src_table, dst_table}} combined, expected exactly 1"

        # The rest of partition 0's original content must still have moved over intact regardless.
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM {dst_table} WHERE id % 2 = 0 AND id != 100",
            "5",
        )
        assert (
            node1.query(f"SELECT count() FROM {src_table} WHERE id % 2 = 0 AND id != 100").strip()
            == "0"
        )
    finally:
        node2.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_move_partition_source_parts_survive_grace_period_then_reclaimed(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_move_gc_src"
    dst_table = "cloud_test_move_gc_dst"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(
        f"CREATE TABLE {src_table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    node1.query(
        f"CREATE TABLE {dst_table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        src_uuid = node1.query(
            f"SELECT uuid FROM system.tables WHERE table = '{src_table}'"
        ).strip()
        node1.query(f"INSERT INTO {src_table} VALUES (1, 'moved')")

        node1.query(f"ALTER TABLE {src_table} MOVE PARTITION ID 'all' TO TABLE {dst_table}")
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 1").strip() == "moved"
        assert node1.query(f"SELECT count() FROM {src_table}").strip() == "0"

        # The moved-away source part is tombstoned (not deleted inline) -- same lazy-GC path as
        # DROP PARTITION and REPLACE PARTITION's replaced-away parts.
        assert (
            int(
                node1.query(
                    f"SELECT count() FROM system.zookeeper WHERE path = "
                    f"'/clickhouse/cloud_tables/{src_uuid}/dropped_parts'"
                ).strip()
            )
            > 0
        )
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{src_uuid}/dropped_parts'",
            "0",
            retry_count=30,
            sleep_time=1,
        )

        # Destination's independent copy is untouched by the source's GC.
        assert node1.query(f"SELECT data FROM {dst_table} WHERE id = 1").strip() == "moved"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_move_partition_mismatched_schema_throws(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_move_schema_src"
    dst_table = "cloud_test_move_schema_dst"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {TABLE_DDL}")
    node1.query(
        f"CREATE TABLE {dst_table} (id UInt64, extra UInt64) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        with pytest.raises(QueryRuntimeException) as exc:
            node1.query(f"ALTER TABLE {src_table} MOVE PARTITION ID 'all' TO TABLE {dst_table}")
        assert "INCOMPATIBLE_COLUMNS" in str(exc.value)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_move_partition_visible_on_both_tables_other_replicas(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src_table = "cloud_test_move_xreplica_src"
    dst_table = "cloud_test_move_xreplica_dst"

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {TABLE_DDL}")
    node1.query(f"CREATE TABLE {dst_table} {TABLE_DDL}")
    src_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{src_table}'"
    ).strip()
    dst_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{dst_table}'"
    ).strip()
    node2.query(f"ATTACH TABLE {src_table} UUID '{src_uuid}' {TABLE_DDL}")
    node2.query(f"ATTACH TABLE {dst_table} UUID '{dst_uuid}' {TABLE_DDL}")
    try:
        node1.query(f"INSERT INTO {src_table} VALUES (1, 'moved')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {src_table}", "1")

        node1.query(f"ALTER TABLE {src_table} MOVE PARTITION ID 'all' TO TABLE {dst_table}")

        # Neither replica of either table ran the MOVE itself -- purely Keeper-watcher-driven.
        assert_eq_with_retry(node2, f"SELECT count() FROM {src_table}", "0")
        assert_eq_with_retry(node2, f"SELECT count() FROM {dst_table}", "1")
        assert_eq_with_retry(node2, f"SELECT data FROM {dst_table} WHERE id = 1", "moved")
    finally:
        node2.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


def test_concurrent_move_partition_opposite_directions_no_deadlock(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table_a = "cloud_test_move_concurrent_a"
    table_b = "cloud_test_move_concurrent_b"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id PARTITION BY id % 2 SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {table_a} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {table_b} SYNC")
    node1.query(f"CREATE TABLE {table_a} {ddl}")
    node1.query(f"CREATE TABLE {table_b} {ddl}")
    a_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table_a}'").strip()
    b_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table_b}'").strip()
    node2.query(f"ATTACH TABLE {table_a} UUID '{a_uuid}' {ddl}")
    node2.query(f"ATTACH TABLE {table_b} UUID '{b_uuid}' {ddl}")
    try:
        # a's partition 0 moves to b; b's partition 1 moves to a -- opposite directions between
        # the same two tables, issued from different replicas at once. Exercises the two-table
        # operation_with_data_parts_mutex lock-ordering discipline: without deadlock-free
        # ordering (std::lock()), these two calls could each hold one table's lock while waiting
        # for the other's, forever.
        node1.query(f"INSERT INTO {table_a} VALUES (10, 'a0')")
        node2.query(f"INSERT INTO {table_b} VALUES (11, 'b1')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table_a}", "1")
        assert_eq_with_retry(node1, f"SELECT count() FROM {table_b}", "1")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    node1.query, f"ALTER TABLE {table_a} MOVE PARTITION 0 TO TABLE {table_b}"
                ),
                executor.submit(
                    node2.query, f"ALTER TABLE {table_b} MOVE PARTITION 1 TO TABLE {table_a}"
                ),
            ]
            for future in futures:
                future.result(timeout=60)

        assert_eq_with_retry(node1, f"SELECT data FROM {table_b} WHERE id = 10", "a0")
        assert_eq_with_retry(node1, f"SELECT data FROM {table_a} WHERE id = 11", "b1")
        assert_eq_with_retry(node1, f"SELECT count() FROM {table_a} WHERE id % 2 = 0", "0")
        assert_eq_with_retry(node1, f"SELECT count() FROM {table_b} WHERE id % 2 = 1", "0")
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table_a} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {table_b} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table_a} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table_b} SYNC")


def test_system_stop_merges_prevents_background_merge(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    try:
        node1.query(f"SYSTEM STOP MERGES {TABLE_NAME}")

        node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')")
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES (3, 'c')")

        # Before getActionLock() was wired up, IStorage's default no-op ActionLock meant
        # scheduleDataProcessingJob() never even checked merges_blocker -- STOP MERGES had zero
        # effect and the background scheduler kept consolidating parts regardless. Give it a
        # window it would have used.
        time.sleep(3)
        assert node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
        ).strip() == "3"

        # SYSTEM START MERGES lifts the blocker and wakes the background scheduler immediately
        # (onActionLockRemove() -> background_operations_assignee.trigger()), but doesn't force a
        # merge to happen -- ordinary (non-aggressive) background selection has no bounded-time
        # guarantee of consolidating a handful of small parts (that's why every other merge-
        # completion test in this file uses an explicit OPTIMIZE TABLE rather than a timing-based
        # wait on the background scheduler alone). OPTIMIZE TABLE itself also respects the same
        # blocker (matches StorageMergeTree::merge(), which throws ABORTED if still stopped) -- so
        # calling it right after START MERGES doubles as direct proof the blocker was genuinely
        # released, not just that the scheduler woke up.
        node1.query(f"SYSTEM START MERGES {TABLE_NAME}")
        node1.query(f"OPTIMIZE TABLE {TABLE_NAME}")
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active",
            "1",
        )
        assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "6"
    finally:
        node1.query(f"SYSTEM START MERGES {TABLE_NAME}")


def test_optimize_throws_when_merges_stopped(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    try:
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')")
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')")

        node1.query(f"SYSTEM STOP MERGES {TABLE_NAME}")

        # An explicit OPTIMIZE must still respect SYSTEM STOP MERGES, matching
        # StorageMergeTree::merge()'s own merges_blocker.isCancelledForPartition() check -- not
        # silently proceed just because it bypasses the background scheduler.
        with pytest.raises(QueryRuntimeException) as exc:
            node1.query(f"OPTIMIZE TABLE {TABLE_NAME}")
        assert "ABORTED" in str(exc.value)

        node1.query(f"SYSTEM START MERGES {TABLE_NAME}")
        node1.query(f"OPTIMIZE TABLE {TABLE_NAME}")
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active",
            "1",
        )
    finally:
        node1.query(f"SYSTEM START MERGES {TABLE_NAME}")


def test_optimize_partition_merges_only_that_partition(cluster):
    node1 = cluster.instances["node1"]
    ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree "
        "PARTITION BY id % 2 ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")

    # The odd-id partition gets exactly one row/part for the table's entire lifetime -- nothing
    # can ever merge it with anything else, so its part count is a stable invariant regardless of
    # background merging timing, unlike the even-id partition below. That makes this test's "the
    # untouched partition wasn't touched" assertion robust without needing a SYSTEM STOP MERGES
    # dance to avoid racing the background scheduler.
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'v1')")
    for i in (2, 4, 6, 8, 10):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

    even_partition_id = node1.query(
        f"SELECT DISTINCT partition_id FROM system.parts "
        f"WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()
    assert even_partition_id != ""

    # PARTITION ID '<id>', not PARTITION <value>: same reasoning as the DROP PARTITION test above
    # -- don't assume how CloudMergeTree/MergeTree formats a partition-key expression into a
    # literal, look the ID up directly and pass it as an ID.
    _optimize_query_until(
        node1,
        f"OPTIMIZE TABLE {TABLE_NAME} PARTITION ID '{even_partition_id}'",
        lambda: node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
        ).strip()
        == "1",
    )

    # selectAllPartsToMergeWithinPartition must only ever grab the requested partition_id's parts
    # -- the odd-id partition's single part must be untouched (same part the whole time, not just
    # "still one part").
    assert (
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '1'"
        ).strip()
        == "1"
    )

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "6"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "31"


def test_optimize_partition_final_skips_already_merged_partition(cluster):
    node1 = cluster.instances["node1"]
    ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree "
        "PARTITION BY id % 2 ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")

    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (2, 'a')")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (4, 'b')")

    even_partition_id = node1.query(
        f"SELECT DISTINCT partition_id FROM system.parts "
        f"WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()
    assert even_partition_id != ""

    # Plain (non-FINAL) PARTITION OPTIMIZE first: merges the two level-0 parts into one part with
    # level > 0 -- the precondition optimize_skip_merged_partitions' short-circuit actually checks
    # for ("already merged", not just "one part").
    _optimize_query_until(
        node1,
        f"OPTIMIZE TABLE {TABLE_NAME} PARTITION ID '{even_partition_id}'",
        lambda: node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
        ).strip()
        == "1",
    )
    part_name_before = node1.query(
        f"SELECT name FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()

    # PARTITION ... FINAL on an already-single-merged-part partition must complete without
    # producing a redundant self-merge -- the part name (and thus the underlying data) must be
    # exactly the one just observed above, not a freshly produced one. optimize_skip_merged_partitions
    # must be explicitly enabled here: it defaults to off, and with it off FINAL unconditionally
    # re-merges even a lone already-merged part every call (upstream's own "materializing" self-merge
    # semantics) -- omitting the setting made this assertion race the unrelated watcher-staleness
    # gate instead of actually exercising the skip path (flaked once in 6 full-suite runs).
    node1.query(
        f"OPTIMIZE TABLE {TABLE_NAME} PARTITION ID '{even_partition_id}' FINAL "
        f"SETTINGS optimize_skip_merged_partitions = 1"
    )
    part_name_after = node1.query(
        f"SELECT name FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()
    assert part_name_after == part_name_before

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "6"


def test_optimize_final_converges_every_partition(cluster):
    node1 = cluster.instances["node1"]
    ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree "
        "PARTITION BY id % 2 ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")

    # Multiple parts in *both* partitions -- whole-table FINAL (no PARTITION clause) must converge
    # every currently-existing partition to one part each, not just whichever one the cost-based
    # selector would have picked first.
    for i in (2, 4, 6):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    for i in (1, 3, 5, 7):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

    def _both_partitions_converged():
        return node1.query(
            f"SELECT countDistinct(partition), count() FROM system.parts "
            f"WHERE table = '{TABLE_NAME}' AND active"
        ).strip() == "2\t2"

    _optimize_query_until(
        node1, f"OPTIMIZE TABLE {TABLE_NAME} FINAL", _both_partitions_converged
    )

    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "7"
    assert node1.query(f"SELECT sum(id) FROM {TABLE_NAME}").strip() == "28"


def test_optimize_partition_visible_on_second_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree "
        "PARTITION BY id % 2 ORDER BY id SETTINGS storage_policy = 's3'"
    )
    node1.query(f"CREATE TABLE {TABLE_NAME} {ddl}")
    table_uuid = node1.query(
        f"SELECT uuid FROM system.tables WHERE table = '{TABLE_NAME}'"
    ).strip()
    node2.query(f"ATTACH TABLE {TABLE_NAME} UUID '{table_uuid}' {ddl}")

    for i in (2, 4, 6):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    node1.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'v1')")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "4")

    even_partition_id = node1.query(
        f"SELECT DISTINCT partition_id FROM system.parts "
        f"WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
    ).strip()
    assert even_partition_id != ""

    # node2 never ran the OPTIMIZE itself -- same Keeper part-set watcher mechanism as every other
    # cross-replica visibility test in this file, just exercised for the new partition-scoped
    # selection path specifically.
    _optimize_query_until(
        node1,
        f"OPTIMIZE TABLE {TABLE_NAME} PARTITION ID '{even_partition_id}'",
        lambda: node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '0'"
        ).strip()
        == "1",
    )

    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active AND partition = '0'",
        "1",
    )
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE_NAME}", "4")
    assert_eq_with_retry(node2, f"SELECT sum(id) FROM {TABLE_NAME}", "13")


def test_lease_loss_during_optimize_does_not_hang(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_lease_loss"
    ddl = (
        "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id "
        "SETTINGS storage_policy = 's3', cloud_merge_tree_lease_staleness_ms = 0"
    )

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        for i in range(1, 6):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "5")

        # cloud_merge_tree_lease_staleness_ms = 0: any concurrent lease acquisition attempt treats
        # an existing lease as immediately stale and steals it, so two replicas racing the same
        # OPTIMIZE reliably drive one side's executeStep() into the lost-lease branch. OPTIMIZE
        # runs synchronously on the query's own thread (see optimizeUntilConverged()'s
        # executeHere() call, not the background pool), so without the finish()/lease_lost fix,
        # that side's query would hang forever inside the unconditional
        # merge_task->getFuture().get() -- surfacing here as a client-side timeout, not a
        # server-side symptom this test would otherwise have to poll for.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(node1.query, f"OPTIMIZE TABLE {table}"),
                executor.submit(node2.query, f"OPTIMIZE TABLE {table}"),
            ]
            for future in futures:
                future.result(timeout=60)

        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "5")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "5")
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
