import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

TABLE_NAME = "cloud_test"

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
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/backups_disk.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/backups_disk.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )

        # AZ-aware nodes, used only by the per-AZ leader fan-out tests below -- node1/node2 above
        # deliberately have no <placement> config, so every other test in this file keeps exercising
        # today's no-AZ-info-configured (gate is a no-op) behavior unchanged.
        cluster.add_instance(
            "node_az_a1",
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/az_a.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node_az_a2",
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/az_a.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node_az_b1",
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/az_b.xml"],
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


_backup_id_counter = 0


def _new_backup_name():
    global _backup_id_counter
    _backup_id_counter += 1
    return f"Disk('backups', 'cloud_merge_tree_{_backup_id_counter}/')"


def test_backup_restore_roundtrip(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_backup_basic"
    restored = "cloud_test_backup_basic_restored"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {restored} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        # Several separate INSERTs -- several parts -- so this also proves
        # attachRestoredParts() handles a multi-part MutableDataPartsVector batch, not just the
        # trivial single-part case.
        row_count = 5
        for i in range(1, row_count + 1):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")
        expected_sum = str(row_count * (row_count + 1) // 2)

        backup_name = _new_backup_name()
        node1.query(f"BACKUP TABLE {table} TO {backup_name}")
        node1.query(f"RESTORE TABLE {table} AS {restored} FROM {backup_name}")

        assert node1.query(f"SELECT count() FROM {restored}").strip() == str(row_count)
        assert node1.query(f"SELECT sum(id) FROM {restored}").strip() == expected_sum

        # The original table is untouched by taking a backup of it.
        assert node1.query(f"SELECT count() FROM {table}").strip() == str(row_count)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {restored} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_backup_restore_partition_only_restores_that_partition(cluster):
    node1 = cluster.instances["node1"]
    table = "cloud_test_backup_partition"
    restored = "cloud_test_backup_partition_restored"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {restored} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"PARTITION BY id % 2 ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        for i in range(1, 5):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")
        # Partition '0' holds the even ids (2, 4).

        backup_name = _new_backup_name()
        node1.query(f"BACKUP TABLE {table} PARTITION 0 TO {backup_name}")
        node1.query(f"RESTORE TABLE {table} AS {restored} FROM {backup_name}")

        assert node1.query(f"SELECT count() FROM {restored}").strip() == "2"
        assert (
            node1.query(f"SELECT arraySort(groupArray(id)) FROM {restored}").strip()
            == "[2,4]"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {restored} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_restored_table_visible_on_second_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_backup_cross_replica"
    restored = "cloud_test_backup_cross_replica_restored"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {restored} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {restored} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        node1.query(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")

        backup_name = _new_backup_name()
        node1.query(f"BACKUP TABLE {table} TO {backup_name}")
        node1.query(f"RESTORE TABLE {table} AS {restored} FROM {backup_name}")

        # attachRestoredParts() commits through the same Keeper path a normal INSERT does -- node2
        # must be able to see the restored rows purely via its own watcher, same as after any
        # other write, with no special-casing needed for "this part arrived via RESTORE."
        restored_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{restored}'").strip()
        node2.query(
            f"ATTACH TABLE {restored} UUID '{restored_uuid}' (id UInt64, data String) "
            f"ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"
        )
        assert_eq_with_retry(node2, f"SELECT count() FROM {restored}", "2")
        assert_eq_with_retry(node2, f"SELECT sum(id) FROM {restored}", "3")
    finally:
        node2.query(f"DROP TABLE IF EXISTS {restored} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {restored} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_second_replica_sees_inserts_without_peer_fetch(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    # CloudMergeTree derives its Keeper root from the table UUID (see README.md), not from an
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
    # part's creation (README.md invariant 3); on node1 they should be locally Outdated, never
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

    # Both replicas race to merge the same range: exactly-once materialization (README.md
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


def test_truncate_does_not_leak_outdated_parts_in_memory(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")
    for i in range(1, 4):
        node1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "3"

    node1.query(f"TRUNCATE TABLE {TABLE_NAME}")

    # removeActivePartsMatching() (TRUNCATE's and DROP PARTITION's shared path) must erase the
    # removed parts from this replica's in-memory data_parts_indexes immediately, not leave them
    # as Outdated pending a generic cleanup timer CloudMergeTree never runs (every other
    # removePartsFromWorkingSet() call site in this file already does this -- see e.g.
    # detachActivePartsMatching()'s identical pattern just above it). Checked via system.parts
    # with no `active` filter, so it also counts Outdated/Deleting entries, not just the active
    # set -- repeated TRUNCATE/DROP PARTITION cycles would otherwise grow this without bound.
    assert (
        node1.query(f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}'").strip()
        == "0"
    )


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


def test_insert_deduplication_token_distinguishes_identical_content(cluster):
    node1 = cluster.instances["node1"]

    node1.query(f"CREATE TABLE {TABLE_NAME} {TABLE_DDL}")

    # insert_deduplication_token is the documented way to tell two byte-identical inserts apart
    # (e.g. two genuinely distinct events that happen to produce the same row) -- a *different*
    # token must not be deduplicated against, even though the content hash alone would collide.
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')",
        settings={"insert_deduplication_token": "batch-1"},
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 'a')",
        settings={"insert_deduplication_token": "batch-2"},
    )
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "2"

    # The *same* token, on the other hand, is still deduplicated -- the token-aware hash isn't
    # just "always distinct", it's keyed on the token when one is present (the documented
    # retry-safety use case: the same token resubmitted after a client-side timeout must still be
    # a no-op).
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')",
        settings={"insert_deduplication_token": "batch-3"},
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (2, 'b')",
        settings={"insert_deduplication_token": "batch-3"},
    )
    assert node1.query(f"SELECT count() FROM {TABLE_NAME}").strip() == "3"


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


def _wait_failpoint_paused(node, failpoint, timeout=60):
    # SYSTEM WAIT FAILPOINT ... PAUSE blocks, so it runs on a worker thread: a failpoint that is
    # never reached must fail the test rather than hang it. The executor is not joined on the
    # failure path, because its worker is still stuck inside the blocking query.
    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(node.query, f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")
    done, _ = wait([future], timeout=timeout)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        raise AssertionError(f"failpoint {failpoint} was not reached within {timeout}s")
    pool.shutdown(wait=False)
    future.result()


def test_concurrent_mutation_selection_from_both_replicas_applies_all(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_concurrent_mutation_select"
    ddl = "(id UInt64, data String) ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    failpoint = "cloud_merge_tree_mutate_lease_acquired"
    try:
        node1.query(f"INSERT INTO {table} VALUES (1, 'a')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "1")

        # Two mutations pending on the same single part at once. node1's own background scheduler
        # is made to genuinely park right after acquiring a mutation's lease (via the
        # cloud_merge_tree_mutate_lease_acquired failpoint). node2's own scheduler is held off with
        # SYSTEM STOP MERGES (which also gates mutation selection, matching upstream's own
        # merges_blocker.isCancelled() check in scheduleDataProcessingJob) until node1 is confirmed
        # paused -- otherwise which replica's scheduler reaches mutation 1 first, and therefore
        # which mutation node1 ends up paused on, isn't controlled, and the two schedulers'
        # natural timing never reliably contends for the same lease at all. selectPartsToMutate()'s
        # former bug `continue`d the mutation loop past a lost lease instead of `break`ing it,
        # silently applying mutation 2 while mutation 1 was still held elsewhere, which permanently
        # hides mutation 1 as "done" (partNeedsMutation() only checks the stamped version) even
        # though its command was never applied.
        node2.query(f"SYSTEM STOP MERGES {table}")
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            node1.query(f"ALTER TABLE {table} UPDATE data = concat(data, '-m1') WHERE id = 1")
            node1.query(f"ALTER TABLE {table} UPDATE data = concat(data, '-m2') WHERE id = 1")

            # node2 is stopped, so node1 -- uncontended -- is guaranteed to select mutations in
            # ascending id order and pause on mutation 1 specifically, holding its lease.
            _wait_failpoint_paused(node1, failpoint)
            node2.query(f"SYSTEM START MERGES {table}")

            # node1 genuinely holds mutation 1's lease (parked, not yet committed) for the whole
            # sleep below -- node2's own scheduler gets a generous number of cycles to (incorrectly,
            # pre-fix) race ahead to mutation 2 on the same part.
            time.sleep(5)
            assert node2.query(f"SELECT data FROM {table} WHERE id = 1").strip() == "a", (
                "node2 must not apply mutation 2 while node1 still holds mutation 1's lease"
            )

            node1.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            node2.query(f"SYSTEM START MERGES {table}")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND is_done = 1",
            "2",
            retry_count=40,
            sleep_time=1,
        )
        assert_eq_with_retry(
            node2,
            f"SELECT count() FROM system.mutations WHERE table = '{table}' AND is_done = 1",
            "2",
            retry_count=40,
            sleep_time=1,
        )
        assert node1.query(f"SELECT data FROM {table} WHERE id = 1").strip() == "a-m1-m2"
        assert_eq_with_retry(node2, f"SELECT data FROM {table} WHERE id = 1", "a-m1-m2")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
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
    gate = "cloud_merge_tree_schedule_pause"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        # SYSTEM STOP MERGES during setup only -- without it, background merging consolidates the
        # two rows below together during the insert loop itself.
        node1.query(f"SYSTEM STOP MERGES {table}")
        node1.query(f"INSERT INTO {table} VALUES (1, 'v1')")
        node1.query(f"INSERT INTO {table} VALUES (2, 'v2')")
        node1.query(f"ALTER TABLE {table} UPDATE data = 'mutated' WHERE 1")

        # cloud_merge_tree_schedule_pause gates every future scheduleDataProcessingJob call at its
        # very entry -- before it can even select, let alone tag, a part. Letting exactly one
        # paused attempt through (one SYSTEM NOTIFY) lets the background scheduler select and start
        # processing exactly one of the two parts above; leaving the gate closed afterwards (no
        # further NOTIFY) prevents any subsequent scheduling attempt from ever selecting the other
        # one, so it's guaranteed to remain un-mutated for as long as we hold the gate. Natural
        # two-part timing was tried first (100 rows, polling for a partial-mutation state) and
        # never reliably caught the window: the background pool processes parts faster than
        # external SQL polling can observe an intermediate state.
        node1.query(f"SYSTEM ENABLE FAILPOINT {gate}")
        try:
            node1.query(f"SYSTEM START MERGES {table}")
            _wait_failpoint_paused(node1, gate)
            node1.query(f"SYSTEM NOTIFY FAILPOINT {gate}")

            # The one part let through above must fully commit its mutation (the gate only blocks
            # *future* scheduling attempts, not work already handed off) while the other, still
            # ungated-out, part remains untouched.
            assert_eq_with_retry(
                node1, f"SELECT count() FROM {table} WHERE data = 'mutated'", "1", retry_count=60, sleep_time=1
            )
            assert node1.query(f"SELECT count() FROM {table} WHERE data != 'mutated'").strip() == "1", (
                "the second part must still be untouched while the schedule gate is held closed"
            )

            # canMergeParts must reject combining the just-mutated part (mutation=1) with the
            # still-pending one (mutation=0): the merge result stamps mutation = max(sources),
            # which would make partNeedsMutation() falsely report the merged part as already
            # covering data that was never actually transformed. OPTIMIZE TABLE is a separate code
            # path (optimizeUntilConverged), not gated by cloud_merge_tree_schedule_pause, so it can
            # run right now while the background scheduler is held off.
            node1.query(f"OPTIMIZE TABLE {table}")
            assert node1.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active").strip() == "2", (
                "OPTIMIZE must not merge a mutated part with a not-yet-mutated one still pending the same mutation"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {gate}")

        # Whatever OPTIMIZE did, every row must eventually show the mutated value once the
        # mutation itself finishes -- not silently skipped for one row because a merge stamped a
        # part as "mutation done" without ever actually running the mutation's commands on it.
        assert_eq_with_retry(
            node1, f"SELECT count() FROM {table} WHERE data = 'mutated'", "2", retry_count=60, sleep_time=1
        )
        assert node1.query(f"SELECT count() FROM {table}").strip() == "2"
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {gate}")
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
            # own watcher. Reproduces a real bug found during development: movePartitionToTable
            # rebuilt its removal list from a fresh partition-wide scan on every attempt, so a
            # part that showed up in the meantime got tombstoned on the source without ever
            # having been cloned to the destination -- lost from both tables.
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
    failpoint = "cloud_merge_tree_merge_lease_acquired"
    gate = "cloud_merge_tree_schedule_pause"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        # Neither replica's *background scheduler* must be the one to reach the merge-lease
        # failpoint below -- only the explicit OPTIMIZE call this test issues on node1 must be,
        # since that's the one whose client-side result() call actually observes a hang.
        # cloud_merge_tree_schedule_pause gates every scheduleDataProcessingJob call at its
        # very entry, unconditionally, independent of SYSTEM STOP/START MERGES -- so enabling it
        # up front on *both* nodes (and never resuming it until
        # the very end) fully removes both background schedulers as candidates; explicit OPTIMIZE
        # goes through a completely separate call path (optimizeUntilConverged), unaffected on
        # either node. Gating node1 alone isn't enough: node2 independently sees all 5 rows via
        # replication and, left ungated, its own background scheduler merges them on its own well
        # before node1's explicit OPTIMIZE even runs -- confirmed via server logs the first time
        # this test was written (node2's own merge, reflected onto node1 via the watcher, before
        # node1's OPTIMIZE reached the failpoint at all).
        node1.query(f"SYSTEM ENABLE FAILPOINT {gate}")
        node2.query(f"SYSTEM ENABLE FAILPOINT {gate}")
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            for i in range(1, 6):
                node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")
            assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "5")

            # node1's own OPTIMIZE is made to genuinely park right after acquiring the merge's
            # lease, so node2's own OPTIMIZE is guaranteed -- not just likely -- to steal it
            # (cloud_merge_tree_lease_staleness_ms = 0: any concurrent acquisition attempt treats
            # an existing lease as immediately stale) while node1 is still paused: natural
            # two-replica timing was tried first and never reproduced the race, since node1's own
            # merge of these few rows reliably finished before node2 could genuinely contend for
            # the lease. OPTIMIZE runs synchronously on the query's own thread (see
            # optimizeUntilConverged()'s executeHere() call, not the background pool), so without
            # the finish()/lease_lost fix, node1's query would hang forever inside the
            # unconditional merge_task->getFuture().get() once it resumes and discovers its lease
            # is gone -- surfacing here as a client-side timeout, not a server-side symptom to poll
            # for.
            # A plain thread (not ThreadPoolExecutor), and daemon=True: on the pre-fix hang, the
            # submitted query never returns and this thread stays blocked on its socket read
            # forever, with no way to cancel a thread that's already running -- a non-daemon
            # thread would then block the whole pytest worker process from exiting even after
            # this test itself finishes and reports failed.
            node1_result = {}

            def run_node1_optimize():
                try:
                    node1_result["value"] = node1.query(f"OPTIMIZE TABLE {table}")
                except Exception as e:  # noqa: BLE001
                    node1_result["error"] = e

            node1_thread = threading.Thread(target=run_node1_optimize, daemon=True)
            node1_thread.start()

            _wait_failpoint_paused(node1, failpoint)
            node2.query(f"OPTIMIZE TABLE {table}")
            assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "5")
            assert (
                node2.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active").strip() == "1"
            )

            node1.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
            node1_thread.join(timeout=30)
            if node1_thread.is_alive():
                raise AssertionError("node1's OPTIMIZE did not return within 30s after losing its lease")
            if "error" in node1_result:
                raise node1_result["error"]
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            node1.query(f"SYSTEM DISABLE FAILPOINT {gate}")
            node2.query(f"SYSTEM DISABLE FAILPOINT {gate}")

        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "5")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "5")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node1.query(f"SYSTEM DISABLE FAILPOINT {gate}")
        node2.query(f"SYSTEM DISABLE FAILPOINT {gate}")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_select_sequential_consistency_sees_fresh_insert_despite_stalled_watcher(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_sequential_consistency"
    gate = "cloud_merge_tree_part_set_watcher_pause"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(
        f"ATTACH TABLE {table} UUID '{table_uuid}' (id UInt64, data String) "
        f"ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        # node2's own background watcher (part_set_updating_task) is what normally picks up
        # node1's INSERT asynchronously -- gating it (before it can ever call
        # updatePartSetFromKeeper()) proves read()'s own synchronous catch-up path under
        # select_sequential_consistency works independently of it, not just "usually fast enough"
        # under natural timing, which is not reliable proof on its own (background adoption
        # can race ahead of a slow assertion and mask a missing synchronous path).
        node2.query(f"SYSTEM ENABLE FAILPOINT {gate}")
        try:
            node1.query(f"INSERT INTO {table} VALUES (1, 'a')")

            # Confirms node2's watcher genuinely got triggered by the insert (via its Keeper
            # watch) and is now stuck at the gate -- i.e. it would *not* have caught up on its
            # own within this test, so the assertions below aren't just a timing fluke.
            _wait_failpoint_paused(node2, gate)

            # Without the setting, node2 genuinely does not see the row yet: the local cache is
            # stale and the background watcher that would normally refresh it is blocked.
            assert node2.query(f"SELECT count() FROM {table}").strip() == "0"

            # With the setting, read() does its own direct, synchronous catch-up -- a separate
            # call to updatePartSetFromKeeper() than the one the gate above is blocking -- and
            # sees the row despite the background watcher still being completely stuck.
            assert (
                node2.query(f"SELECT count() FROM {table} SETTINGS select_sequential_consistency = 1").strip()
                == "1"
            )
        finally:
            node2.query(f"SYSTEM DISABLE FAILPOINT {gate}")

        # Sanity: with the gate released, the background watcher also converges normally.
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "1")
    finally:
        node2.query(f"SYSTEM DISABLE FAILPOINT {gate}")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def _merge_selection_attempts(node):
    # CloudMergeTreeMergeSelectionAttempts is incremented once per scheduleDataProcessingJob()
    # cycle that passes the per-AZ leader gate (see StorageCloudMergeTree.cpp) -- system.events
    # only has a row for an event once it has fired at least once on this node.
    result = node.query(
        "SELECT value FROM system.events WHERE event = 'CloudMergeTreeMergeSelectionAttempts'"
    ).strip()
    return int(result) if result else 0


def _attach_shared_table(creator, attacher, ddl=TABLE_DDL, table=TABLE_NAME):
    creator.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = creator.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    attacher.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")


def test_az_leader_election_only_one_leader_per_az(cluster):
    node_a1 = cluster.instances["node_az_a1"]
    node_a2 = cluster.instances["node_az_a2"]

    _attach_shared_table(node_a1, node_a2)
    try:
        # A few separate parts so the background scheduler has something to consider every cycle,
        # not just idle-poll ticks.
        for i in range(1, 4):
            node_a1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

        before_a1 = _merge_selection_attempts(node_a1)
        before_a2 = _merge_selection_attempts(node_a2)

        # Comfortably longer than cloud_merge_tree_az_leader_recheck_ms's 5s default and the
        # scheduler's own much-shorter idle-poll interval.
        time.sleep(8)

        attempted_a1 = _merge_selection_attempts(node_a1) > before_a1
        attempted_a2 = _merge_selection_attempts(node_a2) > before_a2

        # Same-AZ replicas: exactly one must be attempting selection, the other's count must stay
        # completely flat (not just "attempted less") -- proves the gate actually skips selection
        # on the non-leader rather than merely losing the lease race more often.
        assert attempted_a1 != attempted_a2, (
            f"expected exactly one of node_az_a1/node_az_a2 to attempt merge selection, "
            f"got attempted_a1={attempted_a1}, attempted_a2={attempted_a2}"
        )
    finally:
        node_a1.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")
        node_a2.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")


def test_az_leader_failover_on_restart(cluster):
    node_a1 = cluster.instances["node_az_a1"]
    node_a2 = cluster.instances["node_az_a2"]

    _attach_shared_table(node_a1, node_a2)
    try:
        for i in range(1, 4):
            node_a1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

        before_a1 = _merge_selection_attempts(node_a1)
        before_a2 = _merge_selection_attempts(node_a2)
        time.sleep(8)
        attempted_a1 = _merge_selection_attempts(node_a1) > before_a1
        attempted_a2 = _merge_selection_attempts(node_a2) > before_a2
        assert attempted_a1 != attempted_a2, "expected exactly one leader before restart"

        leader, follower = (node_a1, node_a2) if attempted_a1 else (node_a2, node_a1)

        # The leader's ephemeral election node dies with its session on restart (or is removed
        # explicitly by shutdown()'s own best-effort cleanup for a clean stop) -- either way,
        # leadership must move to the surviving same-AZ replica, not stay stuck orphaned.
        leader.restart_clickhouse()

        before_follower = _merge_selection_attempts(follower)
        time.sleep(8)
        after_follower = _merge_selection_attempts(follower)
        assert after_follower > before_follower, (
            "surviving same-AZ replica did not take over merge-selection leadership "
            "after the previous leader restarted"
        )
    finally:
        node_a1.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")
        node_a2.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")


def test_az_leader_multi_az_no_global_bottleneck(cluster):
    node_a1 = cluster.instances["node_az_a1"]
    node_b1 = cluster.instances["node_az_b1"]

    _attach_shared_table(node_a1, node_b1)
    try:
        for i in range(1, 4):
            node_a1.query(f"INSERT INTO {TABLE_NAME} VALUES ({i}, 'v{i}')")

        before_a1 = _merge_selection_attempts(node_a1)
        before_b1 = _merge_selection_attempts(node_b1)
        time.sleep(8)

        # Each is the sole replica of its own AZ, so each must be its own AZ's leader
        # simultaneously -- confirms this is genuinely per-AZ, not a single global leader
        # excluding replicas in other AZs.
        assert _merge_selection_attempts(node_a1) > before_a1
        assert _merge_selection_attempts(node_b1) > before_b1
    finally:
        node_a1.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")
        node_b1.query(f"DROP TABLE IF EXISTS {TABLE_NAME} SYNC")


def test_lightweight_delete_removes_matching_rows(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = TABLE_NAME

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(
        f"ATTACH TABLE {table} UUID '{table_uuid}' (id UInt64, data String) "
        f"ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"
    )
    try:
        for i in range(1, 6):
            node1.query(f"INSERT INTO {table} VALUES ({i}, 'v{i}')")

        # DELETE FROM ... WHERE (lightweight delete) is implemented as a mutation under the
        # hood (lightweight_delete_mode defaults to ALTER_UPDATE) -- the same Keeper-backed
        # mutation path already proven by Phase 4's own tests. The mutation itself always
        # applied correctly; the bug this test guards against was in getMutationsSnapshot()
        # (see its own doc comment) silently telling supportsTrivialCountOptimization() that no
        # lightweight-delete mask existed, so SELECT count()'s fast path kept returning the
        # pre-delete row count forever even though the mutation had genuinely completed.
        node1.query(f"DELETE FROM {table} WHERE id IN (2, 4)")

        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "3")
        assert (
            node1.query(f"SELECT arraySort(groupArray(id)) FROM {table}").strip()
            == "[1,3,5]"
        )

        # The mutation commits through Keeper like any other -- the second replica's watcher
        # picks it up without ever running the DELETE itself.
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "3")
        assert (
            node2.query(f"SELECT arraySort(groupArray(id)) FROM {table}").strip()
            == "[1,3,5]"
        )
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


LWU_TABLE_DDL_SETTINGS = (
    "storage_policy = 's3', enable_block_number_column = 1, enable_block_offset_column = 1"
)


def test_lightweight_update_applies_and_visible_cross_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = TABLE_NAME
    ddl = f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id SETTINGS {LWU_TABLE_DDL_SETTINGS}"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(20)")

        # UPDATE ... SET (lightweight update) writes a "patch part" -- a normal
        # DataPartKind::Patch part carrying only the updated column(s), committed through the
        # exact same commitInsertedPart() Keeper-commit hook INSERT already uses (see
        # CloudMergeTreeSinkPatch) -- rather than rewriting the base part like a heavy mutation.
        node1.query(
            f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
            f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
        )

        # sum(0..19) = 190; even ids get replaced by their square, odd ids keep c1 unchanged
        # (c2 stays equal to id for odd ids). sum(i*i for even i in 0..18) + sum(odd i in 1..19).
        expected = sum(i * i for i in range(0, 20, 2)) + sum(i for i in range(1, 20, 2))
        assert node1.query(f"SELECT sum(c2) FROM {table}").strip() == str(expected)

        # A patch part exists and is registered in Keeper under its own synthetic
        # 'patch-<hash>-<partition_id>' partition (see PatchPartInfo.h's doc comment).
        assert (
            int(node1.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'").strip())
            > 0
        )

        # The patch part commits through Keeper like any other -- the second replica's watcher
        # picks it up and applies it on read without ever running the UPDATE itself.
        assert_eq_with_retry(node2, f"SELECT sum(c2) FROM {table}", str(expected))
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lightweight_update_concurrent_from_both_replicas_both_apply(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = TABLE_NAME
    ddl = f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id SETTINGS {LWU_TABLE_DDL_SETTINGS}"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(20)")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "20")

        # Two DIFFERENT replicas issue UPDATEs on disjoint row subsets of the SAME partition at
        # the same wall-clock time -- two genuinely concurrent CloudMergeTreeSinkPatch commits,
        # each independently allocating its own Keeper block number (data version) for the same
        # partition's block_numbers/ sequence, each going through getLockForLightweightUpdateInKeeper
        # (default update_parallel_mode=AUTO: since both target column c2, the affected-columns
        # conflict check serializes them at the Keeper lock rather than truly overlapping -- this
        # is the correct, expected AUTO-mode behavior, not a test artifact) and each independently
        # committing its own patch part via commitInsertedPart(), the same Keeper-first commit path
        # every other part kind in this engine already uses.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    node1.query,
                    f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
                    f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1",
                ),
                executor.submit(
                    node2.query,
                    f"UPDATE {table} SET c2 = c1 * 1000 WHERE id % 2 = 1 "
                    f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1",
                ),
            ]
            for future in futures:
                future.result()

        expected = sum(i * i for i in range(0, 20, 2)) + sum(i * 1000 for i in range(1, 20, 2))
        assert_eq_with_retry(node1, f"SELECT sum(c2) FROM {table}", str(expected))
        assert_eq_with_retry(node2, f"SELECT sum(c2) FROM {table}", str(expected))

        # At least one patch part exists (proves both commits genuinely landed and neither
        # silently collided on Keeper block-number allocation or the DETACH-guard/absorption-GC
        # machinery -- if either commit had been lost, sum(c2) above would already have caught it,
        # this is a second, more direct signal). Not asserting an exact count: automatic patch-to-patch compaction (see test_lightweight_update_many_small_updates_
        # merge_into_one_patch) can legitimately have already merged the two small patches this
        # test produces into one by the time this check runs -- that's the feature working
        # correctly, not a race to guard against here.
        assert (
            int(node1.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'").strip())
            >= 1
        )
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lightweight_update_dedup_disabled_identical_updates_both_apply(cluster):
    node1 = cluster.instances["node1"]
    table = TABLE_NAME
    ddl = f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id SETTINGS {LWU_TABLE_DDL_SETTINGS}"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(10)")

        # Two UPDATE statements producing byte-identical patch content (same predicate, same
        # SET, no data changed in between) must both commit as separate patch parts, not
        # dedup-collide on the second one -- see CloudMergeTreeSinkPatch's own doc comment on
        # why deduplication_hashes is always empty for patch parts.
        for _ in range(2):
            node1.query(
                f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
                f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
            )

        patch_part_count = int(
            node1.query(
                f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'"
            ).strip()
        )
        assert patch_part_count == 2, (
            f"expected both identical UPDATEs to commit as separate patch parts, got {patch_part_count}"
        )
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lightweight_update_many_small_updates_merge_into_one_patch(cluster):
    node1 = cluster.instances["node1"]
    table = TABLE_NAME
    ddl = f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id SETTINGS {LWU_TABLE_DDL_SETTINGS}"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(30)")

        # Each UPDATE targets a disjoint slice of rows, so every one of them produces its own
        # patch part in the same synthetic patch partition -- CloudMergeTreePartsCollector's
        # patch-scoped selection path (StorageCloudMergeTree::selectPartsToMerge()'s second
        # attempt) should compact them into one via ordinary background merge selection, the
        # same way it compacts regular parts, without any explicit OPTIMIZE.
        for i in range(6):
            node1.query(
                f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 6 = {i} "
                f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
            )

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'",
            "1",
            retry_count=60,
            sleep_time=1,
        )

        expected = sum(i * i for i in range(30))
        assert node1.query(f"SELECT sum(c2) FROM {table}").strip() == str(expected)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lightweight_update_drop_partition_removes_patch_cascade(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = TABLE_NAME
    ddl = (
        f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id "
        f"PARTITION BY id % 2 SETTINGS {LWU_TABLE_DDL_SETTINGS}"
    )

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(20)")
        node1.query(
            f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
            f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
        )
        assert (
            int(node1.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'").strip())
            > 0
        )

        # DROP PARTITION 0 must synchronously tombstone both the regular parts of partition 0
        # AND the patch part covering it -- see dropPartition()'s partitionIdMatchesOrIsPatchOf
        # predicate -- so no orphaned patch znode is left behind with no surviving regular part
        # in its original partition to ever absorb it.
        node1.query(f"ALTER TABLE {table} DROP PARTITION 0")

        assert node1.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'").strip() == "0"
        assert node1.query(f"SELECT count() FROM {table} WHERE id % 2 = 0").strip() == "0"
        # Partition 1 (odd ids), never targeted by the UPDATE or the DROP, is untouched.
        assert node1.query(f"SELECT count() FROM {table} WHERE id % 2 = 1").strip() == "10"

        # The cascade-drop is a normal Keeper removal -- the second replica's watcher picks it
        # up without running anything itself.
        assert_eq_with_retry(node2, f"SELECT count() FROM {table} WHERE id % 2 = 0", "0")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table} WHERE id % 2 = 1", "10")
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lightweight_update_detach_partition_rejects_with_unabsorbed_patch(cluster):
    node1 = cluster.instances["node1"]
    table = TABLE_NAME
    ddl = f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id SETTINGS {LWU_TABLE_DDL_SETTINGS}"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(10)")
        node1.query(
            f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
            f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
        )
        expected = sum(i * i for i in range(0, 10, 2)) + sum(i for i in range(1, 10, 2))
        assert node1.query(f"SELECT sum(c2) FROM {table}").strip() == str(expected)

        # Only the base part would be detached; the pending patch would be left behind, so a
        # later re-ATTACH would silently revert the update for its rows -- matches vanilla's own
        # StorageMergeTree/StorageReplicatedMergeTree rejection (assertNoPatchesForParts) for the
        # identical scenario, reused unchanged here.
        with pytest.raises(QueryRuntimeException, match="SUPPORT_IS_DISABLED"):
            node1.query(f"ALTER TABLE {table} DETACH PARTITION ID 'all'")
        assert node1.query(f"SELECT sum(c2) FROM {table}").strip() == str(expected)

        # Once the patch is fully absorbed (OPTIMIZE TABLE FINAL merges every regular part past
        # its max_data_version -- same mechanism test_lightweight_update_absorption_gc_removes_
        # patch_after_merge exercises), DETACH/ATTACH is a completely ordinary roundtrip again.
        node1.query(f"OPTIMIZE TABLE {table} FINAL")
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'",
            "0",
            retry_count=60,
            sleep_time=1,
        )

        node1.query(f"ALTER TABLE {table} DETACH PARTITION ID 'all'")
        assert node1.query(f"SELECT count() FROM {table}").strip() == "0"

        node1.query(f"ALTER TABLE {table} ATTACH PARTITION ID 'all'")
        assert node1.query(f"SELECT sum(c2) FROM {table}").strip() == str(expected)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_lightweight_update_replace_partition_rejects_when_unabsorbed_patch(cluster):
    node1 = cluster.instances["node1"]
    src_table = "cloud_test_lwu_replace_src"
    dst_table = "cloud_test_lwu_replace_dst"
    ddl = (
        f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id "
        f"PARTITION BY id % 2 SETTINGS {LWU_TABLE_DDL_SETTINGS}"
    )

    node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")
    node1.query(f"CREATE TABLE {src_table} {ddl}")
    node1.query(f"CREATE TABLE {dst_table} {ddl}")
    try:
        node1.query(f"INSERT INTO {src_table} SELECT number, number, number FROM numbers(10)")
        node1.query(
            f"UPDATE {src_table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
            f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
        )
        assert (
            int(node1.query(f"SELECT count() FROM system.parts WHERE table = '{src_table}' AND active AND partition_id LIKE 'patch-%'").strip())
            > 0
        )

        # v1 scope cut (see replacePartitionFrom()'s own doc comment): cross-table partition
        # transfer with an unabsorbed patch pending in the source partition is rejected
        # outright rather than silently orphaning it.
        with pytest.raises(QueryRuntimeException, match="NOT_IMPLEMENTED"):
            node1.query(f"ALTER TABLE {dst_table} REPLACE PARTITION 0 FROM {src_table}")
    finally:
        node1.query(f"DROP TABLE IF EXISTS {src_table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {dst_table} SYNC")


TTL_TABLE_DDL_SETTINGS = "storage_policy = 's3', merge_with_ttl_timeout = 0"


def test_ttl_delete_removes_expired_rows_and_visible_on_second_replica(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = TABLE_NAME
    ddl = (
        f"(id UInt64, ts DateTime, data String) ENGINE = CloudMergeTree ORDER BY id "
        f"TTL ts + INTERVAL 1 DAY SETTINGS {TTL_TABLE_DDL_SETTINGS}"
    )

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        # Rows 1-3 already satisfy ts + INTERVAL 1 DAY <= now() at insert time (no need to
        # actually wait out the interval). Rows 4-5 are inserted with ts = now() and must survive
        # for the entire test -- a 1-day window (not 1 second) means normal scheduler/test
        # latency (observed up to ~1.5s for a background merge to actually run) can never
        # accidentally cross the threshold and expire them too, unlike a too-short TTL window.
        node1.query(
            f"INSERT INTO {table} VALUES "
            f"(1, now() - INTERVAL 2 DAY, 'old1'), "
            f"(2, now() - INTERVAL 2 DAY, 'old2'), "
            f"(3, now() - INTERVAL 2 DAY, 'old3')"
        )
        node1.query(f"INSERT INTO {table} VALUES (4, now(), 'fresh4'), (5, now(), 'fresh5')")

        # Before this fix, merge_with_ttl_allowed was hardcoded false, so no background merge
        # ever considered TTL -- these rows would stay forever. Reproduce-first: this assertion
        # fails against the pre-fix hardcode (count stays 5) and passes after (count becomes 2).
        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "2")
        assert (
            node1.query(f"SELECT arraySort(groupArray(id)) FROM {table}").strip()
            == "[4,5]"
        )

        # TTL-driven deletion commits through the same commitMergedPart() Keeper path as any
        # other merge -- the second replica's watcher picks it up without running anything itself.
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "2")
        assert (
            node2.query(f"SELECT arraySort(groupArray(id)) FROM {table}").strip()
            == "[4,5]"
        )
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_ttl_stop_start_merges_blocks_and_resumes_ttl_deletion(cluster):
    node1 = cluster.instances["node1"]
    table = TABLE_NAME
    ddl = (
        f"(id UInt64, ts DateTime, data String) ENGINE = CloudMergeTree ORDER BY id "
        f"TTL ts + INTERVAL 1 DAY SETTINGS {TTL_TABLE_DDL_SETTINGS}"
    )

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    try:
        node1.query("SYSTEM STOP TTL MERGES " + table)
        # 1-day window (not 1 second): the fresh row's ts = now() at insert time must not cross
        # the TTL threshold no matter how long this test takes to run (including the STOP-then-
        # sleep-then-START sequence below) -- same reasoning as the sibling delete test.
        node1.query(
            f"INSERT INTO {table} VALUES (1, now() - INTERVAL 2 DAY, 'old1'), (2, now(), 'fresh2')"
        )

        # Give the background scheduler several cycles' worth of time to (not) act -- the row
        # must still be there, proving the independent PartsTTLMerge blocker actually blocks TTL
        # selection specifically (SYSTEM STOP MERGES's own separate PartsMerge blocker is
        # untouched here, so this also confirms the two blockers don't accidentally alias).
        time.sleep(3)
        assert node1.query(f"SELECT count() FROM {table}").strip() == "2"

        node1.query("SYSTEM START TTL MERGES " + table)
        assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "1")
        assert node1.query(f"SELECT id FROM {table}").strip() == "2"
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
