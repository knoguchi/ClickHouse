import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

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

    assert (
        node1.query(
            f"SELECT count() FROM system.parts WHERE table = '{TABLE_NAME}' AND active"
        ).strip()
        == str(row_count)
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
