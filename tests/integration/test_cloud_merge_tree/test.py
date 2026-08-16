import logging

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

    # Single-writer INSERT only: block-number allocation is still process-local in Phase 1
    # (Keeper-side fencing across concurrent writers lands in Phase 2 with merges/leases), so
    # this test does not insert concurrently from both replicas.
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
