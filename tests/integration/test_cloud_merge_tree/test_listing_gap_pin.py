"""
Deterministic regression test for the listing-gap part-eviction bug, and for the
Keeper-authoritative part-location design that structurally closes it.

plain_rewritable rebuilds its in-memory directory tree from a full object storage listing on
every refresh; a backend whose listing transiently omits a just-written (or even long-existing)
object used to get that directory evicted from the tree, turning a healthy, already-adopted
CloudMergeTree part into FILE_DOESNT_EXIST on a real SELECT -- and, before this design, adoption
of a NEW part depended on the listing catching up at all.

Every part znode payload now carries the part's remote directory token(s) and complete file
list (see CloudPartLocation) alongside its header, committed atomically with the part's
registration. Adoption applies this as an authoritative override on the disk's in-memory tree
(IMetadataStorage::setAuthoritativeDirectory) instead of waiting on the listing, so a part can
be adopted -- not just stay readable -- even while the listing is completely blind.

The failpoint `plain_rewritable_listing_returns_empty` simulates that in its most extreme form
-- every refresh sees a completely empty listing -- so this test is deterministic, not
timing-dependent.
"""

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

TABLE = "listing_gap_pin"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "gap1",
            main_configs=["configs/config.d/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "gap2",
            main_configs=["configs/config.d/storage_conf.xml"],
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_parts_adopted_and_survive_listing_gap(cluster):
    node1 = cluster.instances["gap1"]
    node2 = cluster.instances["gap2"]

    node1.query(f"DROP TABLE IF EXISTS {TABLE} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {TABLE} SYNC")
    node1.query(
        f"CREATE TABLE {TABLE} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS storage_policy = 's3'"
    )
    uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{TABLE}'").strip()
    node2.query(
        f"ATTACH TABLE {TABLE} UUID '{uuid}' (id UInt64, data String) "
        f"ENGINE = CloudMergeTree ORDER BY id SETTINGS storage_policy = 's3'"
    )

    node1.query(f"INSERT INTO {TABLE} SELECT number, toString(number) FROM numbers(100000)")
    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE}", "100000", retry_count=60, sleep_time=1)

    # From now on, every plain_rewritable refresh on node2 sees an EMPTY listing. A part not
    # protected by an authoritative override would be evicted from the in-memory tree on the
    # next refresh, and a NEW part could never be adopted at all under the predecessor
    # listing-driven design.
    node2.query("SYSTEM ENABLE FAILPOINT plain_rewritable_listing_returns_empty")
    try:
        for i in range(3):
            marker_id = 1000000 + i
            node1.query(f"INSERT INTO {TABLE} VALUES ({marker_id}, 'marker')")

            # The already-adopted part must stay fully readable throughout -- this SELECT threw
            # FILE_DOESNT_EXIST (or undercounted) before the authoritative-override fix.
            node2.query("SYSTEM DROP MARK CACHE")
            node2.query("SYSTEM DROP UNCOMPRESSED CACHE")
            node2.query("SYSTEM DROP FILESYSTEM CACHE")
            assert node2.query(f"SELECT count() FROM {TABLE} WHERE id < 1000000").strip() == "100000", (
                "previously-adopted part became unreadable during a listing gap"
            )

            # The marker part must be ADOPTED despite the listing being completely blind:
            # adoption resolves the part's location straight from its Keeper znode, never from
            # the listing this failpoint blinds.
            assert_eq_with_retry(
                node2, f"SELECT count() FROM {TABLE} WHERE id = {marker_id}", "1",
                retry_count=30, sleep_time=1,
            )
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT plain_rewritable_listing_returns_empty")

    assert_eq_with_retry(node2, f"SELECT count() FROM {TABLE}", "100003", retry_count=60, sleep_time=1)

    node1.query(f"DROP TABLE IF EXISTS {TABLE} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {TABLE} SYNC")
