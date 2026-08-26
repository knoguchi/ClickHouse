import logging
import time
from concurrent.futures import ThreadPoolExecutor, wait

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

# Split out of test.py: these tests wait on the parts-killer's dropped_parts/grace-period cleanup,
# which piggybacks on the shared BackgroundSchedulePool -- keeping them in the same 90-test,
# module-scoped-cluster file as everything else meant their wall-clock assertions raced whatever
# cumulative background-task load the rest of that file had built up by the time they ran (see
# test_patch_gc_absorption_check_pause_does_not_corrupt_concurrent_gc's own history). A dedicated,
# much smaller module keeps that load bounded to just these tests, the same way test_backup_restore_new
# splits test_cancel_backup.py/test_shutdown_wait_backup.py out of its own test.py.

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


def table_object_names(cluster, table_uuid):
    # The set of object names belonging to one table, resolved through plain_rewritable's own
    # layout: every "data/__meta/<token>/prefix.path" object holds the local directory path the
    # token stands for, and a table's local paths all contain its UUID ("store/xxx/<uuid>/...").
    # Needed because tokens are random -- a bucket-wide object count is NOT scoped to a table, and
    # other tests' async cleanup (or GC) removing THEIR objects must not affect assertions about
    # this table's objects.
    minio = cluster.minio_client
    names = [obj.object_name for obj in list_objects(cluster)]
    tokens = set()
    for name in names:
        parts = name.split("/")
        if len(parts) == 4 and parts[1] == "__meta" and parts[3] == "prefix.path":
            content = minio.get_object(cluster.minio_bucket, name).read().decode()
            if table_uuid in content:
                tokens.add(parts[2])
    owned = set()
    for name in names:
        parts = name.split("/")
        token = parts[2] if len(parts) > 2 and parts[1] == "__meta" else parts[1]
        if token in tokens:
            owned.add(name)
    return owned


LWU_TABLE_DDL_SETTINGS = (
    "storage_policy = 's3', enable_block_number_column = 1, enable_block_offset_column = 1"
)


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
        # commit (README.md invariant 3 is unaffected -- this just rides more ops in the same
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
        # liveness gap is a known, documented limitation of this phase (see README.md
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

        # Scoped to THIS table's objects (see table_object_names): a bucket-wide count also sees
        # other tests' leftovers being cleaned up asynchronously during the sleep below, which is
        # irrelevant to the invariant under test and made this assertion flaky.
        owned_before = table_object_names(cluster, table_uuid)
        assert len(owned_before) > 0

        node1.query(f"ALTER TABLE {table} DETACH PARTITION ID 'all'")

        # Well past grace_period_seconds + gc_interval_ms (see GC_TABLE_DDL_SETTINGS) and several
        # parts-killer cycles -- the detached part's objects must never be touched, since they're
        # recorded under detached_parts/, a namespace the GC scan never reads. This is the core
        # invariant this whole feature exists to protect.
        time.sleep(20)
        names_after_grace_period = {obj.object_name for obj in list_objects(cluster)}
        missing = owned_before - names_after_grace_period
        assert not missing, f"detached table's objects were deleted during the grace period: {sorted(missing)}"

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


def test_drop_table_after_detach_reclaims_detached_parts_objects(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "cloud_test_drop_after_detach_gc"

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(
        f"CREATE TABLE {table} (id UInt64, data String) ENGINE = CloudMergeTree "
        f"ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(
        f"ATTACH TABLE {table} UUID '{table_uuid}' (id UInt64, data String) "
        f"ENGINE = CloudMergeTree ORDER BY id SETTINGS {GC_TABLE_DDL_SETTINGS}"
    )
    try:
        node1.query(f"INSERT INTO {table} VALUES (1, 'a')")
        assert_eq_with_retry(node2, f"SELECT count() FROM {table}", "1")

        # A detached part is deliberately exempt from the parts-killer's normal scan (recorded
        # under detached_parts/, not dropped_parts/ -- see
        # test_detached_part_survives_grace_period_and_can_be_reattached, which checks the
        # opposite invariant: a detached part's objects must NOT be collected while the table is
        # still alive). But once the whole table is DROPped, nothing will ever re-ATTACH or
        # manually clean up that part again -- its objects must not be orphaned forever just
        # because they happened to be sitting under detached_parts/ instead of parts/ at the
        # moment DROP TABLE ran.
        node1.query(f"ALTER TABLE {table} DETACH PARTITION ID 'all'")

        # Drop only on node1 -- node2 keeps its own StorageCloudMergeTree object (and
        # parts-killer task) alive to actually perform the physical/Keeper cleanup below. If
        # every replica dropped the table, no live GC task would remain to drain it (same
        # documented liveness gap as test_drop_table_objects_survive_grace_period_then_get_collected).
        # Also: node1's own DROP TABLE SYNC removes its local copy of the data directory
        # synchronously via DatabaseCatalog's generic teardown -- unrelated to this engine's own
        # Keeper-side detached_parts/ znode, which only node2's still-running parts-killer task
        # (via the trailing table-directory teardown in its GC loop) can ever clean up.
        node1.query(f"DROP TABLE {table} SYNC")

        # Past grace_period_seconds + gc_interval_ms (see GC_TABLE_DDL_SETTINGS) and several
        # parts-killer cycles on node2 -- the detached_parts/ znode itself must be cleaned up, not
        # just the objects it referred to, otherwise repeated DETACH-then-DROP-TABLE cycles
        # accumulate orphaned znodes in Keeper forever.
        assert_eq_with_retry(
            node2,
            f"SELECT count() FROM system.zookeeper WHERE path = "
            f"'/clickhouse/cloud_tables/{table_uuid}/detached_parts'",
            "0",
            retry_count=30,
            sleep_time=1,
        )
    finally:
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


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


def test_lightweight_update_absorption_gc_removes_patch_after_merge(cluster):
    node1 = cluster.instances["node1"]
    table = TABLE_NAME
    ddl = (
        f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id "
        f"SETTINGS {LWU_TABLE_DDL_SETTINGS}, cloud_merge_tree_gc_grace_period_seconds = 5, "
        f"cloud_merge_tree_gc_interval_ms = 1000"
    )

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(10)")
        node1.query(
            f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
            f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
        )
        assert (
            int(node1.query(f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'").strip())
            == 1
        )

        # OPTIMIZE TABLE FINAL merges every regular part into one whose data_version is >= the
        # patch's own max_data_version -- runPartsKillerCycle()'s absorption-GC pass should then
        # find every active regular part in the partition already past the patch's
        # max_data_version and tombstone it. No explicit trigger needed: the low
        # cloud_merge_tree_gc_interval_ms above means the next background cycle picks it up on
        # its own, same as any other GC-driven removal in this suite.
        node1.query(f"OPTIMIZE TABLE {table} FINAL")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'",
            "0",
            retry_count=60,
            sleep_time=1,
        )

        expected = sum(i * i for i in range(0, 10, 2)) + sum(i for i in range(1, 10, 2))
        assert node1.query(f"SELECT sum(c2) FROM {table}").strip() == str(expected)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_patch_gc_absorption_check_pause_does_not_corrupt_concurrent_gc(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = TABLE_NAME
    failpoint = "cloud_merge_tree_patch_gc_absorption_check_pause"
    ddl = (
        f"(id UInt64, c1 UInt64, c2 UInt64) ENGINE = CloudMergeTree ORDER BY id "
        f"SETTINGS {LWU_TABLE_DDL_SETTINGS}, cloud_merge_tree_gc_grace_period_seconds = 5, "
        f"cloud_merge_tree_gc_interval_ms = 1000"
    )

    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node1.query(f"CREATE TABLE {table} {ddl}")
    table_uuid = node1.query(f"SELECT uuid FROM system.tables WHERE table = '{table}'").strip()
    node2.query(f"ATTACH TABLE {table} UUID '{table_uuid}' {ddl}")
    try:
        node1.query(f"INSERT INTO {table} SELECT number, number, number FROM numbers(10)")
        node1.query(
            f"UPDATE {table} SET c2 = c1 * c1 WHERE id % 2 = 0 "
            f"SETTINGS enable_lightweight_update = 1, apply_patch_parts = 1"
        )
        # Park node1's absorption-GC cycle right after it has read the patch's trailer (its
        # committed max_data_version) but before the tombstone decision -- proves the decision
        # made from that single Keeper-fresh read stays self-consistent even while further,
        # unrelated Keeper activity (plus an ordinary concurrent INSERT below) continues in
        # parallel.
        #
        # Enabled on BOTH nodes, and BEFORE the OPTIMIZE below. This ordering is load-bearing,
        # and was the historical source of this test's flakiness (which no wait-timeout increase
        # could ever fix): the absorption pause fires only while an active patch exists, and once
        # OPTIMIZE makes the patch absorbable, ANY GC cycle -- node1's in the gap before a later
        # ENABLE FAILPOINT took effect, or node2's, whose failpoint was never enabled at all
        # (is_az_leader defaults to true for both nodes here, neither has AZ placement
        # configured, so both run absorption GC) -- could consume the patch first. After that the
        # pause line is unreachable forever and the wait below necessarily times out. Arming both
        # nodes first makes reaching the pause deterministic: whichever replica's cycle touches
        # the patch parks instead of absorbing it.
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node2.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")
        try:
            # Generous timeout purely for host load: a saturated CI host can delay the 1s
            # cloud_merge_tree_gc_interval_ms tick by a lot of wall clock. Reaching the pause at
            # all is guaranteed by the arm-before-OPTIMIZE ordering above.
            _wait_failpoint_paused(node1, failpoint, timeout=300)

            # Ordinary, unrelated write activity while node1's GC decision is parked -- must not
            # be disturbed by, or disturb, the paused GC cycle.
            node2.query(f"INSERT INTO {table} VALUES (100, 1, 1)")
            assert_eq_with_retry(node1, f"SELECT count() FROM {table}", "11")

            node1.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            node2.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND partition_id LIKE 'patch-%'",
            "0",
            retry_count=60,
            sleep_time=1,
        )

        expected = sum(i * i for i in range(0, 10, 2)) + sum(i for i in range(1, 10, 2)) + 1
        assert_eq_with_retry(node1, f"SELECT sum(c2) FROM {table}", str(expected))
        assert_eq_with_retry(node2, f"SELECT sum(c2) FROM {table}", str(expected))
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node2.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
