#!/usr/bin/env python3

import time
import uuid

from pymongo import MongoClient, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


# ============================================================
# Replica Set
# ============================================================

URI = (
    "mongodb://"
    "172.20.10.2:27017,"
    "172.20.10.3:27017,"
    "172.20.10.5:27017/"
    "?replicaSet=rs0"
)

DB_NAME = "consistency_test"
COLLECTION_NAME = "ryw_test"

N = 200


# ============================================================
# Connect
# ============================================================

client = MongoClient(
    URI,
    serverSelectionTimeoutMS=5000
)

# Force topology discovery
client.admin.command("ping")

print("Connected to replica set")
print("Primary:    ", client.primary)
print("Secondaries:", client.secondaries)
print()


db = client[DB_NAME]


# ============================================================
# Test A
#
# NO causal consistency
# writeConcern = 1
# readConcern  = local
# reads forced to SECONDARY
#
# This configuration does NOT guarantee RYW.
# ============================================================

weak_collection = db.get_collection(
    COLLECTION_NAME,
    read_preference=ReadPreference.SECONDARY,
    read_concern=ReadConcern("local"),
    write_concern=WriteConcern(w=1),
)


def test_without_causal_consistency():

    print("=" * 60)
    print("TEST A: no causal consistency")
    print("writeConcern = w:1")
    print("readConcern  = local")
    print("readPreference = secondary")
    print("=" * 60)

    run_id = str(uuid.uuid4())

    stale_reads = 0
    latencies = []

    for i in range(N):

        doc_id = f"{run_id}-{i}"

        document = {
            "_id": doc_id,
            "run_id": run_id,
            "seq": i,
            "value": f"value-{i}"
        }

        # ----------------------------------------------
        # WRITE
        # Goes to primary
        # ----------------------------------------------

        weak_collection.insert_one(document)

        # ----------------------------------------------
        # Immediately READ
        # Forced to a secondary
        # ----------------------------------------------

        start = time.perf_counter()

        result = weak_collection.find_one(
            {"_id": doc_id},
            max_time_ms=5000
        )

        latency_ms = (time.perf_counter() - start) * 1000
        latencies.append(latency_ms)

        # Secondary has not replicated the write yet
        if result is None:

            stale_reads += 1

            print(
                f"[STALE] iteration={i:03d} "
                f"document not visible on secondary"
            )

    print()
    print(f"Total operations : {N}")
    print(f"Stale reads      : {stale_reads}")
    print(f"Successful reads : {N - stale_reads}")
    print(
        f"Average latency  : "
        f"{sum(latencies) / len(latencies):.2f} ms"
    )

    if stale_reads > 0:
        print()
        print("RESULT: RYW violation observed.")
    else:
        print()
        print(
            "RESULT: No stale read observed, but this "
            "configuration does NOT guarantee RYW."
        )

    print()

    return run_id


# ============================================================
# Test B
#
# causal session
# writeConcern = majority
# readConcern  = majority
# reads forced to SECONDARY
#
# This should guarantee read-your-own-writes.
# ============================================================

strong_collection = db.get_collection(
    COLLECTION_NAME,
    read_preference=ReadPreference.SECONDARY,
    read_concern=ReadConcern("majority"),
    write_concern=WriteConcern(
        w="majority",
        wtimeout=10000
    ),
)


def test_with_causal_consistency():

    print("=" * 60)
    print("TEST B: causally consistent session")
    print("writeConcern = majority")
    print("readConcern  = majority")
    print("readPreference = secondary")
    print("=" * 60)

    run_id = str(uuid.uuid4())

    failures = 0
    latencies = []

    # --------------------------------------------------------
    # The important part:
    #
    # causal_consistency=True
    # --------------------------------------------------------

    with client.start_session(
        causal_consistency=True
    ) as session:

        for i in range(N):

            doc_id = f"{run_id}-{i}"

            document = {
                "_id": doc_id,
                "run_id": run_id,
                "seq": i,
                "value": f"value-{i}"
            }

            # ----------------------------------------------
            # WRITE
            # Sent to primary
            # majority acknowledged
            # ----------------------------------------------

            strong_collection.insert_one(
                document,
                session=session
            )

            # ----------------------------------------------
            # Immediately READ
            #
            # ReadPreference.SECONDARY forces the read
            # onto a secondary.
            #
            # The causal session carries the causal
            # dependency from the preceding write.
            # ----------------------------------------------

            start = time.perf_counter()

            result = strong_collection.find_one(
                {"_id": doc_id},
                session=session,
                max_time_ms=10000
            )

            latency_ms = (time.perf_counter() - start) * 1000
            latencies.append(latency_ms)

            if result is None:

                failures += 1

                print(
                    f"[FAIL] iteration={i:03d} "
                    f"write not visible"
                )

            elif result["value"] != f"value-{i}":

                failures += 1

                print(
                    f"[FAIL] iteration={i:03d} "
                    f"expected=value-{i}, "
                    f"got={result['value']}"
                )

    print()
    print(f"Total operations : {N}")
    print(f"Failures         : {failures}")
    print(f"Successful reads : {N - failures}")
    print(
        f"Average latency  : "
        f"{sum(latencies) / len(latencies):.2f} ms"
    )

    if failures == 0:
        print()
        print("RESULT: PASS - Read-your-writes maintained.")
    else:
        print()
        print("RESULT: FAIL - RYW violation detected.")

    print()

    return run_id


# ============================================================
# Main
# ============================================================

try:

    weak_run_id = test_without_causal_consistency()

    strong_run_id = test_with_causal_consistency()

finally:

    # Cleanup using majority write concern.
    cleanup_collection = db.get_collection(
        COLLECTION_NAME,
        write_concern=WriteConcern(w="majority")
    )

    cleanup_collection.delete_many({
        "run_id": {
            "$in": [
                weak_run_id if "weak_run_id" in locals() else "",
                strong_run_id if "strong_run_id" in locals() else ""
            ]
        }
    })

    client.close()
