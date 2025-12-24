## About
This script was created after discovering that simultaneous writes to iceberg tables caused errors(`ICEBERG_COMMIT_ERROR`)
. Investigations suggested that modifying table properties could mitigate these issues. The script provides a reproducible way to test and validate such changes.


**Usage:**
- Run the script with an optional process ID argument.
- Adjust worker and epoch counts as needed.
- Ensure Trino and Iceberg are properly configured for concurrent operations.

**Context:**

**File:** `trino_iceberg_concurrency_writer.py`

Before any table modifications:

```python
# Create 1 process with 5 workers, each writing 5 epochs(2,500 rows total)
python 3 iceberg-concurrency-writer/trino_iceberg_concurrency_test.py 1
```

The above command started running and produced the following output:

```
Process 1, Worker 1 failed: TrinoExternalError(type=EXTERNAL, name=ICEBERG_COMMIT_ERROR, message="Failed to commit the transaction during insert: Metadata location [<s3-path>/metadata/01024-02cbe44c-8695-4f9e-afc6-b1ac5d7e0433.metadata.json] is not same as table metadata location [<s3-path>/metadata/01025-701a6206-820c-4eb1-9a57-4012c979ab50.metadata.json] for quality_checks.quality_check_results", query_id=20250922_175140_01809_qbntq)
Process 1, Worker 5 failed: TrinoExternalError(type=EXTERNAL, name=ICEBERG_COMMIT_ERROR, message="Failed to commit the transaction during insert: Metadata location [<s3-path>/quality_checks/quality_check_results/metadata/01024-02cbe44c-8695-4f9e-afc6-b1ac5d7e0433.metadata.json] is not same as table metadata location [<s3-path>/quality_checks/quality_check_results/metadata/01025-701a6206-820c-4eb1-9a57-4012c979ab50.metadata.json] for quality_checks.quality_check_results", query_id=20250922_175140_01810_qbntq)
Process 1, Worker 2 failed: TrinoExternalError(type=EXTERNAL, name=ICEBERG_COMMIT_ERROR, message="Failed to commit the transaction during insert: Metadata location [<s3-path>/metadata/01288-97c232ec-0183-4989-a84d-d5406cbff25e.metadata.json] is not same as table metadata location [<s3-path>/metadata/01289-f99c1513-a2b2-48d6-ab84-1da9d5e25ea0.metadata.json] for quality_checks.quality_check_results", query_id=20250922_175342_01954_qbntq)
Process 1, Worker 3: Completed epoch 1
Process 1, Worker 4: Completed epoch 1
Process 1, Worker 3: Completed epoch 2
Process 1, Worker 4: Completed epoch 2
Process 1, Worker 3: Completed epoch 3
Process 1, Worker 4: Completed epoch 3
Process 1, Worker 3: Completed epoch 4
Process 1, Worker 4: Completed epoch 4
Process 1, Worker 3: Completed epoch 5
Process 1, Worker 4: Completed epoch 5
```

Few workers failed with `ICEBERG_COMMIT_ERROR` while others succeeded.

To mitigate this, we modified the Iceberg table properties increasing the retry attempts for concurrent writes commits:

```sql
ALTER TABLE iceberg.quality_checks.quality_check_results
SET PROPERTIES max_commit_retry =20
```
After applying the above change, we reran the same command:

```python
# Create 1 process with 5 workers, each writing 5 epochs(2,500 rows total)
python 3 trino_iceberg_concurrency_writer.py 1
```
The command started running and produced the following output without any errors:

```

Starting Process 1 with 5 workers, each writing 5 epochs
Process 1, Worker 5: Completed epoch 1
Process 1, Worker 1: Completed epoch 1
Process 1, Worker 4: Completed epoch 1
Process 1, Worker 3: Completed epoch 1
Process 1, Worker 4: Completed epoch 2
Process 1, Worker 5: Completed epoch 2
Process 1, Worker 1: Completed epoch 2
Process 1, Worker 2: Completed epoch 1
Process 1, Worker 4: Completed epoch 3
Process 1, Worker 1: Completed epoch 3
Process 1, Worker 5: Completed epoch 3
Process 1, Worker 2: Completed epoch 2
Process 1, Worker 3: Completed epoch 2
Process 1, Worker 1: Completed epoch 4
Process 1, Worker 5: Completed epoch 4
Process 1, Worker 4: Completed epoch 4
Process 1, Worker 5: Completed epoch 5
Process 1, Worker 2: Completed epoch 3
Process 1, Worker 3: Completed epoch 3
Process 1, Worker 2: Completed epoch 4
Process 1, Worker 3: Completed epoch 4
Process 1, Worker 4: Completed epoch 5
Process 1, Worker 1: Completed epoch 5
Process 1, Worker 2: Completed epoch 5
Process 1, Worker 3: Completed epoch 5

```

References:
- https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/
- https://jack-vanlightly.com/analyses/2024/7/30/understanding-apache-icebergs-consistency-model-part1
- https://medium.com/@shahsoumil519/can-10-spark-writers-perform-concurrent-appends-to-an-iceberg-table-simultaneously-774bccc030c7