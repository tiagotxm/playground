import trino
import os, sys, datetime, uuid, random, json
from threading import Thread

PROCESS_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 1
NUM_WORKERS = 5
NUM_EPOCHS = 5

def generate_data(process_id, epoch, worker_id, batch_size=100):
    types = ['row_count', 'null_check', 'duplicate_check']
    statuses = ['SUCCESS', 'FAILED', 'SKIPPED']
    levels = ['INFO', 'WARN', 'ERROR']
    schemas = ['public', 'analytics', 'sales']
    tables = ['users', 'orders', 'transactions']
    dag_names = ['daily_quality_checks', 'etl_pipeline']
    task_names = ['check_nulls', 'check_duplicates', 'check_row_count']

    now = datetime.datetime.now()
    reference_date = now.strftime('%Y-%m-%d')

    return [{
        'id': str(uuid.uuid4()),
        'type': random.choice(types),
        'status': random.choice(statuses),
        'level': random.choice(levels),
        'schema_name': random.choice(schemas),
        'table_name': random.choice(tables),
        'parameters': json.dumps({'threshold': random.randint(1, 100)}),
        'query': f"SELECT * FROM {random.choice(schemas)}.{random.choice(tables)} WHERE ...",
        'failed_rows_count': float(random.randint(0, 100)),
        'output_results': json.dumps({'result': random.choice(['pass', 'fail'])}),
        'dag_name': random.choice(dag_names),
        'task_name': random.choice(task_names),
        'created_at': now.isoformat(),
        'reference_date': reference_date
    } for _ in range(batch_size)]

def insert_data(conn, data):
    for row in data:
        sql = f"""
        INSERT INTO iceberg.quality_checks.quality_check_results (
            id, type, status, level, schema_name, table_name, parameters, query,
            failed_rows_count, output_results, dag_name, task_name, created_at, reference_date
        ) VALUES (
            '{row['id']}', '{row['type']}', '{row['status']}', '{row['level']}',
            '{row['schema_name']}', '{row['table_name']}', '{row['parameters'].replace("'", "''")}',
            '{row['query'].replace("'", "''")}', {row['failed_rows_count']},
            '{row['output_results'].replace("'", "''")}', '{row['dag_name']}', '{row['task_name']}',
            '{row['created_at']}', DATE '{row['reference_date']}'
        )
        """
        cur = conn.cursor()
        cur.execute(sql)

def worker_all_epochs(process_id, worker_id, num_epochs, conn):
    try:
        for epoch in range(1, num_epochs + 1):
            data = generate_data(process_id, epoch, worker_id)
            insert_data(conn, data)
            print(f"Process {process_id}, Worker {worker_id}: Completed epoch {epoch}")
    except Exception as e:
        print(f"Process {process_id}, Worker {worker_id} failed: {e}")

def main():
    conn = trino.dbapi.connect(
        host='<trino-coordinator-host>',  
        port=443,
        http_scheme='https',
        user='<user>',  
        catalog='iceberg', 
        schema='quality_checks', 
        auth=trino.auth.BasicAuthentication('<user>', '<password>')

    )
    print(f"Starting Process {PROCESS_ID} with {NUM_WORKERS} workers, each writing {NUM_EPOCHS} epochs")

    threads = []
    for worker_id in range(1, NUM_WORKERS + 1):
        thread = Thread(target=worker_all_epochs, args=(PROCESS_ID, worker_id, NUM_EPOCHS, conn))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f"\nProcess {PROCESS_ID} completed!")

if __name__ == "__main__":
    main()