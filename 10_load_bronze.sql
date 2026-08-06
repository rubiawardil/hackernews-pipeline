INSERT INTO bronze.item_versions (item_id, payload_hash, payload, source, captured_at, dag_run_id)
VALUES %s
ON CONFLICT (item_id, payload_hash) DO NOTHING
RETURNING item_id
