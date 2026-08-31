\set ON_ERROR_STOP on
\pset pager off
\timing on

BEGIN;

DELETE FROM runku_environment_sequences
WHERE project_id = 'prj_00000000000000000000000000'
  AND environment_id = 'env_00000000000000000000000000';

INSERT INTO runku_environment_sequences(project_id, environment_id, commit_sequence)
VALUES ('prj_00000000000000000000000000', 'env_00000000000000000000000000', 1);

INSERT INTO runku_documents(
  project_id, environment_id, table_id, document_id, revision, commit_sequence,
  created_at_micros, updated_at_micros, value_bytes
)
SELECT
  'prj_00000000000000000000000000',
  'env_00000000000000000000000000',
  'tbl_00000000000000000000000000',
  'doc_' || lpad(value::text, 26, '0'),
  1, 1, 0, 0, decode('52560100', 'hex')
FROM generate_series(1, 10000) AS value;

INSERT INTO runku_index_entries(
  project_id, environment_id, index_id, key_bytes, table_id, document_id,
  document_revision, commit_sequence
)
SELECT
  'prj_00000000000000000000000000',
  'env_00000000000000000000000000',
  'idx_00000000000000000000000000',
  decode(
    '524b0150' || encode(convert_to(lpad(value::text, 10, '0'), 'UTF8'), 'hex') || '0000',
    'hex'
  ),
  'tbl_00000000000000000000000000',
  'doc_' || lpad(value::text, 26, '0'),
  1, 1
FROM generate_series(1, 10000) AS value;

ANALYZE runku_index_entries;

EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT TEXT)
SELECT index_id, key_bytes, table_id, document_id, document_revision, commit_sequence
FROM runku_index_entries
WHERE project_id = 'prj_00000000000000000000000000'
  AND environment_id = 'env_00000000000000000000000000'
  AND index_id = 'idx_00000000000000000000000000'
  AND key_bytes >= decode('524b0150', 'hex') || convert_to('0000005000', 'UTF8') || decode('0000', 'hex')
  AND key_bytes < decode('524b0150', 'hex') || convert_to('0000005100', 'UTF8') || decode('0000', 'hex')
ORDER BY key_bytes, document_id
LIMIT 100;

ROLLBACK;
