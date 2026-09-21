-- Load one S3 partition into RAW.CFPB.COMPLAINTS.
-- Rendered by Airflow. Defaults to today's incremental partition,
-- incremental/load_date={{ ds }}/. Pass params.path = 'backfill/' to load the
-- one-time historical export.
--
-- COPY skips files it has already loaded (for 64 days). A re-extracted partition
-- reuses the same file names, so incremental loads set FORCE = TRUE; staging
-- dedupes on complaint_id, so reloading is safe.

copy into raw.cfpb.complaints (record, _file, _file_row, _loaded_at)
from (
    select
        $1,
        metadata$filename,
        metadata$file_row_number,
        current_timestamp()::timestamp_ntz
    from @raw.cfpb.cfpb_stage/{{ params.get('path') or 'incremental/load_date=' ~ ds ~ '/' }}
)
file_format = (format_name = 'RAW.CFPB.JSON_GZ')
on_error = 'ABORT_STATEMENT'
force = {{ params.get('force', 'TRUE') }};
