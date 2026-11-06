# Onboarding a new source table (no code changes)

## Database table (Azure SQL, via Copy job)

1. If you want CDC, enable it on the table (`source/azure_sql/001_enable_cdc.sql` pattern).
2. Add `metadata/config/entities/wwi_oltp/<schema>_<table>.yml`:

   ```yaml
   entity_id: wwi_purchasing_suppliers
   system_id: wwi_oltp
   source:
     schema_name: Purchasing
     table: Suppliers
     exclude_columns: [DeliveryLocation]   # geography column
   target: { lakehouse: lh_bronze, schema_name: Purchasing, table: Suppliers, silver_table: suppliers }
   load:
     ingestion_engine: copy_job
     read_method: cdc            # or watermark (+ watermark_column)
     write_method: scd2          # or scd1_merge
     copy_job_group: wwi_cdc_scd2
     primary_keys: [SupplierID]
   columns:
     - { source: SupplierID, target: supplier_id, type: int, nullable: false }
     - { source: SupplierName, target: supplier_name, type: string }
     # SCD2 columns added by Copy job (stored as strings; cast here)
     - { source: copyjob_valid_from, target: valid_from, type: timestamp }
     - { source: copyjob_valid_to, target: valid_to, type: timestamp }
     - { source: copyjob_is_current, target: is_current, type: boolean }
   dq_rules:
     - { rule_type: not_null, column: supplier_id, action: fail }
   ```

3. Refresh the schema snapshot and render the Copy job:

   ```bash
   python scripts/validate_metadata.py        # e.g. a watermark table in a CDC group
   python scripts/snapshot_source_schema.py   # picks up the new table's columns
   python scripts/generate_copy_jobs.py       # updates fabric/cj_<group>.CopyJob
   ```

   The renderer tells you if the table has columns Delta can't store (geography
   and similar) that need to go in `exclude_columns`.
4. Open a PR with the YAML, the snapshot and the updated Copy job. CI validates
   and checks that `fabric/` matches the metadata.
5. After merge, update the DEV workspace from Git (Source control → Update).
   Run the deploy workflow for TEST/PROD.

Bronze tables keep the source schema and table name (`lh_bronze.Purchasing.Suppliers`)
because the lakehouses are schema-enabled and that's what Copy job writes.

Pick `copy_job_group` by **(read_method, write_method)**. Copy job applies both
to every table in the job, and mixing CDC with non-CDC tables silently
downgrades the whole job to watermark.

## Partner file feed (ADLS)

Add `metadata/config/entities/partner_files/<feed>.yml` with
`ingestion_engine: notebook`, `read_method: new_files`, a `file_pattern` and a
`format`. The storage event trigger and `nb_ingest_files` pick it up.

## Real-time stream

Add `metadata/config/streams/<stream>.yml` with `event_type`, `fields`,
optional `aggregates` and `alerts`. Producers send JSON with
`"event_type": "<event_type>"` to the existing Eventstream. Then
`scripts/deploy_kql.py --apply` creates the typed table, parse function,
update policy and materialized views.
