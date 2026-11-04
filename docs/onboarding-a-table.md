# Onboarding a new source table (no code changes)

## Database table (Azure SQL, via Copy job)

1. If you want CDC, enable it on the table (`source/azure_sql/001_enable_cdc.sql` pattern).
2. Add `metadata/config/entities/wwi_oltp/<schema>_<table>.yml`:

   ```yaml
   entity_id: wwi_purchasing_suppliers
   system_id: wwi_oltp
   source: { schema_name: Purchasing, table: Suppliers }
   target: { lakehouse: lh_bronze, table: wwi_purchasing_suppliers, silver_table: suppliers }
   load:
     ingestion_engine: copy_job
     read_method: cdc            # or watermark (+ watermark_column)
     write_method: scd2          # or scd1_merge
     copy_job_group: wwi_cdc_scd2
     primary_keys: [SupplierID]
   columns:
     - { source: SupplierID, target: supplier_id, type: int, nullable: false }
     - { source: SupplierName, target: supplier_name, type: string }
     - { source: Valid_From, target: valid_from, type: timestamp }
     - { source: Valid_To, target: valid_to, type: timestamp }
     - { source: Is_Current, target: is_current, type: boolean }
   dq_rules:
     - { rule_type: not_null, column: supplier_id, action: fail }
   ```

3. `python scripts/validate_metadata.py`. This catches things like putting a
   watermark table into a CDC copy job group.
4. Open a PR. CI validates and regenerates the Copy job specs.
5. After merge: add the table to the matching Copy job (manually for now, or
   rendered from the spec once the template step is in place). Run the deploy workflow.

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
