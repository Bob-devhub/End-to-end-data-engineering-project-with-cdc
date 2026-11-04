# From Copy job spec to deployable Copy job

`scripts/generate_copy_jobs.py` writes a neutral spec per `copy_job_group`
(`build/copy_jobs/cj_*.json`). To make Copy jobs fully generated from metadata:

1. **Build one by hand.** In DEV, create `cj_wwi_cdc_scd2`: source is the Azure SQL
   connection, select `Sales.Customers` and `Warehouse.StockItems`, choose
   CDC-based incremental, destination is `lh_bronze`, write method is SCD Type 2.
2. **Commit it from the Source control pane.** Fabric writes
   `fabric/cj_wwi_cdc_scd2.CopyJob/` containing the item definition.
3. **Turn it into a template.** Copy the definition JSON to
   `metadata/templates/copyjob.template.json`. Note where the table list,
   source connection and write method live.
4. **Render from the spec.** Extend `scripts/generate_copy_jobs.py` to fill the
   template from each spec and write `fabric/<name>.CopyJob/`. fabric-cicd then
   deploys Copy jobs like any other item.
5. **Repeat** for the Merge and watermark groups, and confirm that only the table list
   and methods differ.

Known Copy job CDC constraints that the metadata validator enforces or that you
should keep in mind:

- Mixing CDC and non-CDC tables in one job → all tables use watermark.
  Enforced: one read method per group.
- SCD2 is applied to every table in the job. Enforced: one write method per group.
- Only the default capture instance, and net changes only.
- SQL database in Fabric is **not** a CDC source for Copy job. That's why the OLTP
  source is Azure SQL DB.
- A mapped source column that is dropped or renamed fails the run. New source
  columns are ignored.
