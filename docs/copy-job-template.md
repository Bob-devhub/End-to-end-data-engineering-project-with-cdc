# Copy jobs from metadata

`scripts/generate_copy_jobs.py` renders one Copy job per `copy_job_group` into
`fabric/cj_<group>.CopyJob/`, in the same format Fabric Git integration writes.
CI runs it with `--check` and fails if a committed definition has drifted from
metadata, for example after someone edits the job in the UI without updating YAML.

```bash
python scripts/snapshot_source_schema.py   # only after a source schema change
python scripts/generate_copy_jobs.py        # write/update fabric/*.CopyJob
python scripts/generate_copy_jobs.py --check
```

## How the template was captured

`cj_wwi_cdc_scd2` was built once in the Fabric UI and synced to Git. Its
definition is the reference for the renderer (`src/cdc_framework/copy_jobs.py`).
`tests/test_copy_jobs.py` asserts that rendering from metadata reproduces that
exported definition exactly.

Key fields:

| Definition field | From metadata |
|---|---|
| `properties.jobMode` (`CDC`) | `load.read_method: cdc` |
| `activities[].source.changeDataSettings.readMethod` (`SnapshotPlusIncremental`) | `load.read_method: cdc` |
| `activities[].destination.writeBehavior` (`SCD2` / `Upsert`) | `load.write_method: scd2` / `scd1_merge` (the UI's "Merge" is stored as `Upsert`) |
| `activities[].destination.upsertSettings.keys` | `load.primary_keys` |
| source / destination `datasetSettings` | `source.schema_name/table`, `target.schema_name/table` |
| source `externalReferences.connection` | `source_systems.yml` → `fabric_connection_id` |
| destination `externalReferences.connection` | `fabric.yml` → `lakehouse_connections` |
| destination `artifactId` | logicalId from `fabric/<lakehouse>.Lakehouse/.platform` |
| `translator` | auto-mapping, or an explicit mapping (from `metadata/source_schemas/`) when `source.exclude_columns` is set |

Connection ids are DEV values. `fabric/parameter.yml` swaps them for TEST and
PROD. fabric-cicd handles the logicalId and "same workspace" zeros itself.

## Supported read/write combinations

| read_method | write_method | Captured from | Notes |
|---|---|---|---|
| `cdc` | `scd2` | `cj_wwi_cdc_scd2` | `writeBehavior: SCD2` |
| `cdc` | `scd1_merge` | `cj_wwi_cdc_merge` | the UI's Merge is stored as `writeBehavior: Upsert` |
| `watermark` | `scd1_merge` | `cj_wwi_watermark` | same `jobMode`/`readMethod` as CDC; `changeDataSettings` adds `columns` (the incremental column, typed from the snapshot) and `nullWatermarkBehavior: Skip` |

Any other combination (e.g. watermark + SCD2) is reported as `skipped`. To add
one, build it once by hand in DEV, commit it, add it to `_JOB_SETTINGS` in
`copy_jobs.py`, and add it to the export-match test in `tests/test_copy_jobs.py`.

## Unsupported column types

Delta has no equivalent for `geography`, `geometry`, `hierarchyid`, `sql_variant`
or `xml`. The renderer refuses to auto-map a table containing them. List those
columns in `source.exclude_columns`. That switches the table to an explicit
mapping built from the schema snapshot.

## Known Copy job CDC constraints

- A job that mixes CDC and non-CDC tables makes every table use watermark.
  The validator enforces one read method per group.
- SCD2 applies to every table in the job. The validator enforces one write
  method per group.
- CDC requires the Merge or SCD2 update method.
- Only the default capture instance, and net changes only.
- SQL database in Fabric is **not** a CDC source for Copy job. That's why the
  OLTP source is Azure SQL DB.
- A mapped source column that is dropped or renamed fails the run. New source
  columns are ignored.
