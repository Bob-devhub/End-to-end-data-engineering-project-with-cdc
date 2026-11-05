
-- ---------------------------------------------------------------- views for pipeline Lookups
CREATE   VIEW ctl.vw_active_entities AS
SELECT e.entity_id, e.system_id, s.source_type, s.connection_name, e.priority,
       e.ingestion_engine, e.read_method, e.write_method, e.copy_job_group,
       e.bronze_table, e.silver_table, e.config_json
FROM ctl.source_entity e
JOIN ctl.source_system s ON s.system_id = e.system_id
WHERE e.is_active = 1;

GO

