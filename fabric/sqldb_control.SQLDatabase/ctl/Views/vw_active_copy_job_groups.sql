CREATE   VIEW ctl.vw_active_copy_job_groups AS
SELECT e.copy_job_group,
       'cj_' + e.copy_job_group AS copy_job_name,
       LOWER(CONVERT(VARCHAR(36), cj.item_id)) AS copy_job_id,  -- Fabric uses lowercase ids
       MIN(e.priority)          AS priority,
       COUNT(*)                 AS table_count
FROM ctl.source_entity e
LEFT JOIN ctl.copy_job cj ON cj.copy_job_group = e.copy_job_group
WHERE e.is_active = 1 AND e.ingestion_engine = 'copy_job'
GROUP BY e.copy_job_group, cj.item_id;

GO

