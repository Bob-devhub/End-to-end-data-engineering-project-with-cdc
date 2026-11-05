
CREATE   VIEW ctl.vw_active_copy_job_groups AS
SELECT copy_job_group,
       'cj_' + copy_job_group AS copy_job_name,
       MIN(priority)          AS priority,
       COUNT(*)               AS table_count
FROM ctl.source_entity
WHERE is_active = 1 AND ingestion_engine = 'copy_job'
GROUP BY copy_job_group;

GO

