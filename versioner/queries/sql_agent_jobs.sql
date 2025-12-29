
SELECT
    j.job_id,
    j.name AS JobName,
    j.enabled AS IsEnabled,
    j.description AS JobDescription,
    j.date_created AS DateCreated,
    j.date_modified AS DateModified,
    s.step_id AS StepId,
    s.step_name AS StepName,
    s.command AS Command,
    s.subsystem AS Subsystem,
    s.database_name AS DatabaseName,
    s.on_success_action AS OnSuccessAction,
    s.on_fail_action AS OnFailAction,
    s.retry_attempts AS RetryAttempts,
    s.retry_interval AS RetryInterval
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id
WHERE j.enabled = 1 -- Only enabled jobs
ORDER BY j.name, s.step_id
