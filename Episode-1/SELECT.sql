SELECT 
	le.entity_name AS "Entity Name",  
	pj.job_name AS "Job Name", 
	TRIM (LEADING 'k' from et.env_name) AS "Type", 
	MAX (ROUND (CAST((pjre.source_logical_size_bytes/1e+9) AS NUMERIC), 2)) AS "Source Logical GB",
	SUM (ROUND (CAST((pjre.source_delta_size_bytes/1e+9) AS NUMERIC), 2)) AS "Data Read GB", 
	SUM (ROUND (CAST((pjre.data_written_size_bytes/1e+6) AS NUMERIC), 2)) AS "Data Written MB",
	CAST (SUM((pjre.source_logical_size_bytes/1e+6) *.0001) AS MONEY) AS "Charge"
FROM reporting.protection_job_run_entities AS pjre
INNER JOIN reporting.cluster AS cs
ON pjre.cluster_id=cs.cluster_id
INNER JOIN reporting.leaf_entities AS le
ON le.entity_id=pjre.entity_id
INNER JOIN reporting.protection_jobs AS pj
ON pj.job_id=pjre.job_id
INNER JOIN reporting.registered_sources AS rs
ON rs.source_id=pjre.parent_source_id
INNER JOIN reporting.job_run_status AS jrs
ON jrs.status_id=pjre.status
INNER JOIN reporting.environment_types as et
ON pjre.entity_env_type=et.env_id
WHERE to_timestamp(end_time_usecs::bigint/1000000) IN 
	(SELECT
		to_timestamp(end_time_usecs::bigint/1000000) FROM reporting.protection_job_run_entities
WHERE to_timestamp(end_time_usecs::bigint/1000000) >= (now() - '1 month'::interval))
GROUP BY "Entity Name", "Job Name", "Type"
ORDER BY "Entity Name" 