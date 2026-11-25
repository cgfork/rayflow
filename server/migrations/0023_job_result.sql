CREATE TABLE IF NOT EXISTS job_result (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_uuid CHAR(36) NOT NULL,
    agent_id CHAR(36) NOT NULL,
    catalog_uuid CHAR(36) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_job_result_job_uuid (job_uuid),
    UNIQUE KEY uk_job_result_catalog_uuid (catalog_uuid)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = 'job result owner and catalog association';

INSERT INTO job_result (job_uuid, agent_id, created_at, updated_at)
SELECT uuid, agent_id, COALESCE(polled_at, created_at), updated_at
FROM job
WHERE status = 'Polled' AND agent_id IS NOT NULL
ON DUPLICATE KEY UPDATE job_uuid = VALUES(job_uuid);
