-- Historical file keys encode /run_uuid/graph_name/version/node_name/revision/filename.
-- Encode pinned graph node names byte-for-byte as the agent does (RFC 3986).
-- ORM operations cannot express a recursive data backfill or schema changes.
WITH RECURSIVE encoded_node AS (
    SELECT g.uuid AS graph_id, g.version, n.node_id,
           CAST(n.node_name AS BINARY) AS original,
           1 AS pos, CAST('' AS CHAR(3072) CHARACTER SET ascii) AS encoded
    FROM graph g
    JOIN JSON_TABLE(g.graph, '$.nodes[*]' COLUMNS (
        node_id CHAR(36) PATH '$.id', node_name VARCHAR(255) PATH '$.name'
    )) AS n
    UNION ALL
    SELECT graph_id, version, node_id, original, pos + 1,
        CONCAT(encoded, CASE
            WHEN ASCII(SUBSTRING(original, pos, 1)) BETWEEN 48 AND 57
              OR ASCII(SUBSTRING(original, pos, 1)) BETWEEN 65 AND 90
              OR ASCII(SUBSTRING(original, pos, 1)) BETWEEN 97 AND 122
              OR ASCII(SUBSTRING(original, pos, 1)) IN (45, 46, 95, 126)
            THEN CONVERT(SUBSTRING(original, pos, 1) USING ascii)
            ELSE CONCAT('%', HEX(SUBSTRING(original, pos, 1))) END)
    FROM encoded_node WHERE pos <= OCTET_LENGTH(original)
)
UPDATE /*+ SET_VAR(cte_max_recursion_depth=4096) */ file f
JOIN catalog c ON c.uuid = f.catalog_uuid AND c.`key` = f.fullpath
JOIN node_run nr ON LOCATE(CONCAT('/', nr.run_id, '/'), c.`key`) > 0
JOIN encoded_node n ON n.graph_id = nr.graph_id AND n.version = nr.version
    AND n.node_id = nr.node_id AND n.pos > OCTET_LENGTH(n.original)
SET f.node_run_id = nr.uuid, f.updated_at = f.updated_at
WHERE f.node_run_id IS NULL
    AND SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(c.`key`, LOCATE(CONCAT('/', nr.run_id, '/'), c.`key`) + 38), '/', 2), '/', -1) = CAST(nr.version AS CHAR)
    AND SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(c.`key`, LOCATE(CONCAT('/', nr.run_id, '/'), c.`key`) + 38), '/', 3), '/', -1) = n.encoded
    AND SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(c.`key`, LOCATE(CONCAT('/', nr.run_id, '/'), c.`key`) + 38), '/', 4), '/', -1) = CAST(nr.revision AS CHAR);

-- Fail instead of dropping paths if an owner cannot be resolved. Operators may
-- explicitly map legacy standalone files in node_run_id and retry migration 21.
-- A duplicate catalog reference also fails rather than discarding a file row.
ALTER TABLE file MODIFY COLUMN node_run_id CHAR(36) NOT NULL,
    DROP INDEX uk_file_fullpath, DROP INDEX idx_file_catalog_uuid, DROP COLUMN fullpath,
    ADD UNIQUE KEY uk_file_catalog_uuid (catalog_uuid),
    ADD KEY idx_file_node_run_id (node_run_id);
