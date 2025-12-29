
SELECT
    DB_NAME() AS DatabaseName,
    SCHEMA_NAME(o.schema_id) AS SchemaName,
    o.name AS ObjectName,
    o.type AS ObjectType,
    COALESCE(m.definition, OBJECT_DEFINITION(o.object_id)) AS ObjectDefinition,
    OBJECTPROPERTY(o.object_id, 'IsEncrypted') AS IsEncrypted,
    o.modify_date AS ModifiedDate
FROM sys.objects o
LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
WHERE o.type IN ('V', 'P')
ORDER BY SchemaName, ObjectName
