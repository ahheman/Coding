SELECT column_name
FROM user_tab_columns
WHERE UPPER(table_name) = 'MAPPING_VALUE'
  AND column_name LIKE 'FIELD_MAP_%'
ORDER BY column_name;
