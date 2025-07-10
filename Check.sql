CREATE OR REPLACE TYPE STAGING.mapping_row_type AS OBJECT (
    field_value_mapping VARCHAR2(4000),
    field_map_values    SYS.ODCIVARCHAR2LIST
);
/

CREATE OR REPLACE TYPE STAGING.mapping_table_type AS TABLE OF STAGING.mapping_row_type;
/
