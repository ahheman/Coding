CREATE OR REPLACE PROCEDURE STAGING.dynamic_mapping_value_result (
    p_field_to_be_derived   IN  VARCHAR2,
    p_interface_file        IN  VARCHAR2,
    p_effective_date        IN  DATE,
    o_result_rows           OUT STAGING.mapping_table_type
)
AS
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_sql           VARCHAR2(32767);
BEGIN
    o_result_rows := STAGING.mapping_table_type();  -- initialize output

    -- Step 1: Get field_map_x columns which have data
    FOR col_rec IN (
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = 'MAPPING_VALUE'
          AND column_name LIKE 'FIELD_MAP_%'
        ORDER BY column_name
    ) LOOP
        DECLARE
            v_temp_val VARCHAR2(4000);
        BEGIN
            EXECUTE IMMEDIATE '
                SELECT ' || col_rec.column_name || '
                  FROM STAGING.MAPPING_VALUE
                 WHERE FIELD_TO_BE_DERIVED = :1
                   AND INTERFACE_FILE = :2
                   AND TRUNC(EFFECTIVE_DATE) = :3
                   AND ACTIVE = ''A''
                   AND ' || col_rec.column_name || ' IS NOT NULL
                   AND ROWNUM = 1'
            INTO v_temp_val
            USING p_field_to_be_derived, p_interface_file, p_effective_date;

            IF v_temp_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    IF v_count = 0 THEN
        RETURN;
    END IF;

    -- Step 2: Build dynamic SQL
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.' ||
        CASE UPPER(p_field_to_be_derived)
            WHEN 'PRODUCT_FIELD_MAPPING' THEN 'PRODUCT_FIELD_MAPPING'
            WHEN 'MCC_FIELD_MAPPING' THEN 'MCC_FIELD_MAPPING'
        END ||
        ' WHERE INTERFACE_FILE = :1 AND TRUNC(EFFECTIVE_DATE) = :2 AND ACTIVE = ''A''';

    -- Step 3: Execute and fetch into result
    DECLARE
        TYPE t_row IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
        v_stmt     VARCHAR2(32767);
        v_cur      SYS_REFCURSOR;
        v_val_list t_row;
        v_fvm      VARCHAR2(4000);
        i          INTEGER;
    BEGIN
        OPEN v_cur FOR v_sql USING p_interface_file, p_effective_date;

        LOOP
            v_val_list.DELETE;
            FETCH v_cur INTO v_fvm, v_val_list(1), v_val_list(2), v_val_list(3), v_val_list(4),
                              v_val_list(5), v_val_list(6), v_val_list(7), v_val_list(8), v_val_list(9);
            EXIT WHEN v_cur%NOTFOUND;

            DECLARE
                temp_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
            BEGIN
                FOR j IN 1 .. v_count LOOP
                    temp_list.EXTEND;
                    temp_list(j) := v_val_list(j);
                END LOOP;

                o_result_rows.EXTEND;
                o_result_rows(o_result_rows.COUNT) := STAGING.mapping_row_type(v_fvm, temp_list);
            END;
        END LOOP;

        CLOSE v_cur;
    END;

END;
/
