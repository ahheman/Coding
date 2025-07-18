DECLARE
    v_cursor_id   INTEGER;
    v_status      INTEGER;
    v_fvm         VARCHAR2(4000);
    v_col_val     VARCHAR2(4000);
    v_row         STAGING.mapping_row_type;
    v_desc_tab    DBMS_SQL.DESC_TAB;
    v_col_cnt     INTEGER;
    v_results     STAGING.mapping_table_type := STAGING.mapping_table_type();
    v_sql         VARCHAR2(32767) := 'SELECT FIELD_VALUE_MAPPING, FIELD_MAP_1, FIELD_MAP_2
                                      FROM STAGING.PRODUCT_FIELD_MAPPING
                                      WHERE INTERFACE_FILE = :1
                                        AND TRUNC(EFFECTIVE_DATE) = :2
                                        AND ACTIVE = ''A''';
BEGIN
    v_cursor_id := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);

    DBMS_SQL.BIND_VARIABLE(v_cursor_id, 1, 'SAMPLE_INTERFACE_FILE');
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, 2, TO_DATE('2024-07-01', 'YYYY-MM-DD'));

    DBMS_SQL.DESCRIBE_COLUMNS(v_cursor_id, v_col_cnt, v_desc_tab);

    FOR i IN 1 .. v_col_cnt LOOP
        DBMS_SQL.DEFINE_COLUMN(v_cursor_id, i, v_col_val, 4000);
    END LOOP;

    v_status := DBMS_SQL.EXECUTE(v_cursor_id);

    WHILE DBMS_SQL.FETCH_ROWS(v_cursor_id) > 0 LOOP
        v_row := STAGING.mapping_row_type(NULL, SYS.ODCIVARCHAR2LIST());
        FOR i IN 1 .. v_col_cnt LOOP
            DBMS_SQL.COLUMN_VALUE(v_cursor_id, i, v_col_val);
            IF i = 1 THEN
                v_row.field_value_mapping := v_col_val;
            ELSE
                v_row.field_map_values.EXTEND;
                v_row.field_map_values(v_row.field_map_values.COUNT) := v_col_val;
            END IF;
        END LOOP;

        v_results.EXTEND;
        v_results(v_results.COUNT) := v_row;
    END LOOP;

    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);

    -- Print result (optional)
    FOR i IN 1 .. v_results.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Mapping: ' || v_results(i).field_value_mapping);
        FOR j IN 1 .. v_results(i).field_map_values.COUNT LOOP
            DBMS_OUTPUT.PUT_LINE('  Field Map ' || j || ': ' || v_results(i).field_map_values(j));
        END LOOP;
    END LOOP;
END;
/
