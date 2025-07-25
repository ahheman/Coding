CREATE OR REPLACE PACKAGE BODY PKG_IFRS_UTL AS

  PROCEDURE sp_getdynamicmappingvalues (
      p_field_to_be_derived IN VARCHAR2,
      p_interface_file      IN VARCHAR2,
      p_effective_date      IN DATE,
      o_result_rows         OUT mapping_table_type
  ) IS
    v_sql            VARCHAR2(32767);
    v_cursor_id      INTEGER;
    v_status         INTEGER;
    v_col_val        VARCHAR2(4000);
    v_row            mapping_row_type;
    v_desc_tab       DBMS_SQL.DESC_TAB;
    v_col_cnt        INTEGER := 0;
    v_col_name       VARCHAR2(128);
    v_col_value_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
  BEGIN
    o_result_rows := mapping_table_type();

    -- Identify relevant table
    v_sql := 'SELECT FIELD_VALUE_MAPPING';

    FOR col_rec IN (
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = UPPER(p_field_to_be_derived)
          AND column_name LIKE 'FIELD_MAP_%'
        ORDER BY column_name
    ) LOOP
      v_sql := v_sql || ', ' || col_rec.column_name;
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.' || UPPER(p_field_to_be_derived) ||
             ' WHERE INTERFACE_FILE = :1 AND TRUNC(EFFECTIVE_DATE) = TRUNC(:2) AND ACTIVE = ''A''';

    DBMS_OUTPUT.PUT_LINE('Constructed SQL: ' || v_sql);

    -- Use DBMS_SQL for dynamic column handling
    v_cursor_id := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, 1, p_interface_file);
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, 2, p_effective_date);

    -- Describe and define columns
    DBMS_SQL.DESCRIBE_COLUMNS(v_cursor_id, v_col_cnt, v_desc_tab);
    FOR i IN 1 .. v_col_cnt LOOP
      DBMS_SQL.DEFINE_COLUMN(v_cursor_id, i, v_col_val, 4000);
    END LOOP;

    -- Execute and fetch
    v_status := DBMS_SQL.EXECUTE(v_cursor_id);
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor_id) > 0 LOOP
      v_row := mapping_row_type(NULL, SYS.ODCIVARCHAR2LIST());
      FOR i IN 1 .. v_col_cnt LOOP
        DBMS_SQL.COLUMN_VALUE(v_cursor_id, i, v_col_val);
        IF i = 1 THEN
          v_row.field_value_mapping := v_col_val;
        ELSE
          v_row.field_map_values.EXTEND;
          v_row.field_map_values(v_row.field_map_values.COUNT) := v_col_val;
        END IF;
      END LOOP;
      o_result_rows.EXTEND;
      o_result_rows(o_result_rows.COUNT) := v_row;
    END LOOP;

    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
    DBMS_OUTPUT.PUT_LINE('Total rows fetched: ' || o_result_rows.COUNT);

  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cursor_id) THEN
        DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
      END IF;
      DBMS_OUTPUT.PUT_LINE('Error in main procedure: ' || SQLERRM);
      RAISE;
  END sp_getdynamicmappingvalues;

END PKG_IFRS_UTL;
/
