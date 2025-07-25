CREATE OR REPLACE PACKAGE BODY PKG_IFRS_UTL AS

  PROCEDURE sp_getdynamicmappingvalues (
    p_field_to_be_derived IN VARCHAR2,
    p_interface_file      IN VARCHAR2,
    p_effective_date      IN DATE,
    o_result_rows         OUT mapping_table_type
  ) IS
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_column_value  VARCHAR2(4000);
    v_sql           VARCHAR2(32767);
    v_cursor_id     INTEGER;
    v_count         INTEGER := 0;
    v_status        INTEGER;
  BEGIN
    o_result_rows := mapping_table_type();

    -- Step 1: Discover FIELD_MAP_% columns
    FOR col_rec IN (
      SELECT column_name
      FROM all_tab_columns
      WHERE owner = 'STAGING'
        AND table_name = 'MAPPING_VALUE'
        AND column_name LIKE 'FIELD_MAP_%'
      ORDER BY column_name
    ) LOOP
      BEGIN
        EXECUTE IMMEDIATE '
          SELECT ' || col_rec.column_name || '
          FROM STAGING.MAPPING_VALUE
          WHERE INTERFACE_FILE = :1
            AND TRUNC(EFFECTIVE_DATE) = TRUNC(:2)
            AND ACTIVE = ''A''
            AND ' || col_rec.column_name || ' IS NOT NULL
            AND ROWNUM = 1'
        INTO v_column_value
        USING p_interface_file, p_effective_date;

        v_count := v_count + 1;
        v_field_names(v_count) := col_rec.column_name;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          NULL;
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('⚠️ Error checking column ' || col_rec.column_name || ': ' || SQLERRM);
      END;
    END LOOP;

    IF v_count = 0 THEN
      DBMS_OUTPUT.PUT_LINE('❌ No FIELD_MAP_% columns found. Exiting.');
      RETURN;
    END IF;

    -- Step 2: Determine target table
    DECLARE
      v_table_name VARCHAR2(100);
    BEGIN
      CASE UPPER(p_field_to_be_derived)
        WHEN 'PRODUCT_FIELD_MAPPING' THEN
          v_table_name := 'PRODUCT_FIELD_MAPPING';
        WHEN 'MCC_FIELD_MAPPING' THEN
          v_table_name := 'MCC_FIELD_MAPPING';
        ELSE
          RAISE_APPLICATION_ERROR(-20001, 'Unsupported mapping type: ' || p_field_to_be_derived);
      END CASE;

      -- Step 3: Build dynamic SQL
      v_sql := 'SELECT FIELD_VALUE_MAPPING';
      FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
      END LOOP;

      v_sql := v_sql || ' FROM STAGING.' || v_table_name ||
               ' WHERE INTERFACE_FILE = :iface AND TRUNC(EFFECTIVE_DATE) = TRUNC(:effdate) AND ACTIVE = ''A''';

      DBMS_OUTPUT.PUT_LINE('🧾 Final SQL: ' || v_sql);
    END;

    -- Step 4: Use DBMS_SQL to fetch rows dynamically
    v_cursor_id := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);

    FOR i IN 1 .. v_count + 1 LOOP
      DBMS_SQL.DEFINE_COLUMN(v_cursor_id, i, v_column_value, 4000);
    END LOOP;

    DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':iface', p_interface_file);
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':effdate', p_effective_date);

    v_status := DBMS_SQL.EXECUTE(v_cursor_id);

    WHILE DBMS_SQL.FETCH_ROWS(v_cursor_id) > 0 LOOP
      DECLARE
        rec       mapping_row_type;
        temp_list string_list := string_list();
      BEGIN
        DBMS_SQL.COLUMN_VALUE(v_cursor_id, 1, rec.field_value_mapping);

        FOR i IN 1 .. v_count LOOP
          DBMS_SQL.COLUMN_VALUE(v_cursor_id, i + 1, v_column_value);
          temp_list.EXTEND;
          temp_list(temp_list.COUNT) := v_column_value;
        END LOOP;

        rec.field_map_values := temp_list;
        o_result_rows.EXTEND;
        o_result_rows(o_result_rows.COUNT) := rec;
      END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('✅ Total rows fetched: ' || o_result_rows.COUNT);
    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);

  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cursor_id) THEN
        DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
      END IF;
      DBMS_OUTPUT.PUT_LINE('🔥 Error in procedure: ' || SQLERRM);
      RAISE;
  END sp_getdynamicmappingvalues;

END PKG_IFRS_UTL;
/
