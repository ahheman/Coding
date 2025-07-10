SET SERVEROUTPUT ON;
DECLARE
    -- Input values
    p_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    p_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    p_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Output cursors
    o_cursor_product  SYS_REFCURSOR;
    o_cursor_mcc      SYS_REFCURSOR;

    -- Internal variables
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql_product   CLOB;
    v_sql_mcc       CLOB;

    -- Temp fetch variables
    v_field_value_mapping VARCHAR2(4000);
    v_map1 VARCHAR2(4000);
    v_map2 VARCHAR2(4000);
    v_map3 VARCHAR2(4000);
BEGIN
    -- Step 1: Extract column names with value
    FOR col_rec IN (
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = 'MAPPING_VALUE'
          AND column_name LIKE 'FIELD_MAP_%'
        ORDER BY column_name
    )
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || col_rec.column_name || '
                               FROM STAGING.MAPPING_VALUE
                               WHERE FIELD_TO_BE_DERIVED = :1
                                 AND INTERFACE_FILE = :2
                                 AND TRUNC(EFFECTIVE_DATE) = :3
                                 AND ACTIVE = ''A''
                                 AND ' || col_rec.column_name || ' IS NOT NULL
                                 AND ROWNUM = 1'
            INTO v_val
            USING p_FIELD_TO_BE_DERIVED, p_INTERFACE_FILE, p_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
                DBMS_OUTPUT.PUT_LINE('✔ ' || col_rec.column_name || ' = ' || v_val);
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No field_map_x columns found.');
        RETURN;
    END IF;

    -- Build PRODUCT SQL
    v_sql_product := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_product := v_sql_product || ', ' || v_field_names(i);
    END LOOP;
    v_sql_product := v_sql_product || ' FROM STAGING.PRODUCT_FIELD_MAPPING
        WHERE INTERFACE_FILE = :1
          AND TRUNC(EFFECTIVE_DATE) = :2
          AND ACTIVE = ''A''';

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '▶ PRODUCT SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql_product);

    -- Build MCC SQL
    v_sql_mcc := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_mcc := v_sql_mcc || ', ' || v_field_names(i);
    END LOOP;
    v_sql_mcc := v_sql_mcc || ' FROM STAGING.MCC_FIELD_MAPPING
        WHERE INTERFACE_FILE = :1
          AND TRUNC(EFFECTIVE_DATE) = :2
          AND ACTIVE = ''A''';

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '▶ MCC SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql_mcc);

    -- Open and fetch PRODUCT cursor
    OPEN o_cursor_product FOR v_sql_product USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    LOOP
        FETCH o_cursor_product INTO v_field_value_mapping, v_map1, v_map2, v_map3;
        EXIT WHEN o_cursor_product%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('PRODUCT => ' || v_field_value_mapping || ' | ' || v_map1 || ' | ' || v_map2 || ' | ' || v_map3);
    END LOOP;
    CLOSE o_cursor_product;

    -- Open and fetch MCC cursor
    OPEN o_cursor_mcc FOR v_sql_mcc USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    LOOP
        FETCH o_cursor_mcc INTO v_field_value_mapping, v_map1, v_map2, v_map3;
        EXIT WHEN o_cursor_mcc%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('MCC => ' || v_field_value_mapping || ' | ' || v_map1 || ' | ' || v_map2 || ' | ' || v_map3);
    END LOOP;
    CLOSE o_cursor_mcc;
END;
/
