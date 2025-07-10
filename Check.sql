DECLARE
    -- Input parameters
    p_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'CATEGORY_CODE'; 
    p_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO'; 
    p_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024','DD-MON-YY'); 

    -- Variables
    v_field_names      DBMS_SQL.VARCHAR2_TABLE;
    v_field_values     DBMS_SQL.VARCHAR2_TABLE;
    v_count            INTEGER := 0;
    v_val              VARCHAR2(4000);
    v_sql_product      CLOB;
    v_sql_mcc          CLOB;
    v_product_columns  SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
    v_mcc_columns      SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();

    -- Cursors for output
    c_product SYS_REFCURSOR;
    c_mcc     SYS_REFCURSOR;
BEGIN
    -- Step 1: Get FIELD_MAP_% columns in PRODUCT_FIELD_MAPPING
    FOR rec IN (
        SELECT column_name FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = 'PRODUCT_FIELD_MAPPING'
          AND column_name LIKE 'FIELD_MAP_%'
    ) LOOP
        v_product_columns.EXTEND;
        v_product_columns(v_product_columns.COUNT) := rec.column_name;
    END LOOP;

    -- Step 2: Get FIELD_MAP_% columns in MCC_FIELD_MAPPING
    FOR rec IN (
        SELECT column_name FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = 'MCC_FIELD_MAPPING'
          AND column_name LIKE 'FIELD_MAP_%'
    ) LOOP
        v_mcc_columns.EXTEND;
        v_mcc_columns(v_mcc_columns.COUNT) := rec.column_name;
    END LOOP;

    -- Step 3: Get FIELD_MAP_% values from MAPPING_VALUE
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
                WHERE FIELD_TO_BE_DERIVED = :p1
                  AND INTERFACE_FILE = :p2
                  AND TRUNC(EFFECTIVE_DATE) = :p3
                  AND ACTIVE = ''A''
                  AND ' || col_rec.column_name || ' IS NOT NULL
                  AND ROWNUM = 1
            ' INTO v_val
            USING p_FIELD_TO_BE_DERIVED, p_INTERFACE_FILE, p_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error fetching ' || col_rec.column_name || ': ' || SQLERRM);
        END;
    END LOOP;

    -- Step 4: Handle case when no mappings found
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No FIELD_MAP_% values found.');
        RETURN;
    END IF;

    -- Step 5: Build SQL for PRODUCT_FIELD_MAPPING
    v_sql_product := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        IF v_field_names(i) MEMBER OF v_product_columns THEN
            v_sql_product := v_sql_product || ', ' || v_field_names(i);
        END IF;
    END LOOP;

    v_sql_product := v_sql_product || ' FROM STAGING.PRODUCT_FIELD_MAPPING
        WHERE INTERFACE_FILE = :iface
          AND TRUNC(EFFECTIVE_DATE) = :effdate
          AND ACTIVE = ''A''';

    -- Step 6: Build SQL for MCC_FIELD_MAPPING
    v_sql_mcc := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        IF v_field_names(i) MEMBER OF v_mcc_columns THEN
            v_sql_mcc := v_sql_mcc || ', ' || v_field_names(i);
        END IF;
    END LOOP;

    v_sql_mcc := v_sql_mcc || ' FROM STAGING.MCC_FIELD_MAPPING
        WHERE INTERFACE_FILE = :iface
          AND TRUNC(EFFECTIVE_DATE) = :effdate
          AND ACTIVE = ''A''';

    -- Step 7: Open and fetch PRODUCT cursor
    OPEN c_product FOR v_sql_product USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    DBMS_OUTPUT.PUT_LINE('--- PRODUCT_FIELD_MAPPING ---');
    LOOP
        FETCH c_product INTO v_val;
        EXIT WHEN c_product%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Row: ' || v_val);
    END LOOP;
    CLOSE c_product;

    -- Step 8: Open and fetch MCC cursor
    OPEN c_mcc FOR v_sql_mcc USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    DBMS_OUTPUT.PUT_LINE('--- MCC_FIELD_MAPPING ---');
    LOOP
        FETCH c_mcc INTO v_val;
        EXIT WHEN c_mcc%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('Row: ' || v_val);
    END LOOP;
    CLOSE c_mcc;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unhandled Error: ' || SQLERRM);
END;
/
