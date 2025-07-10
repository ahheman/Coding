SET SERVEROUTPUT ON;

DECLARE
    -- Input values
    v_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING'; -- or 'MCC_FIELD_MAPPING'
    v_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    v_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Field metadata
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);

    -- SQL holders
    v_sql_product   CLOB;
    v_sql_mcc       CLOB;

    -- Cursors
    v_cursor        SYS_REFCURSOR;
    v_f1            VARCHAR2(4000);
    v_f2            VARCHAR2(4000);
    v_f3            VARCHAR2(4000);
BEGIN
    -- STEP 1: Get FIELD_MAP_X columns from MAPPING_VALUE table
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
            EXECUTE IMMEDIATE 'SELECT ' || col_rec.column_name ||
                              ' FROM STAGING.MAPPING_VALUE
                               WHERE FIELD_TO_BE_DERIVED = :1
                                 AND INTERFACE_FILE = :2
                                 AND TRUNC(EFFECTIVE_DATE) = :3
                                 AND ACTIVE = ''A''
                                 AND ' || col_rec.column_name || ' IS NOT NULL
                                 AND ROWNUM = 1'
                INTO v_val
                USING v_FIELD_TO_BE_DERIVED, v_INTERFACE_FILE, v_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- STEP 2: Exit if no mappings found
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No FIELD_MAP_X values found.');
        RETURN;
    END IF;

    -- STEP 3: Generate and run dynamic SQL for PRODUCT_FIELD_MAPPING
    IF UPPER(v_FIELD_TO_BE_DERIVED) = 'PRODUCT_FIELD_MAPPING' THEN
        v_sql_product := 'SELECT FIELD_VALUE_MAPPING';
        FOR i IN 1 .. v_count LOOP
            v_sql_product := v_sql_product || ', ' || v_field_names(i);
        END LOOP;

        v_sql_product := v_sql_product || ' FROM STAGING.PRODUCT_FIELD_MAPPING
            WHERE INTERFACE_FILE = ''' || v_INTERFACE_FILE || '''
              AND TRUNC(EFFECTIVE_DATE) = TO_DATE(''' || TO_CHAR(v_EFFECTIVE_DATE, 'DD-MON-YYYY') || ''', ''DD-MON-YYYY'')
              AND ACTIVE = ''A''';

        DBMS_OUTPUT.PUT_LINE('🛠 PRODUCT SQL:');
        DBMS_OUTPUT.PUT_LINE(v_sql_product);

        OPEN v_cursor FOR v_sql_product;
        LOOP
            FETCH v_cursor INTO v_f1, v_f2, v_f3;
            EXIT WHEN v_cursor%NOTFOUND;
            DBMS_OUTPUT.PUT_LINE('✅ PRODUCT_MAPPING => ' || v_f1 || ' | ' || v_f2 || ' | ' || v_f3);
        END LOOP;
        CLOSE v_cursor;
    END IF;

    -- STEP 4: Generate and run dynamic SQL for MCC_FIELD_MAPPING
    IF UPPER(v_FIELD_TO_BE_DERIVED) = 'MCC_FIELD_MAPPING' THEN
        v_sql_mcc := 'SELECT FIELD_VALUE_MAPPING';
        FOR i IN 1 .. v_count LOOP
            v_sql_mcc := v_sql_mcc || ', ' || v_field_names(i);
        END LOOP;

        v_sql_mcc := v_sql_mcc || ' FROM STAGING.MCC_FIELD_MAPPING
            WHERE INTERFACE_FILE = ''' || v_INTERFACE_FILE || '''
              AND TRUNC(EFFECTIVE_DATE) = TO_DATE(''' || TO_CHAR(v_EFFECTIVE_DATE, 'DD-MON-YYYY') || ''', ''DD-MON-YYYY'')
              AND ACTIVE = ''A''';

        DBMS_OUTPUT.PUT_LINE('🛠 MCC SQL:');
        DBMS_OUTPUT.PUT_LINE(v_sql_mcc);

        OPEN v_cursor FOR v_sql_mcc;
        LOOP
            FETCH v_cursor INTO v_f1, v_f2, v_f3;
            EXIT WHEN v_cursor%NOTFOUND;
            DBMS_OUTPUT.PUT_LINE('✅ MCC_MAPPING => ' || v_f1 || ' | ' || v_f2 || ' | ' || v_f3);
        END LOOP;
        CLOSE v_cursor;
    END IF;

END;
/
