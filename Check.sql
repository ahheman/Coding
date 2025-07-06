SET SERVEROUTPUT ON;

DECLARE
    v_sql   VARCHAR2(1000);
    v_val   VARCHAR2(4000);
    v_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    v_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    v_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');
BEGIN
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
            v_sql := '
                SELECT ' || col_rec.column_name || '
                FROM STAGING.MAPPING_VALUE
                WHERE FIELD_TO_BE_DERIVED = :1
                  AND INTERFACE_FILE = :2
                  AND TRUNC(EFFECTIVE_DATE) = :3
                  AND ACTIVE = ''A''
                  AND ' || col_rec.column_name || ' IS NOT NULL
                  AND ROWNUM = 1';

            DBMS_OUTPUT.PUT_LINE('Trying column: ' || col_rec.column_name);
            DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);

            EXECUTE IMMEDIATE v_sql INTO v_val
            USING v_FIELD_TO_BE_DERIVED, v_INTERFACE_FILE, v_EFFECTIVE_DATE;

            DBMS_OUTPUT.PUT_LINE('✅ Value = ' || v_val);

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('⛔ No data found for: ' || col_rec.column_name);
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('🔥 Error for ' || col_rec.column_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/
