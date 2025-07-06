FOR col_rec IN (
    SELECT column_name
    FROM user_tab_columns
    WHERE table_name = 'MAPPING_VALUE' -- MUST be uppercase
      AND column_name LIKE 'FIELD_MAP_%'
    ORDER BY column_name
)
LOOP
    BEGIN
        v_sql := '
            SELECT ' || col_rec.column_name || '
            FROM MAPPING_VALUE
            WHERE FIELD_TO_BE_DERIVED = :1
              AND INTERFACE_FILE = :2
              AND TRUNC(EFFECTIVE_DATE) = :3
              AND ACTIVE = ''A''
              AND ' || col_rec.column_name || ' IS NOT NULL
              AND ROWNUM = 1';

        DBMS_OUTPUT.PUT_LINE('---');
        DBMS_OUTPUT.PUT_LINE('Trying column: ' || col_rec.column_name);
        DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);
        DBMS_OUTPUT.PUT_LINE('Params: ' || v_FIELD_TO_BE_DERIVED || ', ' || v_INTERFACE_FILE || ', ' || TO_CHAR(v_EFFECTIVE_DATE, 'DD-MON-YYYY'));

        EXECUTE IMMEDIATE v_sql INTO v_val
        USING v_FIELD_TO_BE_DERIVED, v_INTERFACE_FILE, v_EFFECTIVE_DATE;

        IF v_val IS NOT NULL THEN
            v_count := v_count + 1;
            v_field_names(v_count) := col_rec.column_name;
            v_field_values(v_count) := v_val;
            DBMS_OUTPUT.PUT_LINE('✅ Found: ' || col_rec.column_name || ' = ' || v_val);
        END IF;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('⛔ No data found for: ' || col_rec.column_name);
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('🔥 Error: ' || SQLERRM);
    END;
END LOOP;
