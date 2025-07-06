SET SERVEROUTPUT ON;

DECLARE
    v_exists NUMBER := 0;
    v_rowid  ROWID;

    -- To hold all 10 field_map values
    v_f1 VARCHAR2(100);
    v_f2 VARCHAR2(100);
    v_f3 VARCHAR2(100);
    v_f4 VARCHAR2(100);
    v_f5 VARCHAR2(100);
    v_f6 VARCHAR2(100);
    v_f7 VARCHAR2(100);
    v_f8 VARCHAR2(100);
    v_f9 VARCHAR2(100);
    v_f10 VARCHAR2(100);
BEGIN
    -- Step 1: Check if row even exists with these inputs
    SELECT COUNT(*) INTO v_exists
    FROM MAPPING_VALUE
    WHERE FIELD_TO_BE_DERIVED = 'PRODUCT_FIELD_MAPPING'
      AND INTERFACE_FILE = 'MAJESCO'
      AND EFFECTIVE_DATE = TO_DATE('01-JUL-2024', 'DD-MON-YYYY')
      AND ACTIVE = 'A';

    IF v_exists = 0 THEN
        DBMS_OUTPUT.PUT_LINE('❌ No row found in MAPPING_VALUE for given input values.');
        RETURN;
    END IF;

    DBMS_OUTPUT.PUT_LINE('✅ Row exists. Now fetching FIELD_MAP_X values:');

    -- Step 2: Fetch all field_map columns from the row
    SELECT ROWID INTO v_rowid
    FROM MAPPING_VALUE
    WHERE FIELD_TO_BE_DERIVED = 'PRODUCT_FIELD_MAPPING'
      AND INTERFACE_FILE = 'MAJESCO'
      AND EFFECTIVE_DATE = TO_DATE('01-JUL-2024', 'DD-MON-YYYY')
      AND ACTIVE = 'A'
      AND ROWNUM = 1;

    SELECT FIELD_MAP_1, FIELD_MAP_2, FIELD_MAP_3, FIELD_MAP_4, FIELD_MAP_5,
           FIELD_MAP_6, FIELD_MAP_7, FIELD_MAP_8, FIELD_MAP_9, FIELD_MAP_10
    INTO v_f1, v_f2, v_f3, v_f4, v_f5, v_f6, v_f7, v_f8, v_f9, v_f10
    FROM MAPPING_VALUE
    WHERE ROWID = v_rowid;

    -- Step 3: Print values to confirm non-null ones
    IF v_f1 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_1 = ' || v_f1); END IF;
    IF v_f2 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_2 = ' || v_f2); END IF;
    IF v_f3 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_3 = ' || v_f3); END IF;
    IF v_f4 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_4 = ' || v_f4); END IF;
    IF v_f5 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_5 = ' || v_f5); END IF;
    IF v_f6 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_6 = ' || v_f6); END IF;
    IF v_f7 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_7 = ' || v_f7); END IF;
    IF v_f8 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_8 = ' || v_f8); END IF;
    IF v_f9 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_9 = ' || v_f9); END IF;
    IF v_f10 IS NOT NULL THEN DBMS_OUTPUT.PUT_LINE('FIELD_MAP_10 = ' || v_f10); END IF;

    IF v_f1 IS NULL AND v_f2 IS NULL AND v_f3 IS NULL AND v_f4 IS NULL AND 
       v_f5 IS NULL AND v_f6 IS NULL AND v_f7 IS NULL AND v_f8 IS NULL AND
       v_f9 IS NULL AND v_f10 IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('⚠️ All FIELD_MAP_X columns are NULL!');
    END IF;

END;
/