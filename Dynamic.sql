CREATE OR REPLACE PROCEDURE STAGING.DYNAMICMAPPINGVALUE (
    p_FIELD_TO_BE_DERIVED     IN  VARCHAR2,
    p_INTERFACE_FILE          IN  VARCHAR2,
    p_EFFECTIVE_DATE          IN  DATE,
    o_PRODUCT_CURSOR          OUT SYS_REFCURSOR,
    o_MCC_CURSOR              OUT SYS_REFCURSOR
)
AS
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql_product   CLOB;
    v_sql_mcc       CLOB;
BEGIN
    -- Step 1: Fetch FIELD_MAP_X columns from STAGING.MAPPING_VALUE dynamically
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
            v_sql_product := 'SELECT ' || col_rec.column_name ||
                     ' FROM STAGING.MAPPING_VALUE
                      WHERE FIELD_TO_BE_DERIVED = :1
                        AND INTERFACE_FILE = :2
                        AND TRUNC(EFFECTIVE_DATE) = :3
                        AND ACTIVE = ''A''
                        AND ' || col_rec.column_name || ' IS NOT NULL
                        AND ROWNUM = 1';

            EXECUTE IMMEDIATE v_sql_product INTO v_val
            USING p_FIELD_TO_BE_DERIVED, p_INTERFACE_FILE, p_EFFECTIVE_DATE;

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

    IF v_count = 0 THEN
        OPEN o_PRODUCT_CURSOR FOR SELECT NULL AS FIELD_VALUE_MAPPING, NULL AS FIELD_MAP_1, NULL AS FIELD_MAP_2, NULL AS FIELD_MAP_3 FROM DUAL WHERE 1=0;
        OPEN o_MCC_CURSOR FOR SELECT NULL AS FIELD_VALUE_MAPPING, NULL AS FIELD_MAP_1, NULL AS FIELD_MAP_2, NULL AS FIELD_MAP_3 FROM DUAL WHERE 1=0;
        RETURN;
    END IF;

    -- Step 2: Build dynamic SQL for PRODUCT_FIELD_MAPPING
    v_sql_product := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_product := v_sql_product || ', ' || v_field_names(i);
    END LOOP;

    v_sql_product := v_sql_product || ' FROM STAGING.PRODUCT_FIELD_MAPPING
                        WHERE INTERFACE_FILE = :iface
                          AND TRUNC(EFFECTIVE_DATE) = :effdate
                          AND ACTIVE = ''A''';

    -- Step 3: Build dynamic SQL for MCC_FIELD_MAPPING
    v_sql_mcc := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_mcc := v_sql_mcc || ', ' || v_field_names(i);
    END LOOP;

    v_sql_mcc := v_sql_mcc || ' FROM STAGING.MCC_FIELD_MAPPING
                        WHERE INTERFACE_FILE = :iface
                          AND TRUNC(EFFECTIVE_DATE) = :effdate
                          AND ACTIVE = ''A''';

    -- Open cursors
    OPEN o_PRODUCT_CURSOR FOR v_sql_product USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    OPEN o_MCC_CURSOR FOR v_sql_mcc USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
END;
/
