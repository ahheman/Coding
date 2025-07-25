DECLARE
  v_results PKG_IFRS_UTL.mapping_table_type;
BEGIN
  PKG_IFRS_UTL.sp_getdynamicmappingvalues(
    p_field_to_be_derived => 'PRODUCT_FIELD_MAPPING',
    p_interface_file      => 'TEST_FILE',
    p_effective_date      => SYSDATE,
    o_result_rows         => v_results
  );

  -- Print output for verification
  FOR i IN 1 .. v_results.COUNT LOOP
    DBMS_OUTPUT.PUT_LINE('Row ' || i || ': FIELD_VALUE_MAPPING = ' || v_results(i).field_value_mapping);
    FOR j IN 1 .. v_results(i).field_map_values.COUNT LOOP
      DBMS_OUTPUT.PUT_LINE('  FIELD_MAP_' || j || ': ' || v_results(i).field_map_values(j));
    END LOOP;
  END LOOP;
END;
/