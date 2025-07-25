CREATE OR REPLACE PACKAGE PKG_IFRS_UTL AS
  TYPE string_list IS TABLE OF VARCHAR2(4000);

  TYPE mapping_row_type IS RECORD (
    field_value_mapping VARCHAR2(4000),
    field_map_values    string_list
  );

  TYPE mapping_table_type IS TABLE OF mapping_row_type;

  PROCEDURE sp_getdynamicmappingvalues (
    p_field_to_be_derived IN VARCHAR2,
    p_interface_file      IN VARCHAR2,
    p_effective_date      IN DATE,
    o_result_rows         OUT mapping_table_type
  );
END PKG_IFRS_UTL;
/
