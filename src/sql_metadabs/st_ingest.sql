CREATE OR REFRESH STREAMING TABLE IDENTIFIER(CONCAT({{catalog_name}},'.', {{schema_name}}, '.', {{table_name}})) AS
SELECT *
    , _metadata
    , current_timestamp() as _etl_timestamp
FROM STREAM read_files(
    {{ file_path }},
    format => {{ file_format }},
    inferSchema => {{ infer_schema }},
    header => {{ headers_included }},
    sep => {{ line_sep }},
    fileNamePattern => {{ file_name_pattern }},
    schema => {{ schema }}
);

