from databricks.bundles.jobs import task, job, sql_query_task, notebook_task, resource_generator
from databricks.bundles.variables import Bundle

def create_job(catalog_name: str, 
                schema_name: str,
                table_name:str,
                file_path: str,
                file_format: str,
                infer_schema: bool,
                headers_included: bool,
                line_sep: str,
                file_name_pattern: str,
                schema: str,                  
                ):
    @sql_query_task(query_id = '4ba978fd-0aa2-4204-801d-d50291a529b4', warehouse_id = Bundle.variables.warehouse_id)
    def query_task(catalog_name: str, 
                schema_name: str, 
                table_name: str,
                file_path: str,
                file_format: str,
                infer_schema: bool,
                headers_included: bool,
                line_sep: str,
                file_name_pattern: str,
                schema: str,):
        pass

    @job(name=f"ingest_{table_name}", resource_name=f"ingest_{table_name}")
    def ingest_tables():
        query_task(catalog_name=catalog_name, 
                    schema_name=schema_name,
                    table_name=table_name,
                    file_path=file_path,
                    file_format=file_format,
                    infer_schema=infer_schema,
                    headers_included=headers_included,
                    line_sep=line_sep,
                    file_name_pattern=file_name_pattern,
                    schema=schema)

    return ingest_tables

@resource_generator
def create_sql_query_jobs():
    catalog_name = 'justin_kolpak'
    schema_name = 'tpch'
    
    table_names = ['hr']
    for table_name in table_names:
        yield create_job(catalog_name = catalog_name,
                            schema_name = schema_name,
                            table_name = table_name,
                            file_path='/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/sf=10/Batch1/',
                            file_format='csv',
                            infer_schema=False,
                            headers_included=False,
                            line_sep=",",
                            file_name_pattern="HR.csv",
                            schema="employeeid BIGINT COMMENT 'ID of employee', managerid BIGINT COMMENT 'ID of employee’s manager', employeefirstname STRING COMMENT 'First name', employeelastname STRING COMMENT 'Last name', employeemi STRING COMMENT 'Middle initial', employeejobcode STRING COMMENT 'Numeric job code', employeebranch STRING COMMENT 'Facility in which employee has office', employeeoffice STRING COMMENT 'Office number or description', employeephone STRING COMMENT 'Employee phone number'"
                         )