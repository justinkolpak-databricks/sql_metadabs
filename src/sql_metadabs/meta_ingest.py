from databricks.bundles.jobs import task, job, sql_query_task, notebook_task, resource_generator
from databricks.bundles.variables import Bundle

def create_job(catalog_name: str, schema_name: str, table_name:str):
    @sql_query_task(query_id = '4ba978fd-0aa2-4204-801d-d50291a529b4', warehouse_id = Bundle.variables.warehouse_id)
    def query_task(catalog_name: str, schema_name: str, table_name: str):
        pass

    @job(name=f"ingest_{table_name}", resource_name=f"ingest_{table_name}")
    def ingest_tables():
        query_task(catalog_name=catalog_name, schema_name=schema_name,table_name=table_name)

    
    return ingest_tables

@resource_generator
def create_sql_query_jobs():
    catalog_name = 'justin_kolpak'
    schema_name = 'tpch'
    tables = ['customer','fact_orders']
    for table in tables:
        yield create_job(catalog_name, schema_name, table)