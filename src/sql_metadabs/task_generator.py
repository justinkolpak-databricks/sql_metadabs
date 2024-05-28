from databricks.bundles.jobs import Job, notebook_task, sql_notebook_task, task

@sql_notebook_task(notebook_path="/src/sql_metadabs/sql_notebook.sql" , warehouse_id='475b94ddc7cd5211')
def ingestion_task(catalog_name: str, 
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
  pass

# Input (List): Table Names
# Output (Dictionary): Key-values of table name and Task object
def create_tasks(table_names):
  task_dict = {}
  for table_name in table_names:
    task = ingestion_task(
            catalog_name = 'justin_kolpak',
            schema_name = 'tpch',
            table_name = table_name,
            file_path='/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/sf=10/Batch1/',
            file_format='csv',
            infer_schema=False,
            headers_included=False,
            line_sep=",",
            file_name_pattern="HR.csv",
            schema="employeeid BIGINT COMMENT 'ID of employee', managerid BIGINT COMMENT 'ID of employee’s manager', employeefirstname STRING COMMENT 'First name', employeelastname STRING COMMENT 'Last name', employeemi STRING COMMENT 'Middle initial', employeejobcode STRING COMMENT 'Numeric job code', employeebranch STRING COMMENT 'Facility in which employee has office', employeeoffice STRING COMMENT 'Office number or description', employeephone STRING COMMENT 'Employee phone number'"
          ).with_task_key(table_name)
    
    task_dict[table_name] = task

  return task_dict

# Input (dictionary): Key-values of table name and Task object
# Output (dictionary): Key-values of table name and Task object - with dependencies added
def add_deps(tasks):
  for table_name in tasks:
    if table_name == 'hr': # to be automated, just for testing purposes
      depends_on_task_names = ['hr2','hr3']
    else:
      depends_on_task_names = []

    depends_on_tasks = [v for k, v in tasks.items() if k in depends_on_task_names]
      
    for task in depends_on_tasks:
      tasks[table_name] = tasks[table_name].add_depends_on(task)
  
  return tasks


## MAIN ##
if __name__ == '__main__':
  table_names = ['hr', 'hr2', 'hr3']
  tasks = create_tasks(table_names) # Need to create all tasks first before adding dependencies
  tasks_with_deps = add_deps(tasks)

  ingestion_job = Job.create(
    resource_name = "ingestion_job",
    name = "Ingestion Job",
    tasks = list(tasks_with_deps.values())
  )