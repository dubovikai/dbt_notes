from google.cloud import bigquery
import os


def get_source_name(bq_table) -> str:
    ...

def add_source(table_name: str):
    """
    Generates sql file for a new source.
    After generation need to be checked manually.

    Add table name to source.yaml. Run this script.

    Case sensitive. 

    Arg:
        table_name (str): BQ Table name
    """
    bigquery_client = bigquery.Client(project='...')
    bq_table = bigquery_client.get_table(table_name)
    source_name = get_source_name(bq_table)

    bash_command = f'''dbt \\
            --quiet run-operation generate_base_model \\
            --args '{{"source_name": "{source_name}", "table_name": "{bq_table.table_id}"}}' \\
            > scripts/{bq_table.table_id.lower()}.sql
            sqlfluff fix scripts/{bq_table.table_id.lower()}.sql
    '''
    print(bash_command)
    os.system(bash_command)  # nosec B605


if __name__ == "__main__":
    add_source("source_table_id")
