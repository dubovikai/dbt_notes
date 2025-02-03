from google.cloud import bigquery


def compare_tables(table_name: str, environment: str):
    """
    Compares two tables number of rows, columns and volume
    Arg:
        table_name (str): BQ Table name
        environment (str): "production"/"staging"
    Returns comparison log to console.
    """
    print('---------------------------------------------')
    print(f'Table comparison: {table_name} ({environment})')
    origin_table_name = f"table_id_1"
    dbt_table_name = f"table_id_2"
    bigquery_client = bigquery.Client(project='...')
    origin_table = bigquery_client.get_table(origin_table_name)
    dbt_table = bigquery_client.get_table(dbt_table_name)
    if origin_table.num_rows == dbt_table.num_rows:
        print("Row nums - Ok")
    else:
        print(f"Rows: {origin_table.num_rows - dbt_table.num_rows:,d}")

    if origin_table.num_bytes == dbt_table.num_bytes:
        print("Bytes nums - Ok")
    else:
        print(f"Bytes: {origin_table.num_bytes - dbt_table.num_bytes:,d}")

    origin_columns = {field.name for field in origin_table.schema}
    dbt_columns = {field.name for field in dbt_table.schema}
    if all((origin_col in dbt_columns for origin_col in origin_columns)):
        print("Cols - Ok")
    else:
        print(f"Missed cols: {[origin_col for origin_col in origin_columns if origin_col not in dbt_columns]}")


if __name__ == "__main__":
    compare_tables("...", "staging")
