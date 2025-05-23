def generate_filtered_replace_script(chunk_keys, filter_key, partition_mapping, table_id, temp_table_id):
    """
    Generate a script to delete and replace rows in a table based on a filtered set of partitions.

    Args:
        chunk_keys (list): List of chunk keys to group by
        filter_key (str): The key to filter by
        partition_mapping (list): List of partition mappings, each containing 'min_filter_val' and 'max_filter_val'

    Returns:
        tuple: A tuple containing the SQL query and the exists conditions


    Example query might look like:
    DELETE FROM target_table t
    WHERE 
        (customer_id = '123' AND date >= '2025-01-01' AND date <= '2025-01-31')
        OR (customer_id = '456' AND date >= '2025-01-15' AND date <= '2025-02-28')

    INSERT INTO target_table
    SELECT *
    FROM temporary_source_table;
    """
    where_clauses = []
    for part in partition_mapping:
        chunk_conditions = " AND ".join(
            f"{key} = '{part[key]}'" for key in chunk_keys
        )
        chunk_conditions = chunk_conditions or "1=1"
        filter_condition = (
            f"{filter_key} >= '{part['min_filter_val']}' AND "
            f"{filter_key} <= '{part['max_filter_val']}'"
        )
        where_clauses.append(f"({chunk_conditions} AND {filter_condition})")

    exists_conditions = " OR ".join(where_clauses)

    query = """
    DELETE FROM `{table}` t
    WHERE 
        {exists_conditions};

    INSERT INTO `{table}`
    SELECT *
    FROM `{temp_table}`;
    """


    query = query.format(
        table=table_id,
        temp_table=temp_table_id,
        filter_key=filter_key,
        exists_conditions=exists_conditions
    )
    return query


def build_filtered_replace_partition_mapping(result):
    partition_mapping = []
    for page in result:
        for row in page:
            partition_group = {}
            for key, col_num in row._xxx_field_to_index.items():
                value = row._xxx_values[col_num]
                partition_group[key] = value

            partition_mapping.append(partition_group)
    return partition_mapping


def get_filtered_partition_ranges(client, chunk_keys, filter_key, temp_table):
    get_range_query = """
        SELECT {chunk_keys}, MIN({filter_key}) as min_filter_val, MAX({filter_key}) as max_filter_val
        FROM `{temp_table}`
        GROUP BY {chunk_keys}
    """
    chunk_keys_str = ', '.join(chunk_keys) if chunk_keys else "1=1"
    get_range_query = get_range_query.format(
        chunk_keys=chunk_keys_str,
        filter_key=filter_key,
        temp_table=temp_table
    )

    get_range_job = client.query(get_range_query)
    result = get_range_job.result().pages
    return result

