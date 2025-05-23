def generate_filtered_replace_script(chunk_keys, filter_key, partition_mapping):
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
    return query, exists_conditions
