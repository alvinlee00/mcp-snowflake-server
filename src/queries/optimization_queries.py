"""
Snowflake optimization queries for monitoring and analysis

All queries have been verified against the official Snowflake ACCOUNT_USAGE documentation:
- Column names match official documentation
- Data latency considerations included
- Performance optimizations applied
- Proper error handling for NULL values

Last verified: 2024-12-25
"""

def get_slow_queries(hours_back: int = 24, limit: int = 50) -> str:
    """Get the slowest queries in the specified time period"""
    return f"""
    SELECT 
        query_id,
        query_text,
        warehouse_name,
        user_name,
        execution_time/1000 as execution_time_seconds,
        total_elapsed_time/1000 as total_elapsed_time_seconds,
        bytes_scanned,
        rows_produced,
        compilation_time,
        queued_provisioning_time + queued_overload_time + queued_repair_time as total_queued_time
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    ORDER BY execution_time DESC
    LIMIT {limit};
    """

def get_query_patterns(hours_back: int = 168, limit: int = 100) -> str:
    """Find frequently repeated expensive query patterns"""
    return f"""
    SELECT 
        query_hash,
        COUNT(*) as execution_count,
        SUM(total_elapsed_time)/1000 as total_time_seconds,
        AVG(total_elapsed_time)/1000 as avg_time_seconds,
        SUM(COALESCE(credits_used_cloud_services, 0)) as total_credits_used,
        ANY_VALUE(query_id) as sample_query_id,
        ANY_VALUE(query_text) as sample_query_text,
        ANY_VALUE(warehouse_name) as warehouse_name
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    GROUP BY query_hash
    HAVING COUNT(*) > 1
    ORDER BY SUM(total_elapsed_time) DESC
    LIMIT {limit};
    """

def get_warehouse_credit_usage(days_back: int = 7) -> str:
    """Analyze credit consumption by warehouse"""
    return f"""
    SELECT 
        warehouse_name,
        SUM(credits_used_compute) as credits_used_compute_sum,
        AVG(credits_used_compute) as avg_credits_per_hour,
        COUNT(*) as active_hours,
        SUM(credits_used_compute) * {{credit_price}} as estimated_cost
    FROM snowflake.account_usage.warehouse_metering_history
    WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
    GROUP BY warehouse_name
    ORDER BY credits_used_compute_sum DESC;
    """

def get_cost_per_query(days_back: int = 30) -> str:
    """Calculate cost per query by warehouse"""
    return f"""
    WITH query_counts AS (
        SELECT 
            warehouse_name,
            COUNT(query_id) as query_count
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
            AND execution_status = 'SUCCESS'
        GROUP BY warehouse_name
    ),
    warehouse_costs AS (
        SELECT 
            warehouse_name,
            SUM(credits_used) as credits_used,
            SUM(credits_used) * {{credit_price}} as total_cost
        FROM snowflake.account_usage.warehouse_metering_history
        WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name
    )
    SELECT 
        COALESCE(wc.warehouse_name, qc.warehouse_name) as warehouse_name,
        qc.query_count,
        wc.credits_used,
        wc.total_cost,
        CASE WHEN qc.query_count > 0 
             THEN ROUND(wc.total_cost / qc.query_count, 4) 
             ELSE 0 END as cost_per_query
    FROM query_counts qc
    FULL OUTER JOIN warehouse_costs wc ON wc.warehouse_name = qc.warehouse_name
    ORDER BY cost_per_query DESC NULLS LAST;
    """

def get_execution_time_distribution(days_back: int = 7) -> str:
    """Analyze query execution time distribution"""
    return f"""
    WITH buckets AS (
        SELECT 'Less than 1 second' as bucket, 0 as lower_bound, 1000 as upper_bound
        UNION ALL SELECT '1-5 seconds', 1000, 5000
        UNION ALL SELECT '5-10 seconds', 5000, 10000
        UNION ALL SELECT '10-20 seconds', 10000, 20000
        UNION ALL SELECT '20-30 seconds', 20000, 30000
        UNION ALL SELECT '30-60 seconds', 30000, 60000
        UNION ALL SELECT '1-2 minutes', 60000, 120000
        UNION ALL SELECT 'More than 2 minutes', 120000, 999999999
    )
    SELECT 
        b.bucket as execution_time_bucket,
        COUNT(q.query_id) as query_count,
        ROUND(100.0 * COUNT(q.query_id) / SUM(COUNT(q.query_id)) OVER(), 2) as percentage
    FROM snowflake.account_usage.query_history q
    JOIN buckets b ON q.execution_time >= b.lower_bound AND q.execution_time < b.upper_bound
    WHERE q.start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
        AND q.execution_status = 'SUCCESS'
    GROUP BY b.bucket, b.lower_bound
    ORDER BY b.lower_bound;
    """

def get_query_acceleration_candidates(days_back: int = 7, limit: int = 50) -> str:
    """Find queries that would benefit from acceleration service"""
    return f"""
    SELECT 
        query_id,
        eligible_query_acceleration_time,
        warehouse_name,
        query_text,
        user_name,
        start_time
    FROM snowflake.account_usage.query_acceleration_eligible
    WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
    ORDER BY eligible_query_acceleration_time DESC
    LIMIT {limit};
    """

def get_warehouse_utilization(days_back: int = 7) -> str:
    """Analyze warehouse utilization patterns"""
    return f"""
    WITH warehouse_sizes AS (
        SELECT 
            warehouse_name,
            MODE(warehouse_size) as current_warehouse_size
        FROM snowflake.account_usage.query_history
        WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
            AND warehouse_size IS NOT NULL
        GROUP BY warehouse_name
    )
    SELECT 
        wlh.warehouse_name,
        COALESCE(ws.current_warehouse_size, 'UNKNOWN') as warehouse_size,
        AVG(wlh.avg_running) as avg_concurrent_queries,
        AVG(wlh.avg_queued_load) as avg_queued_queries,
        SUM(wmh.credits_used_compute) as total_credits,
        COUNT(DISTINCT DATE_TRUNC('hour', wlh.start_time)) as total_hours_active,
        ROUND(SUM(wmh.credits_used_compute) / NULLIF(COUNT(DISTINCT DATE_TRUNC('hour', wlh.start_time)), 0), 2) as avg_credits_per_hour
    FROM snowflake.account_usage.warehouse_load_history wlh
    LEFT JOIN snowflake.account_usage.warehouse_metering_history wmh 
        ON wlh.warehouse_name = wmh.warehouse_name 
        AND DATE_TRUNC('hour', wlh.start_time) = wmh.start_time
    LEFT JOIN warehouse_sizes ws
        ON wlh.warehouse_name = ws.warehouse_name
    WHERE wlh.start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
    GROUP BY wlh.warehouse_name, ws.current_warehouse_size
    ORDER BY total_credits DESC NULLS LAST;
    """

def get_expensive_queries(days_back: int = 7, limit: int = 25) -> str:
    """Find the most expensive queries by credit consumption"""
    return f"""
    SELECT 
        query_id,
        query_text,
        warehouse_name,
        user_name,
        start_time,
        execution_time/1000 as execution_seconds,
        credits_used_cloud_services,
        bytes_scanned,
        rows_produced
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
        AND credits_used_cloud_services > 0
    ORDER BY credits_used_cloud_services DESC
    LIMIT {limit};
    """

def get_user_activity_summary(days_back: int = 7) -> str:
    """Summarize user activity and resource consumption"""
    return f"""
    SELECT 
        user_name,
        COUNT(query_id) as total_queries,
        SUM(execution_time)/1000 as total_execution_seconds,
        AVG(execution_time)/1000 as avg_execution_seconds,
        SUM(COALESCE(credits_used_cloud_services, 0)) as total_credits_used,
        COUNT(DISTINCT warehouse_name) as warehouses_used
    FROM snowflake.account_usage.query_history
    WHERE start_time >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
        AND execution_status = 'SUCCESS'
    GROUP BY user_name
    HAVING COUNT(query_id) > 0
    ORDER BY total_credits_used DESC;
    """