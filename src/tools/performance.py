"""
Performance analysis tools for Snowflake optimization
"""

import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_connection import snowflake_conn
from queries.optimization_queries import (
    get_slow_queries, 
    get_query_patterns, 
    get_execution_time_distribution
)

def analyze_slow_queries(hours_back: int = 24, limit: int = 50) -> str:
    """
    Find and analyze the slowest queries in your Snowflake account.
    
    Args:
        hours_back: Number of hours to look back (default: 24)
        limit: Maximum number of queries to return (default: 50)
    
    Returns:
        Formatted analysis of slow queries with recommendations
    """
    try:
        query = get_slow_queries(hours_back, limit)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No queries found in the last {hours_back} hours."
        
        # Format results
        result = f"## Slowest Queries (Last {hours_back} Hours)\n\n"
        result += f"Found {len(df)} slow queries:\n\n"
        
        for idx, row in df.head(10).iterrows():
            result += f"**Query {idx + 1}:**\n"
            result += f"- Execution Time: {row['EXECUTION_TIME_SECONDS']:.2f} seconds\n"
            result += f"- Warehouse: {row['WAREHOUSE_NAME']}\n"
            result += f"- User: {row['USER_NAME']}\n"
            result += f"- Bytes Scanned: {row['BYTES_SCANNED']:,}\n"
            result += f"- Query ID: {row['QUERY_ID']}\n"
            
            # Truncate long queries
            query_text = str(row['QUERY_TEXT'])[:200]
            if len(str(row['QUERY_TEXT'])) > 200:
                query_text += "..."
            result += f"- Query: `{query_text}`\n\n"
        
        # Add recommendations
        result += "## Recommendations:\n"
        if df['EXECUTION_TIME_SECONDS'].max() > 300:  # 5 minutes
            result += "- Consider enabling Query Acceleration Service for queries > 5 minutes\n"
        if df['BYTES_SCANNED'].mean() > 1000000000:  # 1GB
            result += "- Review table clustering and partitioning for large scans\n"
        if df['TOTAL_QUEUED_TIME'].mean() > 30000:  # 30 seconds
            result += "- Consider scaling up warehouses to reduce queue times\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing slow queries: {str(e)}"

def analyze_query_patterns(hours_back: int = 168, limit: int = 50) -> str:
    """
    Identify frequently repeated expensive query patterns.
    
    Args:
        hours_back: Number of hours to look back (default: 168 = 1 week)
        limit: Maximum number of patterns to return (default: 50)
    
    Returns:
        Analysis of repeated query patterns with optimization suggestions
    """
    try:
        query = get_query_patterns(hours_back, limit)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No repeated query patterns found in the last {hours_back} hours."
        
        result = f"## Repeated Query Patterns (Last {hours_back} Hours)\n\n"
        result += f"Found {len(df)} repeated query patterns:\n\n"
        
        total_wasted_time = 0
        
        for idx, row in df.head(10).iterrows():
            execution_count = row['EXECUTION_COUNT']
            avg_time = row['AVG_TIME_SECONDS']
            total_time = row['TOTAL_TIME_SECONDS']
            
            # Calculate potential savings if optimized by 50%
            potential_savings = total_time * 0.5
            total_wasted_time += potential_savings
            
            result += f"**Pattern {idx + 1}:**\n"
            result += f"- Executions: {execution_count}\n"
            result += f"- Avg Time: {avg_time:.2f} seconds\n"
            result += f"- Total Time: {total_time:.2f} seconds\n"
            result += f"- Potential 50% Savings: {potential_savings:.2f} seconds\n"
            result += f"- Warehouse: {row['WAREHOUSE_NAME']}\n"
            
            # Show sample query
            query_text = str(row['SAMPLE_QUERY_TEXT'])[:150]
            if len(str(row['SAMPLE_QUERY_TEXT'])) > 150:
                query_text += "..."
            result += f"- Sample Query: `{query_text}`\n\n"
        
        result += f"## Summary:\n"
        result += f"- Total potential time savings: {total_wasted_time:.2f} seconds\n"
        result += f"- Equivalent to: {total_wasted_time/3600:.2f} hours\n\n"
        
        result += "## Optimization Recommendations:\n"
        result += "- Cache results for frequently repeated queries\n"
        result += "- Create materialized views for common aggregations\n"
        result += "- Consider query result caching settings\n"
        result += "- Review and optimize the most repeated patterns first\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing query patterns: {str(e)}"

def analyze_execution_time_distribution(days_back: int = 7) -> str:
    """
    Analyze the distribution of query execution times.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Analysis of query execution time distribution
    """
    try:
        query = get_execution_time_distribution(days_back)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No query data found in the last {days_back} days."
        
        result = f"## Query Execution Time Distribution (Last {days_back} Days)\n\n"
        
        total_queries = df['QUERY_COUNT'].sum()
        result += f"Total queries analyzed: {total_queries:,}\n\n"
        
        result += "| Time Range | Query Count | Percentage |\n"
        result += "|------------|-------------|------------|\n"
        
        for _, row in df.iterrows():
            bucket = row['EXECUTION_TIME_BUCKET']
            count = row['QUERY_COUNT']
            percentage = row['PERCENTAGE']
            result += f"| {bucket} | {count:,} | {percentage}% |\n"
        
        # Analysis and recommendations
        result += "\n## Analysis:\n"
        
        quick_queries = df[df['EXECUTION_TIME_BUCKET'] == 'Less than 1 second']['PERCENTAGE'].iloc[0] if len(df[df['EXECUTION_TIME_BUCKET'] == 'Less than 1 second']) > 0 else 0
        slow_queries = df[df['EXECUTION_TIME_BUCKET'].str.contains('minutes', na=False)]['PERCENTAGE'].sum()
        
        result += f"- **Quick queries (<1s):** {quick_queries}%\n"
        result += f"- **Slow queries (>1min):** {slow_queries}%\n\n"
        
        result += "## Recommendations:\n"
        
        if slow_queries > 10:
            result += f"- {slow_queries}% of queries take >1 minute - investigate these for optimization\n"
        if quick_queries < 50:
            result += "- Consider query result caching to improve response times\n"
        
        result += "- Focus optimization efforts on the longest-running query categories\n"
        result += "- Monitor query performance trends over time\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing execution time distribution: {str(e)}"