"""
Cost optimization tools for Snowflake
"""

import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_connection import snowflake_conn
from queries.optimization_queries import (
    get_warehouse_credit_usage,
    get_cost_per_query,
    get_expensive_queries,
    get_user_activity_summary
)

def analyze_warehouse_costs(days_back: int = 7) -> str:
    """
    Analyze credit consumption and costs by warehouse.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Analysis of warehouse costs with optimization recommendations
    """
    try:
        credit_price = snowflake_conn.get_credit_price()
        query = get_warehouse_credit_usage(days_back).format(credit_price=credit_price)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No warehouse usage data found in the last {days_back} days."
        
        result = f"## Warehouse Cost Analysis (Last {days_back} Days)\n"
        result += f"*Credit Price: ${credit_price:.2f}*\n\n"
        
        total_cost = df['ESTIMATED_COST'].sum()
        total_credits = df['CREDITS_USED_COMPUTE_SUM'].sum()
        
        result += f"**Total Cost: ${total_cost:.2f}**\n"
        result += f"**Total Credits: {total_credits:.2f}**\n\n"
        
        result += "### Warehouse Breakdown:\n\n"
        
        for idx, row in df.iterrows():
            warehouse = row['WAREHOUSE_NAME']
            credits = row['CREDITS_USED_COMPUTE_SUM']
            cost = row['ESTIMATED_COST']
            avg_credits = row['AVG_CREDITS_PER_HOUR']
            active_hours = row['ACTIVE_HOURS']
            
            cost_percentage = (cost / total_cost * 100) if total_cost > 0 else 0
            
            result += f"**{warehouse}:**\n"
            result += f"- Cost: ${cost:.2f} ({cost_percentage:.1f}% of total)\n"
            result += f"- Credits Used: {credits:.2f}\n"
            result += f"- Active Hours: {active_hours}\n"
            result += f"- Avg Credits/Hour: {avg_credits:.2f}\n\n"
        
        # Recommendations
        result += "## Cost Optimization Recommendations:\n\n"
        
        # Find most expensive warehouse
        most_expensive = df.iloc[0]
        result += f"- **{most_expensive['WAREHOUSE_NAME']}** is your most expensive warehouse (${most_expensive['ESTIMATED_COST']:.2f})\n"
        
        # Check for idle warehouses
        low_utilization = df[df['AVG_CREDITS_PER_HOUR'] < 0.1]
        if not low_utilization.empty:
            result += f"- Consider auto-suspend settings for low-utilization warehouses: {', '.join(low_utilization['WAREHOUSE_NAME'].tolist())}\n"
        
        # High credit consumption per hour
        high_usage = df[df['AVG_CREDITS_PER_HOUR'] > 2.0]
        if not high_usage.empty:
            result += f"- Review sizing for high credit/hour warehouses: {', '.join(high_usage['WAREHOUSE_NAME'].tolist())}\n"
        
        result += f"- Projected monthly cost at current rate: ${(total_cost / days_back * 30):.2f}\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing warehouse costs: {str(e)}"

def analyze_cost_per_query(days_back: int = 30) -> str:
    """
    Calculate and analyze cost per query by warehouse.
    
    Args:
        days_back: Number of days to look back (default: 30)
    
    Returns:
        Analysis of cost efficiency per query
    """
    try:
        credit_price = snowflake_conn.get_credit_price()
        query = get_cost_per_query(days_back).format(credit_price=credit_price)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No cost per query data found in the last {days_back} days."
        
        result = f"## Cost Per Query Analysis (Last {days_back} Days)\n\n"
        
        # Calculate overall metrics
        total_queries = df['QUERY_COUNT'].sum()
        total_cost = df['TOTAL_COST'].sum()
        overall_cost_per_query = total_cost / total_queries if total_queries > 0 else 0
        
        result += f"**Overall Metrics:**\n"
        result += f"- Total Queries: {total_queries:,}\n"
        result += f"- Total Cost: ${total_cost:.2f}\n"
        result += f"- Average Cost per Query: ${overall_cost_per_query:.4f}\n\n"
        
        result += "### By Warehouse:\n\n"
        result += "| Warehouse | Queries | Total Cost | Cost/Query |\n"
        result += "|-----------|---------|------------|------------|\n"
        
        for _, row in df.iterrows():
            warehouse = row['WAREHOUSE_NAME'] if row['WAREHOUSE_NAME'] else 'Unknown'
            queries = row['QUERY_COUNT'] if row['QUERY_COUNT'] else 0
            cost = row['TOTAL_COST'] if row['TOTAL_COST'] else 0
            cost_per_query = row['COST_PER_QUERY'] if row['COST_PER_QUERY'] else 0
            
            result += f"| {warehouse} | {queries:,} | ${cost:.2f} | ${cost_per_query:.4f} |\n"
        
        # Analysis and recommendations
        result += "\n## Analysis:\n\n"
        
        # Find most/least efficient warehouses
        efficient_df = df[df['COST_PER_QUERY'].notna() & (df['COST_PER_QUERY'] > 0)].sort_values('COST_PER_QUERY')
        
        if not efficient_df.empty:
            most_efficient = efficient_df.iloc[0]
            least_efficient = efficient_df.iloc[-1]
            
            result += f"- **Most efficient:** {most_efficient['WAREHOUSE_NAME']} (${most_efficient['COST_PER_QUERY']:.4f}/query)\n"
            result += f"- **Least efficient:** {least_efficient['WAREHOUSE_NAME']} (${least_efficient['COST_PER_QUERY']:.4f}/query)\n"
            
            efficiency_ratio = least_efficient['COST_PER_QUERY'] / most_efficient['COST_PER_QUERY']
            result += f"- **Efficiency gap:** {efficiency_ratio:.1f}x difference\n\n"
        
        result += "## Recommendations:\n\n"
        result += "- Focus optimization efforts on warehouses with highest cost/query\n"
        result += "- Consider workload consolidation for low-volume, high-cost warehouses\n"
        result += "- Review warehouse sizing for cost-inefficient warehouses\n"
        result += "- Implement query result caching to reduce redundant execution costs\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing cost per query: {str(e)}"

def find_expensive_queries(days_back: int = 7, limit: int = 25) -> str:
    """
    Identify the most credit-consuming queries.
    
    Args:
        days_back: Number of days to look back (default: 7)
        limit: Maximum number of queries to return (default: 25)
    
    Returns:
        List of most expensive queries with cost details
    """
    try:
        query = get_expensive_queries(days_back, limit)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No expensive queries found in the last {days_back} days."
        
        credit_price = snowflake_conn.get_credit_price()
        
        result = f"## Most Expensive Queries (Last {days_back} Days)\n\n"
        
        total_credits = df['CREDITS_USED_CLOUD_SERVICES'].sum()
        total_cost = total_credits * credit_price
        
        result += f"**Top {limit} queries consumed {total_credits:.4f} credits (${total_cost:.2f})**\n\n"
        
        for idx, row in df.head(10).iterrows():
            credits = row['CREDITS_USED_CLOUD_SERVICES']
            cost = credits * credit_price
            execution_time = row['EXECUTION_SECONDS']
            
            result += f"**Query {idx + 1}:**\n"
            result += f"- Cost: ${cost:.4f} ({credits:.4f} credits)\n"
            result += f"- Execution Time: {execution_time:.2f} seconds\n"
            result += f"- Warehouse: {row['WAREHOUSE_NAME']}\n"
            result += f"- User: {row['USER_NAME']}\n"
            result += f"- Bytes Scanned: {row['BYTES_SCANNED']:,}\n"
            result += f"- Rows Produced: {row['ROWS_PRODUCED']:,}\n"
            
            # Show truncated query
            query_text = str(row['QUERY_TEXT'])[:200]
            if len(str(row['QUERY_TEXT'])) > 200:
                query_text += "..."
            result += f"- Query: `{query_text}`\n\n"
        
        # Recommendations
        result += "## Optimization Recommendations:\n\n"
        
        avg_credits = df['CREDITS_USED_CLOUD_SERVICES'].mean()
        high_credit_queries = df[df['CREDITS_USED_CLOUD_SERVICES'] > avg_credits * 2]
        
        if not high_credit_queries.empty:
            result += f"- {len(high_credit_queries)} queries use >2x average credits - prioritize these for optimization\n"
        
        # Check for large scans
        large_scans = df[df['BYTES_SCANNED'] > 1000000000]  # 1GB
        if not large_scans.empty:
            result += f"- {len(large_scans)} queries scan >1GB - consider clustering/partitioning\n"
        
        # Check for long-running expensive queries
        long_expensive = df[(df['EXECUTION_SECONDS'] > 60) & (df['CREDITS_USED_CLOUD_SERVICES'] > avg_credits)]
        if not long_expensive.empty:
            result += f"- {len(long_expensive)} queries are both slow (>1min) and expensive - high optimization priority\n"
        
        result += "- Consider query result caching for frequently executed expensive queries\n"
        result += "- Review warehouse sizing for consistently expensive operations\n"
        
        return result
        
    except Exception as e:
        return f"Error finding expensive queries: {str(e)}"

def analyze_user_costs(days_back: int = 7) -> str:
    """
    Analyze resource consumption and costs by user.
    
    Args:
        days_back: Number of days to look back (default: 7)
        
    Returns:
        Analysis of user activity and associated costs
    """
    try:
        query = get_user_activity_summary(days_back)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No user activity data found in the last {days_back} days."
        
        credit_price = snowflake_conn.get_credit_price()
        
        result = f"## User Cost Analysis (Last {days_back} Days)\n\n"
        
        # Calculate costs
        df['ESTIMATED_COST'] = df['TOTAL_CREDITS_USED'] * credit_price
        
        total_cost = df['ESTIMATED_COST'].sum()
        total_queries = df['TOTAL_QUERIES'].sum()
        
        result += f"**Summary:**\n"
        result += f"- Total Users: {len(df)}\n"
        result += f"- Total Queries: {total_queries:,}\n"
        result += f"- Total Estimated Cost: ${total_cost:.2f}\n\n"
        
        result += "### Top Users by Cost:\n\n"
        result += "| User | Queries | Total Cost | Avg Time/Query | Warehouses |\n"
        result += "|------|---------|------------|----------------|------------|\n"
        
        for _, row in df.head(10).iterrows():
            user = row['USER_NAME']
            queries = row['TOTAL_QUERIES']
            cost = row['ESTIMATED_COST']
            avg_time = row['AVG_EXECUTION_SECONDS']
            warehouses = row['WAREHOUSES_USED']
            
            result += f"| {user} | {queries:,} | ${cost:.2f} | {avg_time:.1f}s | {warehouses} |\n"
        
        # Analysis
        result += "\n## Analysis:\n\n"
        
        # Find power users
        power_users = df[df['TOTAL_QUERIES'] > df['TOTAL_QUERIES'].quantile(0.8)]
        result += f"- **Power users (top 20%):** {len(power_users)} users account for {power_users['TOTAL_QUERIES'].sum():,} queries\n"
        
        # Find high-cost users
        high_cost_users = df[df['ESTIMATED_COST'] > df['ESTIMATED_COST'].quantile(0.8)]
        cost_concentration = high_cost_users['ESTIMATED_COST'].sum() / total_cost * 100
        result += f"- **High-cost users (top 20%):** Account for {cost_concentration:.1f}% of total cost\n"
        
        # Multi-warehouse users
        multi_warehouse = df[df['WAREHOUSES_USED'] > 3]
        if not multi_warehouse.empty:
            result += f"- **Multi-warehouse users:** {len(multi_warehouse)} users access 4+ warehouses\n"
        
        result += "\n## Recommendations:\n\n"
        result += "- Engage with high-cost users on query optimization best practices\n"
        result += "- Provide training for users with long average query times\n"
        result += "- Consider workload management policies for power users\n"
        result += "- Review access patterns for multi-warehouse users\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing user costs: {str(e)}"