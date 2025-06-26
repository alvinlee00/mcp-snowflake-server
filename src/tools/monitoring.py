"""
Monitoring and utilization tools for Snowflake
"""

import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_connection import snowflake_conn
from queries.optimization_queries import (
    get_warehouse_utilization,
    get_query_acceleration_candidates
)

def analyze_warehouse_utilization(days_back: int = 7) -> str:
    """
    Analyze warehouse utilization patterns and efficiency.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Analysis of warehouse utilization with sizing recommendations
    """
    try:
        query = get_warehouse_utilization(days_back)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No warehouse utilization data found in the last {days_back} days."
        
        result = f"## Warehouse Utilization Analysis (Last {days_back} Days)\n\n"
        
        total_credits = df['TOTAL_CREDITS'].sum()
        total_hours = df['TOTAL_HOURS_ACTIVE'].sum()
        
        result += f"**Overall Metrics:**\n"
        result += f"- Total Credits Consumed: {total_credits:.2f}\n"
        result += f"- Total Active Hours: {total_hours:,}\n"
        result += f"- Average Credits/Hour: {total_credits/total_hours:.2f}\n\n"
        
        result += "### Warehouse Details:\n\n"
        result += "| Warehouse | Size | Avg Concurrent | Avg Queued | Credits | Hours | Credits/Hr |\n"
        result += "|-----------|------|----------------|------------|---------|-------|------------|\n"
        
        for _, row in df.iterrows():
            warehouse = row['WAREHOUSE_NAME']
            size = row['WAREHOUSE_SIZE']
            concurrent = row['AVG_CONCURRENT_QUERIES']
            queued = row['AVG_QUEUED_QUERIES']
            credits = row['TOTAL_CREDITS']
            hours = row['TOTAL_HOURS_ACTIVE']
            credits_per_hour = row['AVG_CREDITS_PER_HOUR']
            
            result += f"| {warehouse} | {size} | {concurrent:.1f} | {queued:.1f} | {credits:.1f} | {hours} | {credits_per_hour:.2f} |\n"
        
        # Analysis and recommendations
        result += "\n## Utilization Analysis:\n\n"
        
        # Find underutilized warehouses
        underutilized = df[df['AVG_CONCURRENT_QUERIES'] < 0.5]
        if not underutilized.empty:
            result += f"**Underutilized warehouses ({len(underutilized)}):**\n"
            for _, row in underutilized.iterrows():
                result += f"- {row['WAREHOUSE_NAME']}: {row['AVG_CONCURRENT_QUERIES']:.1f} avg concurrent queries\n"
            result += "\n"
        
        # Find over-queued warehouses
        over_queued = df[df['AVG_QUEUED_QUERIES'] > 1.0]
        if not over_queued.empty:
            result += f"**Warehouses with queuing issues ({len(over_queued)}):**\n"
            for _, row in over_queued.iterrows():
                result += f"- {row['WAREHOUSE_NAME']}: {row['AVG_QUEUED_QUERIES']:.1f} avg queued queries\n"
            result += "\n"
        
        # Efficiency analysis
        result += "## Sizing Recommendations:\n\n"
        
        for _, row in df.iterrows():
            warehouse = row['WAREHOUSE_NAME']
            concurrent = row['AVG_CONCURRENT_QUERIES']
            queued = row['AVG_QUEUED_QUERIES']
            size = row['WAREHOUSE_SIZE']
            
            if concurrent < 0.3 and queued < 0.1:
                result += f"- **{warehouse}**: Consider downsizing from {size} (low utilization)\n"
            elif queued > 2.0:
                result += f"- **{warehouse}**: Consider upsizing from {size} (high queue times)\n"
            elif concurrent > 5.0 and queued < 0.5:
                result += f"- **{warehouse}**: Well-sized {size} warehouse (high utilization, low queuing)\n"
        
        result += "\n**General Recommendations:**\n"
        result += "- Set auto-suspend to 60 seconds for underutilized warehouses\n"
        result += "- Monitor query queuing during peak hours\n"
        result += "- Consider multi-cluster warehouses for highly variable workloads\n"
        result += "- Review warehouse sizing monthly based on utilization patterns\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing warehouse utilization: {str(e)}"

def find_query_acceleration_opportunities(days_back: int = 7, limit: int = 50) -> str:
    """
    Identify queries that would benefit from Query Acceleration Service.
    
    Args:
        days_back: Number of days to look back (default: 7)
        limit: Maximum number of queries to return (default: 50)
    
    Returns:
        List of queries eligible for acceleration with potential benefits
    """
    try:
        query = get_query_acceleration_candidates(days_back, limit)
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No query acceleration candidates found in the last {days_back} days."
        
        result = f"## Query Acceleration Opportunities (Last {days_back} Days)\n\n"
        
        total_eligible_time = df['ELIGIBLE_QUERY_ACCELERATION_TIME'].sum()
        total_eligible_hours = total_eligible_time / 3600000  # Convert ms to hours
        
        result += f"**Summary:**\n"
        result += f"- Queries eligible for acceleration: {len(df)}\n"
        result += f"- Total eligible acceleration time: {total_eligible_hours:.2f} hours\n"
        result += f"- Potential time savings: {total_eligible_hours * 0.5:.2f} hours (50% acceleration)\n\n"
        
        result += "### Top Acceleration Candidates:\n\n"
        
        for idx, row in df.head(20).iterrows():
            eligible_time_ms = row['ELIGIBLE_QUERY_ACCELERATION_TIME']
            eligible_time_seconds = eligible_time_ms / 1000
            eligible_time_minutes = eligible_time_seconds / 60
            
            warehouse = row['WAREHOUSE_NAME']
            user = row['USER_NAME']
            
            result += f"**Query {idx + 1}:**\n"
            result += f"- Eligible acceleration time: {eligible_time_minutes:.1f} minutes\n"
            result += f"- Warehouse: {warehouse}\n"
            result += f"- User: {user}\n"
            result += f"- Query ID: {row['QUERY_ID']}\n"
            
            # Show truncated query
            query_text = str(row['QUERY_TEXT'])[:150]
            if len(str(row['QUERY_TEXT'])) > 150:
                query_text += "..."
            result += f"- Query: `{query_text}`\n\n"
        
        # Analysis by warehouse
        warehouse_summary = df.groupby('WAREHOUSE_NAME').agg({
            'ELIGIBLE_QUERY_ACCELERATION_TIME': ['count', 'sum', 'mean']
        }).round(2)
        
        if not warehouse_summary.empty:
            result += "### Acceleration Opportunities by Warehouse:\n\n"
            result += "| Warehouse | Eligible Queries | Total Time (hrs) | Avg Time (min) |\n"
            result += "|-----------|------------------|------------------|----------------|\n"
            
            for warehouse in warehouse_summary.index:
                count = warehouse_summary.loc[warehouse, ('ELIGIBLE_QUERY_ACCELERATION_TIME', 'count')]
                total_time_ms = warehouse_summary.loc[warehouse, ('ELIGIBLE_QUERY_ACCELERATION_TIME', 'sum')]
                avg_time_ms = warehouse_summary.loc[warehouse, ('ELIGIBLE_QUERY_ACCELERATION_TIME', 'mean')]
                
                total_hours = total_time_ms / 3600000
                avg_minutes = avg_time_ms / 60000
                
                result += f"| {warehouse} | {count} | {total_hours:.2f} | {avg_minutes:.1f} |\n"
        
        result += "\n## Recommendations:\n\n"
        
        # Find warehouses with most opportunities
        if not warehouse_summary.empty:
            top_warehouse = warehouse_summary.idxmax()[('ELIGIBLE_QUERY_ACCELERATION_TIME', 'sum')]
            result += f"- **{top_warehouse}** has the most acceleration opportunities\n"
        
        # Check for patterns
        long_candidates = df[df['ELIGIBLE_QUERY_ACCELERATION_TIME'] > 300000]  # >5 minutes
        if not long_candidates.empty:
            result += f"- {len(long_candidates)} queries have >5 minutes eligible time - high priority for acceleration\n"
        
        result += "- Enable Query Acceleration Service for warehouses with frequent long-running queries\n"
        result += "- Monitor acceleration service usage and costs vs. time savings\n"
        result += "- Consider automatic query acceleration for eligible queries\n"
        
        return result
        
    except Exception as e:
        return f"Error finding query acceleration opportunities: {str(e)}"

def generate_optimization_report(days_back: int = 7) -> str:
    """
    Generate a comprehensive optimization report combining multiple analyses.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Comprehensive optimization report with key metrics and recommendations
    """
    try:
        credit_price = snowflake_conn.get_credit_price()
        
        result = f"# Snowflake Optimization Report\n"
        result += f"**Analysis Period:** Last {days_back} days\n"
        result += f"**Credit Price:** ${credit_price:.2f}\n"
        result += f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # Quick metrics
        result += "## Executive Summary\n\n"
        
        # Get warehouse costs
        try:
            cost_query = get_warehouse_credit_usage(days_back).format(credit_price=credit_price)
            cost_df = snowflake_conn.execute_query(cost_query)
            
            if not cost_df.empty:
                total_cost = cost_df['ESTIMATED_COST'].sum()
                total_credits = cost_df['CREDITS_USED_COMPUTE_SUM'].sum()
                projected_monthly = total_cost / days_back * 30
                
                result += f"- **Total Cost:** ${total_cost:.2f}\n"
                result += f"- **Total Credits:** {total_credits:.2f}\n"
                result += f"- **Projected Monthly:** ${projected_monthly:.2f}\n"
                result += f"- **Active Warehouses:** {len(cost_df)}\n\n"
        except:
            result += "- Cost data unavailable\n\n"
        
        # Get query metrics
        try:
            perf_query = get_slow_queries(days_back * 24, 1000)  # Convert days to hours
            perf_df = snowflake_conn.execute_query(perf_query)
            
            if not perf_df.empty:
                total_queries = len(perf_df)
                avg_execution = perf_df['EXECUTION_TIME_SECONDS'].mean()
                slow_queries = len(perf_df[perf_df['EXECUTION_TIME_SECONDS'] > 60])
                
                result += f"- **Total Queries Analyzed:** {total_queries:,}\n"
                result += f"- **Average Execution Time:** {avg_execution:.2f} seconds\n"
                result += f"- **Slow Queries (>1min):** {slow_queries:,} ({slow_queries/total_queries*100:.1f}%)\n\n"
        except:
            result += "- Query performance data unavailable\n\n"
        
        # Key recommendations
        result += "## Top Recommendations\n\n"
        
        # Cost optimization
        result += "### 💰 Cost Optimization\n"
        try:
            if not cost_df.empty:
                most_expensive = cost_df.iloc[0]
                result += f"- Review **{most_expensive['WAREHOUSE_NAME']}** warehouse (${most_expensive['ESTIMATED_COST']:.2f} cost)\n"
                
                low_util = cost_df[cost_df['AVG_CREDITS_PER_HOUR'] < 0.1]
                if not low_util.empty:
                    result += f"- Optimize auto-suspend for {len(low_util)} low-utilization warehouses\n"
        except:
            pass
        result += "- Implement query result caching for repeated queries\n"
        result += "- Review warehouse sizing based on utilization patterns\n\n"
        
        # Performance optimization
        result += "### ⚡ Performance Optimization\n"
        try:
            if not perf_df.empty and slow_queries > 0:
                result += f"- Optimize {slow_queries:,} slow queries (>1 minute execution time)\n"
        except:
            pass
        result += "- Consider Query Acceleration Service for long-running queries\n"
        result += "- Review table clustering and partitioning strategies\n"
        result += "- Implement workload management policies\n\n"
        
        # Monitoring recommendations
        result += "### 📊 Monitoring & Governance\n"
        result += "- Set up automated alerts for cost anomalies\n"
        result += "- Monitor query performance trends weekly\n"
        result += "- Implement resource monitoring dashboards\n"
        result += "- Regular optimization reviews with high-usage teams\n\n"
        
        result += "---\n\n"
        result += "*This report was generated by the MCP Snowflake Optimization Server*\n"
        result += "*For detailed analysis, use individual optimization tools*\n"
        
        return result
        
    except Exception as e:
        return f"Error generating optimization report: {str(e)}"