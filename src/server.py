#!/usr/bin/env python3
"""
MCP Snowflake Optimization Server

This server provides tools for analyzing and optimizing Snowflake usage,
including performance monitoring, cost analysis, and optimization recommendations.
"""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastmcp import FastMCP
from tools.performance import (
    analyze_slow_queries,
    analyze_query_patterns,
    analyze_execution_time_distribution
)
from tools.costs import (
    analyze_warehouse_costs,
    analyze_cost_per_query,
    find_expensive_queries,
    analyze_user_costs
)
from tools.monitoring import (
    analyze_warehouse_utilization,
    find_query_acceleration_opportunities,
    generate_optimization_report
)

# Initialize FastMCP server
mcp = FastMCP("Snowflake Optimizer")

# Performance Analysis Tools
@mcp.tool()
def find_slow_queries(hours_back: int = 24, limit: int = 50) -> str:
    """
    Find and analyze the slowest queries in your Snowflake account.
    
    Args:
        hours_back: Number of hours to look back (default: 24)
        limit: Maximum number of queries to return (default: 50)
    
    Returns:
        Formatted analysis of slow queries with optimization recommendations
    """
    return analyze_slow_queries(hours_back, limit)

@mcp.tool()
def analyze_repeated_queries(hours_back: int = 168, limit: int = 50) -> str:
    """
    Identify frequently repeated expensive query patterns that could benefit from optimization.
    
    Args:
        hours_back: Number of hours to look back (default: 168 = 1 week)
        limit: Maximum number of patterns to return (default: 50)
    
    Returns:
        Analysis of repeated query patterns with potential time and cost savings
    """
    return analyze_query_patterns(hours_back, limit)

@mcp.tool()
def query_execution_distribution(days_back: int = 7) -> str:
    """
    Analyze the distribution of query execution times to understand performance patterns.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Analysis showing percentage of queries in different execution time buckets
    """
    return analyze_execution_time_distribution(days_back)

# Cost Analysis Tools
@mcp.tool()
def warehouse_cost_analysis(days_back: int = 7) -> str:
    """
    Analyze credit consumption and costs by warehouse with optimization recommendations.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Detailed cost breakdown by warehouse with optimization suggestions
    """
    return analyze_warehouse_costs(days_back)

@mcp.tool()
def cost_per_query_analysis(days_back: int = 30) -> str:
    """
    Calculate and analyze cost efficiency per query by warehouse.
    
    Args:
        days_back: Number of days to look back (default: 30)
    
    Returns:
        Analysis of cost per query with efficiency comparisons across warehouses
    """
    return analyze_cost_per_query(days_back)

@mcp.tool()
def find_most_expensive_queries(days_back: int = 7, limit: int = 25) -> str:
    """
    Identify the queries consuming the most credits and costing the most money.
    
    Args:
        days_back: Number of days to look back (default: 7)
        limit: Maximum number of queries to return (default: 25)
    
    Returns:
        List of most expensive queries with cost details and optimization recommendations
    """
    return find_expensive_queries(days_back, limit)

@mcp.tool()
def user_cost_analysis(days_back: int = 7) -> str:
    """
    Analyze resource consumption and costs by user to identify optimization opportunities.
    
    Args:
        days_back: Number of days to look back (default: 7)
        
    Returns:
        Analysis of user activity patterns and associated costs
    """
    return analyze_user_costs(days_back)

# Monitoring and Optimization Tools
@mcp.tool()
def warehouse_utilization_analysis(days_back: int = 7) -> str:
    """
    Analyze warehouse utilization patterns and provide sizing recommendations.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Analysis of warehouse efficiency with sizing and configuration recommendations
    """
    return analyze_warehouse_utilization(days_back)

@mcp.tool()
def query_acceleration_candidates(days_back: int = 7, limit: int = 50) -> str:
    """
    Find queries that would benefit from Snowflake's Query Acceleration Service.
    
    Args:
        days_back: Number of days to look back (default: 7)
        limit: Maximum number of queries to return (default: 50)
    
    Returns:
        List of queries eligible for acceleration with potential performance benefits
    """
    return find_query_acceleration_opportunities(days_back, limit)

@mcp.tool()
def optimization_report(days_back: int = 7) -> str:
    """
    Generate a comprehensive optimization report with key metrics and recommendations.
    
    Args:
        days_back: Number of days to look back (default: 7)
    
    Returns:
        Executive summary report combining cost, performance, and utilization analysis
    """
    return generate_optimization_report(days_back)

# Add some helpful prompts
@mcp.prompt()
def optimize_snowflake_costs() -> str:
    """
    Help me optimize my Snowflake costs by analyzing warehouse usage and identifying expensive queries.
    """
    return """I'll help you optimize your Snowflake costs. Let me analyze your usage patterns:

1. First, let's look at your warehouse costs:
   {warehouse_cost_analysis}

2. Then identify your most expensive queries:
   {find_most_expensive_queries}

3. Finally, check warehouse utilization:
   {warehouse_utilization_analysis}

Based on these analyses, I'll provide specific recommendations to reduce your Snowflake costs."""

@mcp.prompt()
def find_performance_bottlenecks() -> str:
    """
    Help me identify and resolve Snowflake performance bottlenecks.
    """
    return """I'll help you identify performance bottlenecks in your Snowflake environment:

1. Let's find your slowest queries:
   {find_slow_queries}

2. Look for repeated expensive patterns:
   {analyze_repeated_queries}

3. Check overall query performance distribution:
   {query_execution_distribution}

4. Find acceleration opportunities:
   {query_acceleration_candidates}

This analysis will help identify the biggest performance improvement opportunities."""

@mcp.prompt()
def weekly_optimization_review() -> str:
    """
    Generate my weekly Snowflake optimization review with key metrics and recommendations.
    """
    return """Here's your weekly Snowflake optimization review:

{optimization_report}

This report provides an executive summary of your Snowflake usage with actionable recommendations for cost and performance optimization."""

if __name__ == "__main__":
    # Run the MCP server
    mcp.run()