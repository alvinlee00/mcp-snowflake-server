#!/usr/bin/env python3
"""
MCP Snowflake Account Intelligence Server

This server provides comprehensive tools for Snowflake account analysis including:
- Generic query execution against ACCOUNT_USAGE schema
- Security and access monitoring
- Performance analysis and optimization
- Cost tracking and recommendations
"""

import asyncio
import sys
import os
from typing import List

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastmcp import FastMCP

# Import generic tools
from tools.generic import (
    execute_account_usage_query,
    explore_account_usage_schema,
    build_query_from_description
)

# Import security tools
from tools.security import (
    analyze_user_authentication,
    audit_privilege_changes,
    detect_unusual_access_patterns
)

# Import existing tools
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
mcp = FastMCP("Snowflake Account Intelligence")

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

# Generic Query Tools
@mcp.tool()
def execute_query(query: str, limit: int = 1000, interpret: bool = True) -> str:
    """
    Execute any SELECT query against SNOWFLAKE.ACCOUNT_USAGE schema.
    
    The AI will help interpret results and suggest optimizations.
    Safety features: automatic LIMIT, timeout, read-only validation.
    
    Args:
        query: SQL SELECT query to execute
        limit: Maximum rows to return (default: 1000)
        interpret: Whether to provide AI interpretation (default: True)
    
    Returns:
        Query results with interpretation and suggestions
    """
    return execute_account_usage_query(query, limit, interpret)

@mcp.tool()
def explore_schema(table_pattern: str = None, show_columns: bool = False) -> str:
    """
    Explore available tables and columns in ACCOUNT_USAGE schema.
    
    Helps discover what data is available for analysis.
    
    Args:
        table_pattern: Optional pattern to filter tables (e.g., '%HISTORY%')
        show_columns: Whether to show column details (default: False)
    
    Returns:
        List of tables with descriptions and optionally columns
    """
    return explore_account_usage_schema(table_pattern, show_columns)

@mcp.tool()
def help_build_query(description: str) -> str:
    """
    Get help building a query from natural language description.
    
    Provides query templates and suggestions based on your request.
    
    Args:
        description: Natural language description of what you want to query
    
    Returns:
        Suggested queries with explanations
    """
    return build_query_from_description(description)

# Security and Access Tools
@mcp.tool()
def check_user_authentication(users: List[str] = None, days_back: int = 30) -> str:
    """
    Analyze authentication methods (RSA vs password) for specified users.
    
    Perfect for security audits and identifying users who haven't migrated to RSA.
    
    Args:
        users: List of usernames to check (None for all users)
        days_back: Number of days to look back (default: 30)
    
    Returns:
        Authentication analysis with security recommendations
    """
    return analyze_user_authentication(users, days_back)

@mcp.tool()
def audit_privileges(days_back: int = 7, role_filter: str = None) -> str:
    """
    Track privilege and role changes in your account.
    
    Identifies privilege escalations and security concerns.
    
    Args:
        days_back: Number of days to look back (default: 7)
        role_filter: Optional role to filter (e.g., 'ACCOUNTADMIN')
    
    Returns:
        Privilege change audit with security analysis
    """
    return audit_privilege_changes(days_back, role_filter)

@mcp.tool()
def detect_anomalies(days_back: int = 7, sensitivity: str = "medium") -> str:
    """
    Detect unusual data access patterns that might indicate security issues.
    
    Uses behavioral analysis to identify anomalies.
    
    Args:
        days_back: Number of days to look back (default: 7)
        sensitivity: Detection level - 'low', 'medium', 'high' (default: 'medium')
    
    Returns:
        Anomaly detection report with risk assessment
    """
    return detect_unusual_access_patterns(days_back, sensitivity)

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

@mcp.prompt()
def security_audit() -> str:
    """
    Perform a comprehensive security audit of your Snowflake account.
    """
    return """I'll perform a security audit of your Snowflake account:

1. Check user authentication methods:
   {check_user_authentication}

2. Audit recent privilege changes:
   {audit_privileges}

3. Detect unusual access patterns:
   {detect_anomalies}

Based on these findings, I'll provide security recommendations and identify potential risks."""

@mcp.prompt()
def custom_analysis() -> str:
    """
    Help me write and execute custom queries against ACCOUNT_USAGE schema.
    """
    return """I can help you analyze any aspect of your Snowflake account. 

First, let me show you what data is available:
{explore_schema}

Then I can help you build and execute custom queries based on your specific needs. 
Just describe what you want to analyze and I'll help create the appropriate query."""

if __name__ == "__main__":
    # Run the MCP server
    mcp.run()