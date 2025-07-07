"""
Generic query tools for flexible Snowflake ACCOUNT_USAGE analysis
"""

import pandas as pd
import re
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_connection import snowflake_conn
from typing import List, Dict, Any

def is_read_only_query(query: str) -> bool:
    """Validate that a query is read-only for safety"""
    # Remove comments and normalize
    query_upper = re.sub(r'--.*', '', query).upper().strip()
    
    # Check for dangerous keywords
    dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'MERGE']
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return False
    
    # Ensure it starts with SELECT or WITH
    if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
        return False
    
    return True

def add_limit_if_missing(query: str, limit: int) -> str:
    """Add LIMIT clause if not present"""
    query_upper = query.upper()
    if 'LIMIT' not in query_upper:
        # Remove trailing semicolon if present
        query = query.rstrip(';').rstrip()
        query += f" LIMIT {limit}"
    return query

def format_dataframe(df: pd.DataFrame, max_rows: int = 20) -> str:
    """Format DataFrame for CLI display"""
    if df.empty:
        return "No data returned."
    
    result = ""
    
    # Show shape
    result += f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n\n"
    
    # Convert to string for display
    if len(df) > max_rows:
        result += f"Showing first {max_rows} of {len(df)} rows:\n\n"
        display_df = df.head(max_rows)
    else:
        display_df = df
    
    # Format as table
    result += display_df.to_string(index=False, max_cols=10)
    
    return result

def detect_query_type(query: str) -> str:
    """Detect the type of query for better interpretation"""
    query_upper = query.upper()
    
    if 'LOGIN_HISTORY' in query_upper:
        return 'authentication'
    elif 'QUERY_HISTORY' in query_upper:
        return 'performance'
    elif 'WAREHOUSE_METERING_HISTORY' in query_upper:
        return 'cost'
    elif 'ACCESS_HISTORY' in query_upper:
        return 'security'
    elif 'GRANTS' in query_upper or 'ROLES' in query_upper:
        return 'permissions'
    else:
        return 'general'

def execute_account_usage_query(query: str, limit: int = 1000, interpret: bool = True) -> str:
    """
    Execute any query against SNOWFLAKE.ACCOUNT_USAGE schema.
    
    Args:
        query: SQL query to execute
        limit: Maximum rows to return (default: 1000)
        interpret: Whether to provide AI interpretation of results (default: True)
    
    Returns:
        Formatted query results with optional interpretation
    """
    try:
        # Validate query is read-only
        if not is_read_only_query(query):
            return "❌ Error: Only SELECT queries are allowed for safety. Detected non-read operation."
        
        # Add limit if missing
        query = add_limit_if_missing(query, limit)
        
        # Get timeout from env
        timeout = int(os.getenv('QUERY_TIMEOUT_SECONDS', '300'))
        
        # Execute query
        df = snowflake_conn.execute_query(query)
        
        result = "## Query Results\n\n"
        
        if df.empty:
            result += "No data returned. This could mean:\n"
            result += "- The filter criteria returned no matches\n"
            result += "- The table might be empty or have data latency\n"
            result += "- Check ACCOUNT_USAGE data latency (up to 3 hours for some tables)\n"
            return result
        
        # Format results
        result += format_dataframe(df, max_rows=50)
        
        if interpret:
            query_type = detect_query_type(query)
            result += f"\n\n## Analysis (Query Type: {query_type})\n\n"
            
            # Provide basic interpretation based on query type
            if query_type == 'authentication':
                result += "This query analyzes authentication and login patterns.\n"
                if 'FIRST_AUTHENTICATION_FACTOR' in df.columns:
                    auth_methods = df['FIRST_AUTHENTICATION_FACTOR'].value_counts()
                    result += f"Authentication methods found: {', '.join(auth_methods.index.tolist())}\n"
            
            elif query_type == 'performance':
                result += "This query analyzes query performance metrics.\n"
                if 'EXECUTION_TIME' in df.columns:
                    avg_time = df['EXECUTION_TIME'].mean() / 1000
                    result += f"Average execution time: {avg_time:.2f} seconds\n"
            
            elif query_type == 'cost':
                result += "This query analyzes compute costs and credit usage.\n"
                if 'CREDITS_USED' in df.columns:
                    total_credits = df['CREDITS_USED'].sum()
                    result += f"Total credits analyzed: {total_credits:.2f}\n"
        
        return result
        
    except Exception as e:
        error_msg = f"❌ Query Error: {str(e)}\n\n"
        error_msg += "Tips:\n"
        error_msg += "- Check table and column names (case-sensitive)\n"
        error_msg += "- Ensure you're querying SNOWFLAKE.ACCOUNT_USAGE schema\n"
        error_msg += "- Use explore_account_usage_schema() to see available tables\n"
        error_msg += "- Some tables have data latency (up to 3 hours)\n"
        return error_msg

def explore_account_usage_schema(table_pattern: str = None, show_columns: bool = False) -> str:
    """
    Explore available tables and columns in ACCOUNT_USAGE schema.
    
    Args:
        table_pattern: Optional pattern to filter tables (e.g., '%HISTORY%')
        show_columns: Whether to show column details for matching tables
    
    Returns:
        List of available tables and optionally their columns
    """
    try:
        # Query to get tables
        table_query = """
        SELECT 
            TABLE_NAME,
            ROW_COUNT,
            COMMENT
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
        WHERE TABLE_SCHEMA = 'ACCOUNT_USAGE'
            AND TABLE_CATALOG = 'SNOWFLAKE'
        """
        
        if table_pattern:
            table_query += f" AND TABLE_NAME LIKE '{table_pattern.upper()}'"
        
        table_query += " ORDER BY TABLE_NAME"
        
        df = snowflake_conn.execute_query(table_query)
        
        result = "## ACCOUNT_USAGE Schema Tables\n\n"
        
        if df.empty:
            result += "No tables found matching your criteria.\n"
            return result
        
        result += f"Found {len(df)} tables:\n\n"
        
        for _, row in df.iterrows():
            table_name = row['TABLE_NAME']
            row_count = row['ROW_COUNT'] if pd.notna(row['ROW_COUNT']) else 'Unknown'
            comment = row['COMMENT'] if pd.notna(row['COMMENT']) else 'No description'
            
            result += f"### {table_name}\n"
            result += f"- Rows: {row_count:,}\n" if isinstance(row_count, (int, float)) else f"- Rows: {row_count}\n"
            result += f"- Description: {comment}\n"
            
            if show_columns:
                # Get columns for this table
                col_query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COMMENT
                FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
                WHERE TABLE_SCHEMA = 'ACCOUNT_USAGE'
                    AND TABLE_CATALOG = 'SNOWFLAKE'
                    AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
                """
                
                col_df = snowflake_conn.execute_query(col_query)
                
                if not col_df.empty:
                    result += "- Columns:\n"
                    for _, col in col_df.iterrows():
                        nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"
                        result += f"  - {col['COLUMN_NAME']} ({col['DATA_TYPE']}) {nullable}\n"
            
            result += "\n"
        
        result += "\n## Common Table Categories:\n"
        result += "- **History Tables**: Query, Login, Access, Warehouse histories\n"
        result += "- **Metering Tables**: Credit usage and cost tracking\n"
        result += "- **Security Tables**: Grants, Roles, Users, Authentication\n"
        result += "- **Storage Tables**: Tables, Databases, Stages storage info\n"
        
        return result
        
    except Exception as e:
        return f"Error exploring schema: {str(e)}"

def build_query_from_description(description: str, include_explanation: bool = True) -> str:
    """
    Help build a query from natural language description.
    
    Args:
        description: Natural language description of what you want to query
        include_explanation: Whether to include explanation of the query
    
    Returns:
        SQL query template and explanation
    """
    result = "## Query Builder Assistant\n\n"
    result += f"Request: {description}\n\n"
    
    # Provide common query templates based on keywords
    description_lower = description.lower()
    
    suggested_queries = []
    
    # Authentication/Login patterns
    if any(word in description_lower for word in ['login', 'authentication', 'rsa', 'password']):
        suggested_queries.append({
            'title': 'User Authentication Analysis',
            'query': """SELECT 
    USER_NAME,
    FIRST_AUTHENTICATION_FACTOR,
    MAX(EVENT_TIMESTAMP) as LAST_LOGIN,
    COUNT(*) as LOGIN_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND IS_SUCCESS = 'YES'
GROUP BY USER_NAME, FIRST_AUTHENTICATION_FACTOR
ORDER BY USER_NAME, LAST_LOGIN DESC""",
            'explanation': 'Shows authentication methods used by each user with last login time'
        })
    
    # Cost/Credit patterns
    if any(word in description_lower for word in ['cost', 'credit', 'expensive', 'warehouse']):
        suggested_queries.append({
            'title': 'Warehouse Credit Usage',
            'query': """SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) as TOTAL_CREDITS,
    AVG(CREDITS_USED) as AVG_CREDITS_PER_HOUR,
    COUNT(*) as ACTIVE_HOURS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
ORDER BY TOTAL_CREDITS DESC""",
            'explanation': 'Shows credit consumption by warehouse over the last 7 days'
        })
    
    # Query performance patterns
    if any(word in description_lower for word in ['slow', 'query', 'performance', 'execution']):
        suggested_queries.append({
            'title': 'Slow Query Analysis',
            'query': """SELECT 
    QUERY_ID,
    QUERY_TEXT,
    USER_NAME,
    WAREHOUSE_NAME,
    EXECUTION_TIME/1000 as EXECUTION_SECONDS,
    BYTES_SCANNED,
    ROWS_PRODUCED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    AND EXECUTION_STATUS = 'SUCCESS'
    AND EXECUTION_TIME > 60000  -- Over 1 minute
ORDER BY EXECUTION_TIME DESC
LIMIT 50""",
            'explanation': 'Finds queries that took over 1 minute to execute in the last 24 hours'
        })
    
    # Access/Security patterns
    if any(word in description_lower for word in ['access', 'security', 'permission', 'grant', 'role']):
        suggested_queries.append({
            'title': 'User Role Grants',
            'query': """SELECT 
    GRANTEE_NAME as USER_NAME,
    ROLE,
    GRANTED_ON,
    GRANTED_BY,
    CREATED_ON
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE DELETED_ON IS NULL
ORDER BY USER_NAME, ROLE""",
            'explanation': 'Shows current role assignments for all users'
        })
    
    if suggested_queries:
        result += "### Suggested Queries:\n\n"
        for i, sq in enumerate(suggested_queries, 1):
            result += f"**{i}. {sq['title']}**\n\n"
            result += "```sql\n"
            result += sq['query']
            result += "\n```\n\n"
            if include_explanation:
                result += f"*{sq['explanation']}*\n\n"
    else:
        result += "### Generic Query Template:\n\n"
        result += "```sql\n"
        result += "SELECT \n"
        result += "    column1,\n"
        result += "    column2,\n"
        result += "    COUNT(*) as count,\n"
        result += "    SUM(metric) as total\n"
        result += "FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_NAME\n"
        result += "WHERE condition = 'value'\n"
        result += "    AND timestamp_column >= DATEADD(day, -7, CURRENT_TIMESTAMP())\n"
        result += "GROUP BY column1, column2\n"
        result += "ORDER BY total DESC\n"
        result += "LIMIT 100\n"
        result += "```\n\n"
    
    result += "### Tips:\n"
    result += "- Always include date filters to limit data volume\n"
    result += "- Use LIMIT to control result size\n"
    result += "- Remember ACCOUNT_USAGE data has latency (up to 3 hours)\n"
    result += "- Use explore_account_usage_schema() to find exact table and column names\n"
    
    return result