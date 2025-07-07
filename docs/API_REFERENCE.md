# API Reference

## Overview

The Snowflake Account Intelligence Server provides tools organized into functional categories. Each tool is exposed as an MCP tool that Claude can call.

## Generic Query Tools

### execute_query

Execute any SELECT query against SNOWFLAKE.ACCOUNT_USAGE schema with safety features and AI interpretation.

**Signature:**
```python
execute_query(query: str, limit: int = 1000, interpret: bool = True) -> str
```

**Parameters:**
- `query` (str): SQL SELECT statement to execute
- `limit` (int, optional): Maximum rows to return. Default: 1000
- `interpret` (bool, optional): Enable AI interpretation of results. Default: True

**Returns:** 
- Formatted query results with optional AI analysis and suggestions

**Safety Features:**
- Read-only validation (rejects INSERT, UPDATE, DELETE, etc.)
- Automatic LIMIT addition if missing
- Query timeout protection
- Result size limits

**Example Usage:**
```
Execute this query: SELECT USER_NAME, COUNT(*) FROM LOGIN_HISTORY GROUP BY USER_NAME LIMIT 10
```

---

### explore_schema

Discover available tables and columns in the SNOWFLAKE.ACCOUNT_USAGE schema.

**Signature:**
```python
explore_schema(table_pattern: str = None, show_columns: bool = False) -> str
```

**Parameters:**
- `table_pattern` (str, optional): SQL LIKE pattern to filter tables (e.g., '%HISTORY%')
- `show_columns` (bool, optional): Include detailed column information. Default: False

**Returns:** 
- Formatted list of tables with descriptions and optionally column details

**Example Usage:**
```
Show me all tables related to user activity with their columns
```

---

### help_build_query

Generate SQL query suggestions from natural language descriptions.

**Signature:**
```python
help_build_query(description: str) -> str
```

**Parameters:**
- `description` (str): Natural language description of what you want to query

**Returns:** 
- Suggested SQL queries with explanations and usage tips

**Example Usage:**
```
Help me build a query to find users who haven't logged in for 30 days
```

## Security Tools

### check_user_authentication

Analyze authentication methods (RSA vs password) for specified users.

**Signature:**
```python
check_user_authentication(users: List[str] = None, days_back: int = 30) -> str
```

**Parameters:**
- `users` (List[str], optional): List of usernames to analyze. If None, analyzes all users
- `days_back` (int, optional): Number of days to look back. Default: 30

**Returns:** 
- Comprehensive authentication analysis with security recommendations

**Analysis Includes:**
- Last login by authentication method
- Login frequency by method
- Authentication status classification
- Security recommendations

**Example Usage:**
```
Check authentication methods for: ['ALICE', 'BOB', 'CHARLIE']
```

---

### audit_privileges

Track privilege and role changes in the account.

**Signature:**
```python
audit_privileges(days_back: int = 7, role_filter: str = None) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7
- `role_filter` (str, optional): Focus on specific role (e.g., 'ACCOUNTADMIN')

**Returns:** 
- Privilege change audit with security analysis

**Detects:**
- Role grants and revokes
- Privilege escalations
- Sensitive role assignments (ACCOUNTADMIN, SECURITYADMIN)
- Unusual granting patterns

**Example Usage:**
```
Audit privilege changes for ACCOUNTADMIN role in the last week
```

---

### detect_anomalies

Identify unusual data access patterns using behavioral analysis.

**Signature:**
```python
detect_anomalies(days_back: int = 7, sensitivity: str = "medium") -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to analyze. Default: 7
- `sensitivity` (str, optional): Detection sensitivity level. Options: 'low', 'medium', 'high'. Default: 'medium'

**Returns:** 
- Anomaly detection report with risk assessment

**Anomaly Types Detected:**
- Unusual hours access (outside business hours)
- High query volume (statistical outliers)
- Excessive data access (above normal patterns)
- Multiple database access (broader than usual scope)
- Access to newly created objects

**Example Usage:**
```
Detect unusual access patterns with high sensitivity
```

## Performance Tools

### find_slow_queries

Find and analyze the slowest queries in your Snowflake account.

**Signature:**
```python
find_slow_queries(hours_back: int = 24, limit: int = 50) -> str
```

**Parameters:**
- `hours_back` (int, optional): Number of hours to look back. Default: 24
- `limit` (int, optional): Maximum number of queries to return. Default: 50

**Returns:** 
- Analysis of slow queries with optimization recommendations

---

### analyze_repeated_queries

Identify frequently repeated expensive query patterns.

**Signature:**
```python
analyze_repeated_queries(hours_back: int = 168, limit: int = 50) -> str
```

**Parameters:**
- `hours_back` (int, optional): Number of hours to look back. Default: 168 (1 week)
- `limit` (int, optional): Maximum number of patterns to return. Default: 50

**Returns:** 
- Analysis of repeated patterns with potential savings calculations

---

### query_execution_distribution

Analyze the distribution of query execution times.

**Signature:**
```python
query_execution_distribution(days_back: int = 7) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7

**Returns:** 
- Statistical analysis of query performance distribution

---

### query_acceleration_candidates

Find queries eligible for Snowflake's Query Acceleration Service.

**Signature:**
```python
query_acceleration_candidates(days_back: int = 7, limit: int = 50) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7
- `limit` (int, optional): Maximum number of queries to return. Default: 50

**Returns:** 
- List of acceleration candidates with potential benefits

## Cost Analysis Tools

### warehouse_cost_analysis

Analyze credit consumption and costs by warehouse.

**Signature:**
```python
warehouse_cost_analysis(days_back: int = 7) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7

**Returns:** 
- Detailed cost breakdown by warehouse with optimization suggestions

---

### cost_per_query_analysis

Calculate and analyze cost efficiency per query by warehouse.

**Signature:**
```python
cost_per_query_analysis(days_back: int = 30) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 30

**Returns:** 
- Cost efficiency analysis across warehouses

---

### find_most_expensive_queries

Identify queries consuming the most credits.

**Signature:**
```python
find_most_expensive_queries(days_back: int = 7, limit: int = 25) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7
- `limit` (int, optional): Maximum number of queries to return. Default: 25

**Returns:** 
- List of most expensive queries with cost details

---

### user_cost_analysis

Analyze resource consumption and costs by user.

**Signature:**
```python
user_cost_analysis(days_back: int = 7) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7

**Returns:** 
- User activity analysis with associated costs

## Monitoring Tools

### warehouse_utilization_analysis

Analyze warehouse utilization patterns and provide sizing recommendations.

**Signature:**
```python
warehouse_utilization_analysis(days_back: int = 7) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7

**Returns:** 
- Warehouse efficiency analysis with sizing recommendations

---

### optimization_report

Generate a comprehensive optimization report.

**Signature:**
```python
optimization_report(days_back: int = 7) -> str
```

**Parameters:**
- `days_back` (int, optional): Number of days to look back. Default: 7

**Returns:** 
- Executive summary combining cost, performance, and utilization analysis

## Prompts

The server provides pre-built prompts for common workflows:

### optimize_snowflake_costs
Complete cost optimization workflow that runs multiple cost analysis tools and provides recommendations.

### find_performance_bottlenecks
Performance analysis workflow that identifies slow queries, patterns, and acceleration opportunities.

### weekly_optimization_review
Generate a comprehensive weekly optimization report.

### security_audit (NEW)
Comprehensive security analysis including authentication, privileges, and anomalies.

### custom_analysis (NEW)
Interactive query building assistance with schema exploration.

## Error Handling

All tools implement consistent error handling:

- **Connection Errors**: Clear messages about authentication or network issues
- **Permission Errors**: Guidance on required roles and privileges
- **Query Errors**: SQL syntax help and debugging tips
- **Timeout Errors**: Suggestions for optimization and configuration
- **Data Availability**: Information about ACCOUNT_USAGE latency

## Data Sources

The tools query various SNOWFLAKE.ACCOUNT_USAGE views:

| View | Purpose | Latency |
|------|---------|---------|
| QUERY_HISTORY | Query performance analysis | 45 minutes |
| LOGIN_HISTORY | Authentication analysis | 2 hours |
| ACCESS_HISTORY | Data access patterns | 3 hours |
| WAREHOUSE_METERING_HISTORY | Cost analysis | 3 hours |
| GRANTS_TO_USERS | Privilege auditing | 2 hours |
| GRANTS_TO_ROLES | Role analysis | 2 hours |
| TABLES | Schema exploration | 2 hours |
| COLUMNS | Column information | 2 hours |

## Configuration

Tools respect these environment variables:

- `SNOWFLAKE_CREDIT_PRICE`: Used in cost calculations
- `QUERY_TIMEOUT_SECONDS`: Maximum query execution time
- `DEBUG_MODE`: Enable detailed logging
- `CACHE_RESULTS`: Enable result caching