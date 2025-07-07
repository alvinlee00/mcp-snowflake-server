# Snowflake Account Intelligence Server - Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Installation & Setup](#installation--setup)
4. [Core Concepts](#core-concepts)
5. [Tool Categories](#tool-categories)
6. [Usage Examples](#usage-examples)
7. [Security Considerations](#security-considerations)
8. [Troubleshooting](#troubleshooting)
9. [Development Guide](#development-guide)
10. [API Reference](#api-reference)

## Overview

The Snowflake Account Intelligence Server is a Model Context Protocol (MCP) server that transforms Claude into your intelligent Snowflake analyst. It provides:

- **Flexible Query Execution**: Run any SELECT query against ACCOUNT_USAGE with AI interpretation
- **Security Monitoring**: Track authentication methods, privilege changes, and access patterns
- **Cost Optimization**: Analyze credit usage and identify savings opportunities
- **Performance Analysis**: Find slow queries and optimization opportunities
- **Natural Language Interface**: Describe what you want to analyze in plain English

### Key Benefits

1. **AI-Powered Analysis**: Claude interprets results and suggests next steps
2. **Safety First**: Read-only queries with automatic limits and validation
3. **Comprehensive Coverage**: From generic queries to specialized security audits
4. **Extensible Design**: Easy to add new analysis tools as needed

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Claude Desktop                         │
│                  (MCP Client)                           │
└───────────────────┬─────────────────────────────────────┘
                    │ MCP Protocol
┌───────────────────┴─────────────────────────────────────┐
│          Snowflake Account Intelligence Server           │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │                  server.py                        │   │
│  │  - FastMCP initialization                        │   │
│  │  - Tool registration                             │   │
│  │  - Prompt definitions                            │   │
│  └──────────┬──────────────────────┬───────────────┘   │
│             │                      │                     │
│  ┌──────────▼──────────┐ ┌────────▼────────────────┐   │
│  │   Generic Tools     │ │   Specialized Tools     │   │
│  │                     │ │                         │   │
│  │ - execute_query     │ │ - security.py          │   │
│  │ - explore_schema    │ │ - performance.py       │   │
│  │ - help_build_query  │ │ - costs.py             │   │
│  │                     │ │ - monitoring.py        │   │
│  └─────────────────────┘ └─────────────────────────┘   │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Utilities & Helpers                  │   │
│  │                                                   │   │
│  │ - snowflake_connection.py (connection mgmt)      │   │
│  │ - query_interpreter.py (AI result analysis)      │   │
│  │ - optimization_queries.py (SQL templates)        │   │
│  └──────────────────────────────────────────────────┘   │
└───────────────────┬─────────────────────────────────────┘
                    │ Snowflake Connector
┌───────────────────▼─────────────────────────────────────┐
│              Snowflake ACCOUNT_USAGE Schema             │
│                                                          │
│  Tables: QUERY_HISTORY, LOGIN_HISTORY, ACCESS_HISTORY,  │
│          WAREHOUSE_METERING_HISTORY, GRANTS_TO_USERS,   │
│          TABLES, COLUMNS, and 100+ more...              │
└──────────────────────────────────────────────────────────┘
```

## Installation & Setup

### Prerequisites

- Python 3.10 or higher
- Snowflake account with ACCOUNTADMIN role
- Claude Desktop application
- RSA key pair (recommended) or password authentication

### Step 1: Clone and Install

```bash
# Clone the repository
git clone https://github.com/yourusername/mcp-snowflake-server.git
cd mcp-snowflake-server

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Authentication

#### Option A: Password Authentication (Quick Start)

1. Copy the example configuration:
```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:
```env
SNOWFLAKE_ACCOUNT=myaccount.snowflakecomputing.com
SNOWFLAKE_USER=myusername
SNOWFLAKE_PASSWORD=mypassword
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

#### Option B: RSA Key Authentication (Recommended for Production)

1. Generate RSA key pair:
```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt

# Generate public key
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub

# Display public key for Snowflake
cat snowflake_key.pub
```

2. Add public key to Snowflake user:
```sql
ALTER USER myusername SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```

3. Update `.env`:
```env
SNOWFLAKE_ACCOUNT=myaccount.snowflakecomputing.com
SNOWFLAKE_USER=myusername
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/snowflake_key.p8
# SNOWFLAKE_PASSWORD=  # Comment out password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Step 3: Configure Claude Desktop

1. Open Claude Desktop configuration:
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

2. Add the server configuration:
```json
{
  "mcpServers": {
    "snowflake-intelligence": {
      "command": "python",
      "args": ["/absolute/path/to/mcp-snowflake-server/src/server.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "myaccount.snowflakecomputing.com",
        "SNOWFLAKE_USER": "myusername",
        "SNOWFLAKE_PASSWORD": "mypassword",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_ROLE": "ACCOUNTADMIN",
        "SNOWFLAKE_CREDIT_PRICE": "4.00",
        "QUERY_TIMEOUT_SECONDS": "300"
      }
    }
  }
}
```

3. Restart Claude Desktop

### Step 4: Verify Installation

In Claude Desktop, try:
```
Show me what tables are available in ACCOUNT_USAGE
```

## Core Concepts

### 1. Generic vs Specialized Tools

The server provides two types of tools:

**Generic Tools**: Flexible, query-anything approach
- Best for: Ad-hoc analysis, custom queries, exploration
- Example: `execute_query("SELECT * FROM LOGIN_HISTORY LIMIT 10")`

**Specialized Tools**: Purpose-built for specific tasks
- Best for: Common operations, guided analysis, best practices
- Example: `check_user_authentication(['USER1', 'USER2'])`

### 2. Safety Features

All queries are protected by multiple safety layers:

1. **Read-Only Validation**: Only SELECT queries allowed
2. **Automatic LIMIT**: Adds LIMIT clause if missing
3. **Query Timeout**: Configurable timeout (default 5 minutes)
4. **Result Size Control**: Prevents overwhelming output

### 3. AI Integration

The server leverages Claude's capabilities for:

- **Query Construction**: Natural language to SQL
- **Result Interpretation**: Understanding what the data means
- **Recommendation Engine**: Suggesting next steps
- **Pattern Recognition**: Identifying anomalies and trends

### 4. Data Latency

ACCOUNT_USAGE views have inherent latency:

| View | Latency |
|------|---------|
| QUERY_HISTORY | 45 minutes |
| LOGIN_HISTORY | 2 hours |
| WAREHOUSE_METERING_HISTORY | 3 hours |
| ACCESS_HISTORY | 3 hours |
| Most others | 2-3 hours |

## Tool Categories

### 🔍 Generic Query Tools

#### execute_query
Execute any SELECT query with AI interpretation.

```python
execute_query(
    query: str,              # SQL SELECT statement
    limit: int = 1000,       # Max rows to return
    interpret: bool = True   # Enable AI analysis
) -> str
```

**Example:**
```
Execute this query: 
SELECT USER_NAME, COUNT(*) as login_count 
FROM LOGIN_HISTORY 
WHERE IS_SUCCESS = 'NO' 
GROUP BY USER_NAME 
HAVING COUNT(*) > 5
```

#### explore_schema
Discover available tables and columns.

```python
explore_schema(
    table_pattern: str = None,  # Filter pattern (e.g., '%HISTORY%')
    show_columns: bool = False  # Include column details
) -> str
```

**Example:**
```
Show me all tables related to user access with their columns
```

#### help_build_query
Get help constructing queries from descriptions.

```python
help_build_query(
    description: str  # Natural language description
) -> str
```

**Example:**
```
Help me build a query to find queries that scanned over 1TB of data
```

### 🔒 Security Tools

#### check_user_authentication
Analyze authentication methods across users.

```python
check_user_authentication(
    users: List[str] = None,  # Specific users or None for all
    days_back: int = 30      # Lookback period
) -> str
```

**Key Features:**
- Identifies password-only users
- Tracks RSA adoption progress
- Shows last login by method
- Security recommendations

#### audit_privileges
Monitor role and permission changes.

```python
audit_privileges(
    days_back: int = 7,        # Lookback period
    role_filter: str = None    # Focus on specific role
) -> str
```

**Detects:**
- ACCOUNTADMIN grants
- Rapid privilege accumulation
- Unusual grant patterns

#### detect_anomalies
Identify unusual access patterns using behavioral analysis.

```python
detect_anomalies(
    days_back: int = 7,           # Analysis window
    sensitivity: str = "medium"   # low/medium/high
) -> str
```

**Anomaly Types:**
- Unusual hours access
- High query volume
- Excessive data access
- Multiple database access

### 💰 Cost Analysis Tools

- `warehouse_cost_analysis`: Credit usage by warehouse
- `cost_per_query_analysis`: Efficiency metrics
- `find_most_expensive_queries`: Top credit consumers
- `user_cost_analysis`: Costs by user

### 🚀 Performance Tools

- `find_slow_queries`: Identify performance bottlenecks
- `analyze_repeated_queries`: Find optimization opportunities
- `query_execution_distribution`: Performance patterns
- `query_acceleration_candidates`: Acceleration opportunities

### 📊 Monitoring Tools

- `warehouse_utilization_analysis`: Sizing recommendations
- `optimization_report`: Executive summary

## Usage Examples

### Example 1: Security Audit for Specific Users

**User Request:**
```
Check if our data team users are using RSA: ['ALICE_ANALYST', 'BOB_ENGINEER', 'CHARLIE_SCIENTIST']
```

**Claude's Response:**
```
## User Authentication Analysis (Last 30 Days)

**Analyzing 3 specified users**

### Summary:
- Total Users: 3
- Password Only: 1 (33.3%)
- RSA Only: 1 (33.3%)
- Both Methods Active: 1 (33.3%)

⚠️ **Security Alert: 1 user still using password-only authentication**

### User Details:
| User | Status | Last Password | Last RSA | Password Logins | RSA Logins |
|------|--------|---------------|----------|-----------------|------------|
| ALICE_ANALYST | ⚠️ Password Only | 2024-01-15 | Never | 45 | 0 |
| BOB_ENGINEER | ⚡ Both Methods Active | 2024-01-14 | 2024-01-15 | 12 | 33 |
| CHARLIE_SCIENTIST | RSA Only | Never | 2024-01-15 | 0 | 28 |

### Recommendations:
**High Priority - Password-Only Users:**
- ALICE_ANALYST: Migrate to RSA key authentication

**Medium Priority - Both Methods Active:**
- Consider disabling password authentication for BOB_ENGINEER after confirming RSA adoption
```

### Example 2: Custom Query with Interpretation

**User Request:**
```
Execute query: SELECT DATABASE_NAME, COUNT(DISTINCT QUERY_ID) as query_count, 
SUM(BYTES_SCANNED)/1024/1024/1024 as gb_scanned 
FROM QUERY_HISTORY 
WHERE START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP()) 
GROUP BY DATABASE_NAME 
ORDER BY gb_scanned DESC
```

**Claude's Response:**
```
## Query Results

Shape: 12 rows × 3 columns

| DATABASE_NAME | QUERY_COUNT | GB_SCANNED |
|---------------|-------------|------------|
| ANALYTICS_DB  | 45,231      | 8,456.7    |
| RAW_DATA      | 12,456      | 3,234.5    |
| STAGING       | 8,901       | 1,234.8    |
...

## Analysis (Query Type: performance)

This query analyzes database usage patterns.
- Most active database: ANALYTICS_DB with 45,231 queries
- Highest data volume: ANALYTICS_DB scanned 8.5TB
- 12 databases accessed in the last 7 days

## Suggested Actions:
- Review table clustering in ANALYTICS_DB to reduce scan volume
- Consider creating materialized views for frequently accessed data
- Implement query result caching for repeated queries
```

### Example 3: Interactive Query Building

**User Request:**
```
Help me find users who logged in during unusual hours (midnight to 6am)
```

**Claude's Response:**
```
## Query Builder Assistant

Request: find users who logged in during unusual hours (midnight to 6am)

### Suggested Query:

**Unusual Hours Login Analysis**

```sql
SELECT 
    USER_NAME,
    COUNT(*) as NIGHT_LOGIN_COUNT,
    MIN(EVENT_TIMESTAMP) as FIRST_NIGHT_LOGIN,
    MAX(EVENT_TIMESTAMP) as LAST_NIGHT_LOGIN,
    LISTAGG(DISTINCT HOUR(EVENT_TIMESTAMP), ', ') as LOGIN_HOURS
FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
WHERE EVENT_TIMESTAMP >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    AND IS_SUCCESS = 'YES'
    AND HOUR(EVENT_TIMESTAMP) BETWEEN 0 AND 5
GROUP BY USER_NAME
HAVING COUNT(*) > 2  -- More than 2 night logins
ORDER BY NIGHT_LOGIN_COUNT DESC
```

*Shows users with multiple successful logins between midnight and 6am*

### Tips:
- Adjust the HAVING clause to change sensitivity
- Add DATE(EVENT_TIMESTAMP) to see patterns by date
- Join with user roles to focus on privileged accounts
```

### Example 4: Detecting Privilege Escalation

**User Request:**
```
Has anyone been granted ACCOUNTADMIN in the last week?
```

**Claude's Response:**
```
## Privilege Change Audit (Last 7 Days)

### Summary:
- Total Changes: 23
- Grants: 18
- Revokes: 5
- Unique Grantees: 12
- Unique Grantors: 3

### ⚠️ Sensitive Role Assignments:

- GRANT: **ACCOUNTADMIN** to USER 'NEW_DBA' by SECURITYADMIN on 2024-01-14 15:30
- GRANT: **SECURITYADMIN** to USER 'SARAH_ADMIN' by ACCOUNTADMIN on 2024-01-12 09:15

### Security Analysis:
- All grants made by authorized roles (ACCOUNTADMIN, SECURITYADMIN)
- NEW_DBA received ACCOUNTADMIN - verify if this was approved
- Consider implementing approval workflow for sensitive roles

### Recommendations:
- Review ACCOUNTADMIN grant to NEW_DBA
- Document business justification for privilege changes
- Set up real-time alerts for ACCOUNTADMIN grants
```

## Security Considerations

### 1. Authentication Best Practices

**RSA Key Authentication (Recommended)**
- Generate keys with strong encryption
- Store private keys securely
- Rotate keys periodically
- Never commit keys to version control

**Password Authentication**
- Use only for development/testing
- Store passwords in environment variables
- Never hardcode credentials

### 2. Role Requirements

The server requires ACCOUNTADMIN role for full functionality:
- ACCOUNT_USAGE schema access
- Complete visibility into account activity
- No data modification capabilities (read-only)

Consider creating a dedicated user:
```sql
-- Create monitoring user
CREATE USER SNOWFLAKE_MONITOR_USER;
ALTER USER SNOWFLAKE_MONITOR_USER SET DEFAULT_ROLE = 'ACCOUNTADMIN';

-- For more restricted access, create custom role
CREATE ROLE ACCOUNT_MONITOR;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE ACCOUNT_MONITOR;
GRANT ROLE ACCOUNT_MONITOR TO USER SNOWFLAKE_MONITOR_USER;
```

### 3. Query Safety

All queries are validated before execution:
- SQL injection prevention
- Read-only enforcement
- Resource limits (timeout, row limits)
- No access to sensitive schemas outside ACCOUNT_USAGE

### 4. Data Privacy

- Query results stay within your Claude Desktop session
- No data is sent to external services
- All processing happens locally
- Respect your organization's data governance policies

## Troubleshooting

### Common Issues

#### 1. Connection Errors

**Error:** "Failed to connect to Snowflake"

**Solutions:**
- Verify account identifier format: `account.region.cloud`
- Check network connectivity and firewall rules
- Ensure user has proper authentication configured
- Verify warehouse exists and is not suspended

#### 2. Permission Errors

**Error:** "Insufficient privileges to access ACCOUNT_USAGE"

**Solutions:**
- Confirm user has ACCOUNTADMIN role
- Check role is set as default or explicitly used
- Verify IMPORTED PRIVILEGES on SNOWFLAKE database

#### 3. Authentication Issues

**Error:** "Authentication failed"

**Solutions:**
- For RSA: Ensure public key is correctly added to user
- For RSA: Verify private key path and permissions
- For password: Check for special characters that need escaping
- Try authentication method in Snowflake web UI first

#### 4. Query Timeouts

**Error:** "Query execution timed out"

**Solutions:**
- Increase QUERY_TIMEOUT_SECONDS in .env
- Add more restrictive filters to reduce data volume
- Use smaller time windows for historical queries
- Check warehouse size and scaling settings

#### 5. No Data Returned

**Issue:** Queries return empty results

**Considerations:**
- ACCOUNT_USAGE has 45min-3hr latency
- Check if features are enabled (e.g., query acceleration)
- Verify data retention (365 days for most tables)
- Ensure filters aren't too restrictive

### Debug Mode

Enable debug mode for detailed logging:

```env
DEBUG_MODE=true
```

This provides:
- SQL query execution logs
- Connection diagnostics
- Performance metrics
- Error stack traces

## Development Guide

### Adding New Tools

1. **Create Tool Function** in appropriate module:

```python
# In tools/custom.py
def analyze_custom_metric(param1: str, param2: int = 7) -> str:
    """
    Analyze some custom metric.
    
    Args:
        param1: Description
        param2: Optional parameter
        
    Returns:
        Formatted analysis results
    """
    try:
        query = f"""
        SELECT ...
        FROM SNOWFLAKE.ACCOUNT_USAGE.SOME_TABLE
        WHERE ...
        """
        
        df = snowflake_conn.execute_query(query)
        
        # Process results
        result = format_analysis(df)
        
        return result
        
    except Exception as e:
        return f"Error: {str(e)}"
```

2. **Register in server.py**:

```python
from tools.custom import analyze_custom_metric

@mcp.tool()
def custom_metric_analysis(param1: str, param2: int = 7) -> str:
    """
    Tool description for Claude.
    """
    return analyze_custom_metric(param1, param2)
```

### Query Best Practices

1. **Always include date filters**:
```sql
WHERE timestamp_column >= DATEADD(day, -7, CURRENT_TIMESTAMP())
```

2. **Use efficient column selection**:
```sql
-- Good: Select only needed columns
SELECT user_name, query_id, execution_time

-- Bad: Select all columns
SELECT *
```

3. **Leverage clustering**:
```sql
-- Filter on clustered columns first
WHERE DATE(start_time) = '2024-01-15'
  AND user_name = 'SPECIFIC_USER'
```

4. **Handle NULL values**:
```sql
COALESCE(credits_used, 0) as credits_used
```

### Testing

Run tests locally:
```bash
python test_optimization_tools.py
```

Test individual tools:
```python
from src.tools.security import analyze_user_authentication

result = analyze_user_authentication(['TEST_USER'])
print(result)
```

## API Reference

### Generic Tools

#### execute_query(query: str, limit: int = 1000, interpret: bool = True) -> str
Executes a SELECT query against ACCOUNT_USAGE schema.

**Parameters:**
- `query`: SQL SELECT statement to execute
- `limit`: Maximum rows to return (default: 1000)
- `interpret`: Enable AI interpretation of results (default: True)

**Returns:** Formatted query results with optional interpretation

**Raises:** Error if query is not read-only or execution fails

---

#### explore_schema(table_pattern: str = None, show_columns: bool = False) -> str
Explores available tables and columns in ACCOUNT_USAGE schema.

**Parameters:**
- `table_pattern`: SQL LIKE pattern to filter tables (e.g., '%HISTORY%')
- `show_columns`: Include column information for each table

**Returns:** Formatted list of tables with metadata

---

#### help_build_query(description: str) -> str
Generates SQL query suggestions from natural language description.

**Parameters:**
- `description`: Natural language description of desired query

**Returns:** Suggested queries with explanations

### Security Tools

#### check_user_authentication(users: List[str] = None, days_back: int = 30) -> str
Analyzes authentication methods for specified users.

**Parameters:**
- `users`: List of usernames to analyze (None for all users)
- `days_back`: Number of days to look back (default: 30)

**Returns:** Authentication analysis with security recommendations

---

#### audit_privileges(days_back: int = 7, role_filter: str = None) -> str
Tracks privilege and role changes.

**Parameters:**
- `days_back`: Number of days to look back (default: 7)
- `role_filter`: Optional role name to filter

**Returns:** Privilege change audit with security analysis

---

#### detect_anomalies(days_back: int = 7, sensitivity: str = "medium") -> str
Detects unusual access patterns using behavioral analysis.

**Parameters:**
- `days_back`: Number of days to analyze (default: 7)
- `sensitivity`: Detection sensitivity - 'low', 'medium', or 'high'

**Returns:** Anomaly detection report with risk assessment

### Configuration Options

Environment variables that control server behavior:

| Variable | Description | Default |
|----------|-------------|---------|
| SNOWFLAKE_ACCOUNT | Snowflake account identifier | Required |
| SNOWFLAKE_USER | Username for authentication | Required |
| SNOWFLAKE_PASSWORD | Password (if not using RSA) | Optional |
| SNOWFLAKE_PRIVATE_KEY_PATH | Path to RSA private key | Optional |
| SNOWFLAKE_PRIVATE_KEY_PASSPHRASE | RSA key passphrase | Optional |
| SNOWFLAKE_WAREHOUSE | Compute warehouse to use | Required |
| SNOWFLAKE_ROLE | Role to use (needs ACCOUNTADMIN) | ACCOUNTADMIN |
| SNOWFLAKE_CREDIT_PRICE | Per-credit cost for calculations | 4.00 |
| QUERY_TIMEOUT_SECONDS | Maximum query execution time | 300 |
| CACHE_RESULTS | Enable result caching | true |
| DEBUG_MODE | Enable detailed logging | false |

## Conclusion

The Snowflake Account Intelligence Server transforms Claude into your personal Snowflake analyst, capable of executing any query, identifying security risks, optimizing costs, and providing actionable insights. Its flexible architecture allows both ad-hoc exploration and systematic analysis, making it suitable for DBAs, security teams, and data analysts alike.

For support, feature requests, or contributions, please visit the [GitHub repository](https://github.com/yourusername/mcp-snowflake-server).