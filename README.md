# MCP Snowflake Account Intelligence Server

A Model Context Protocol (MCP) server that provides comprehensive Snowflake account analysis through flexible query tools and intelligent insights. Execute custom queries, monitor security, optimize costs, and analyze performance - all through natural language interactions.

## Features

### 🔍 Generic Query Capabilities (NEW)
- **Execute Custom Queries**: Run any SELECT query against ACCOUNT_USAGE with AI interpretation
- **Schema Explorer**: Discover available tables and columns in ACCOUNT_USAGE
- **Query Builder Assistant**: Get help building queries from natural language
- **Smart Result Interpretation**: AI analyzes results and suggests next steps

### 🔐 Security & Access Monitoring (NEW)
- **Authentication Analysis**: Track RSA vs password usage across users
- **Privilege Auditing**: Monitor role grants and permission changes
- **Anomaly Detection**: Identify unusual access patterns and potential threats
- **Compliance Reporting**: Security-focused analysis for audits

### 🚀 Performance Analysis
- **Slow Query Detection**: Find and analyze your slowest queries
- **Query Pattern Analysis**: Identify frequently repeated expensive queries
- **Execution Time Distribution**: Understand your query performance patterns
- **Query Acceleration Opportunities**: Find queries eligible for acceleration

### 💰 Cost Optimization
- **Warehouse Cost Analysis**: Track credit consumption by warehouse
- **Cost Per Query Analysis**: Calculate efficiency metrics
- **Expensive Query Identification**: Find your most costly operations
- **User Cost Analysis**: Monitor resource consumption by user

### 📊 Monitoring & Utilization
- **Warehouse Utilization**: Analyze usage patterns and sizing
- **Optimization Reports**: Comprehensive analysis with recommendations
- **Real-time Monitoring**: Track usage trends and anomalies

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd mcp-snowflake-server

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Copy `.env.example` to `.env` and configure authentication:

```bash
cp .env.example .env
```

#### Option A: Password Authentication (Simple)
Edit `.env`:
```env
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_CREDIT_PRICE=4.00
```

#### Option B: RSA Key Authentication (Recommended)
1. **Generate RSA key pair** (if you don't have one):
```bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

2. **Add public key to Snowflake user**:
```sql
-- In Snowflake, run this with your public key content
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0B...';
```

3. **Configure .env**:
```env
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USER=your-username
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your-passphrase-if-any
SNOWFLAKE_CREDIT_PRICE=4.00
```

### 3. Test the Server

```bash
# Test the server locally
cd src
python server.py
```

### 4. Configure Claude Desktop

Add the server to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "snowflake-optimizer": {
      "command": "python",
      "args": ["/absolute/path/to/mcp-snowflake-server/src/server.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account.snowflakecomputing.com",
        "SNOWFLAKE_USER": "your-username",
        "SNOWFLAKE_PASSWORD": "your-password",
        "SNOWFLAKE_WAREHOUSE": "your-warehouse",
        "SNOWFLAKE_ROLE": "ACCOUNTADMIN",
        "SNOWFLAKE_CREDIT_PRICE": "4.00"
      }
    }
  }
}
```

Restart Claude Desktop to load the server.

## Available Tools

### Generic Query Tools (NEW)
- `execute_query(query, limit=1000, interpret=True)` - Execute any SELECT query with AI interpretation
- `explore_schema(table_pattern=None, show_columns=False)` - Explore ACCOUNT_USAGE tables and columns
- `help_build_query(description)` - Get help building queries from natural language

### Security Tools (NEW)
- `check_user_authentication(users=None, days_back=30)` - Analyze RSA vs password authentication
- `audit_privileges(days_back=7, role_filter=None)` - Track privilege and role changes
- `detect_anomalies(days_back=7, sensitivity="medium")` - Detect unusual access patterns

### Performance Tools
- `find_slow_queries(hours_back=24, limit=50)` - Find slowest queries
- `analyze_repeated_queries(hours_back=168, limit=50)` - Identify repeated expensive patterns
- `query_execution_distribution(days_back=7)` - Analyze execution time distribution
- `query_acceleration_candidates(days_back=7, limit=50)` - Find acceleration opportunities

### Cost Tools
- `warehouse_cost_analysis(days_back=7)` - Analyze warehouse costs
- `cost_per_query_analysis(days_back=30)` - Calculate cost per query
- `find_most_expensive_queries(days_back=7, limit=25)` - Find expensive queries
- `user_cost_analysis(days_back=7)` - Analyze costs by user

### Monitoring Tools
- `warehouse_utilization_analysis(days_back=7)` - Analyze warehouse utilization
- `optimization_report(days_back=7)` - Generate comprehensive report

## Available Prompts

Use these prompts in Claude Desktop for guided workflows:

- **"optimize_snowflake_costs"** - Complete cost optimization workflow
- **"find_performance_bottlenecks"** - Performance analysis workflow  
- **"weekly_optimization_review"** - Generate weekly optimization report
- **"security_audit"** - Comprehensive security analysis (NEW)
- **"custom_analysis"** - Interactive query building assistance (NEW)

## Example Usage

### Generic Query Execution (NEW):

```
Execute this query: SELECT COUNT(*) FROM LOGIN_HISTORY WHERE FIRST_AUTHENTICATION_FACTOR = 'PASSWORD'
```

```
Show me all tables in ACCOUNT_USAGE that contain user activity data
```

```
Help me build a query to find users who haven't logged in for 30 days
```

### Security Analysis (NEW):

```
Check if these users are using both RSA and password: ['USER1', 'USER2', 'USER3']
```

```
Audit privilege changes for the ACCOUNTADMIN role in the last week
```

```
Detect any unusual access patterns with high sensitivity
```

### Cost & Performance:

```
Can you help me optimize my Snowflake costs?
```

```
Use the find_slow_queries tool to show me queries from the last 48 hours
```

```
Generate an optimization report for the last 14 days
```

## Requirements

- Python 3.10+
- Snowflake account with ACCOUNTADMIN role (for ACCOUNT_USAGE schema access)
- Claude Desktop (for MCP integration)

## Important Notes

- **Data Latency**: ACCOUNT_USAGE views have latency:
  - QUERY_HISTORY: 45 minutes
  - WAREHOUSE_METERING_HISTORY: 3 hours
  - WAREHOUSE_LOAD_HISTORY: 3 hours
  - QUERY_ACCELERATION_ELIGIBLE: 3 hours
- **Data Retention**: Historical data is available for 365 days
- **Permissions**: ACCOUNTADMIN role required for ACCOUNT_USAGE schema access
- **Performance**: ACCOUNT_USAGE queries can be slow on large datasets; consider adding date filters
- **Column Selection**: Always select specific columns instead of using SELECT * for better performance

## Security Notes

- Store credentials securely in environment variables
- Use least-privilege access when possible
- The ACCOUNTADMIN role is required for ACCOUNT_USAGE schema access
- Consider using key-pair authentication for production deployments

## Project Structure

```
mcp-snowflake-server/
├── src/
│   ├── server.py              # Main MCP server
│   ├── tools/                 # MCP tool implementations
│   │   ├── performance.py     # Performance analysis tools
│   │   ├── costs.py          # Cost optimization tools
│   │   └── monitoring.py     # Monitoring tools
│   ├── queries/              # SQL query templates
│   │   └── optimization_queries.py
│   └── utils/               # Utilities
│       └── snowflake_connection.py
├── requirements.txt
├── .env.example
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test
4. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
1. Check the GitHub issues
2. Review Snowflake documentation for ACCOUNT_USAGE schema
3. Consult MCP documentation at modelcontextprotocol.io