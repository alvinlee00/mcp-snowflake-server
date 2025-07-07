# Quick Start Guide

## 🚀 Get Running in 5 Minutes

### Step 1: Install Dependencies
```bash
cd mcp-snowflake-server
pip install -r requirements.txt
```

### Step 2: Configure Snowflake
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### Step 3: Configure Claude Desktop
Add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "snowflake-intelligence": {
      "command": "python",
      "args": ["/absolute/path/to/mcp-snowflake-server/src/server.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account.snowflakecomputing.com",
        "SNOWFLAKE_USER": "your-username",
        "SNOWFLAKE_PASSWORD": "your-password",
        "SNOWFLAKE_WAREHOUSE": "your-warehouse",
        "SNOWFLAKE_ROLE": "ACCOUNTADMIN"
      }
    }
  }
}
```

### Step 4: Test It Out
Restart Claude Desktop and try:

```
Show me what tables are available in ACCOUNT_USAGE
```

## 🎯 Common Use Cases

### Security Audit
```
Check if these users are using RSA authentication: ['USER1', 'USER2']
```

### Cost Analysis
```
Can you help me optimize my Snowflake costs?
```

### Custom Query
```
Execute this query: SELECT COUNT(*) FROM LOGIN_HISTORY WHERE IS_SUCCESS = 'NO'
```

### Performance Analysis
```
Find my slowest queries from the last 24 hours
```

## 🔧 Troubleshooting

**Connection issues?**
- Verify account format: `account.region.cloud`
- Check user has ACCOUNTADMIN role

**No data returned?**
- Remember ACCOUNT_USAGE has 45min-3hr latency
- Check date filters aren't too restrictive

**Query timeouts?**
- Increase `QUERY_TIMEOUT_SECONDS` in .env
- Add more restrictive date filters

## 📚 Next Steps

- Read the full [DOCUMENTATION.md](DOCUMENTATION.md)
- Explore all available tools and prompts
- Set up RSA key authentication for production use