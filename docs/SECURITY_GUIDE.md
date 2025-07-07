# Security Guide

## Overview

The Snowflake Account Intelligence Server is designed with security as a fundamental principle. This guide covers security considerations, best practices, and implementation details.

## Core Security Principles

### 1. Read-Only Access
- **No Data Modification**: Only SELECT queries are permitted
- **Query Validation**: All queries are validated before execution
- **Safe Operations**: No DDL, DML, or administrative commands allowed

### 2. Principle of Least Privilege
- **Dedicated User**: Create a monitoring-specific user
- **Role-Based Access**: Use minimum required roles
- **Time-Limited Sessions**: Configure session timeouts

### 3. Defense in Depth
- **Multiple Validation Layers**: Query syntax, permissions, timeouts
- **Connection Security**: TLS encryption, certificate validation
- **Input Sanitization**: Protection against SQL injection

## Authentication Methods

### RSA Key Authentication (Recommended)

#### Benefits
- **Stronger Security**: No password transmission
- **Key Rotation**: Easy to rotate without service interruption
- **Audit Trail**: Clear authentication method tracking
- **Non-Interactive**: Suitable for automated systems

#### Implementation

1. **Generate Strong Keys**:
```bash
# Generate 2048-bit RSA key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt

# Generate public key
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

2. **Secure Key Storage**:
```bash
# Set restrictive permissions
chmod 600 snowflake_key.p8
chmod 644 snowflake_key.pub

# Store in secure location
sudo mkdir -p /etc/snowflake/keys
sudo mv snowflake_key.p8 /etc/snowflake/keys/
sudo chown root:root /etc/snowflake/keys/snowflake_key.p8
```

3. **Configure Snowflake User**:
```sql
-- Set RSA public key
ALTER USER monitoring_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0B...';

-- Verify configuration
DESC USER monitoring_user;
```

4. **Key Rotation Process**:
```sql
-- Generate new key pair
-- Upload new public key
ALTER USER monitoring_user SET RSA_PUBLIC_KEY='NEW_KEY_HERE';

-- Test new key works
-- Remove old key from server
-- Update application configuration
```

### Password Authentication

#### When to Use
- Development environments only
- Quick testing and prototyping
- Temporary access scenarios

#### Security Requirements
- **Strong Passwords**: Minimum 12 characters, mixed case, numbers, symbols
- **Secure Storage**: Environment variables only, never hardcoded
- **Regular Rotation**: Change passwords regularly
- **MFA Enabled**: Use multi-factor authentication when possible

## User and Role Configuration

### Dedicated Monitoring User

Create a user specifically for monitoring:

```sql
-- Create monitoring user
CREATE USER snowflake_monitor_user 
    PASSWORD = 'StrongP@ssw0rd123!'
    DEFAULT_ROLE = 'ACCOUNT_MONITOR'
    DEFAULT_WAREHOUSE = 'MONITORING_WH'
    COMMENT = 'User for MCP Snowflake Intelligence Server';

-- Set RSA key (recommended)
ALTER USER snowflake_monitor_user 
    SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0B...';
```

### Role Strategy Options

#### Option 1: ACCOUNTADMIN (Full Access)
```sql
-- Grant ACCOUNTADMIN role
GRANT ROLE ACCOUNTADMIN TO USER snowflake_monitor_user;
```

**Pros:**
- Complete ACCOUNT_USAGE access
- All monitoring capabilities available
- Simple configuration

**Cons:**
- High privilege level
- Security risk if compromised
- May violate principle of least privilege

#### Option 2: Custom Monitoring Role (Recommended)
```sql
-- Create custom monitoring role
CREATE ROLE account_monitor;

-- Grant necessary privileges
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE account_monitor;
GRANT USAGE ON WAREHOUSE monitoring_wh TO ROLE account_monitor;

-- Grant to user
GRANT ROLE account_monitor TO USER snowflake_monitor_user;
ALTER USER snowflake_monitor_user SET DEFAULT_ROLE = 'ACCOUNT_MONITOR';
```

**Pros:**
- Minimum required privileges
- Follows security best practices
- Easier to audit and control

**Cons:**
- May miss some ACCOUNT_USAGE tables
- Requires more setup
- Need to verify all tools work

#### Option 3: Hybrid Approach
```sql
-- Create monitoring role with specific grants
CREATE ROLE snowflake_monitor;

-- Grant access to key ACCOUNT_USAGE tables
GRANT SELECT ON snowflake.account_usage.query_history TO ROLE snowflake_monitor;
GRANT SELECT ON snowflake.account_usage.login_history TO ROLE snowflake_monitor;
GRANT SELECT ON snowflake.account_usage.access_history TO ROLE snowflake_monitor;
GRANT SELECT ON snowflake.account_usage.warehouse_metering_history TO ROLE snowflake_monitor;
GRANT SELECT ON snowflake.account_usage.grants_to_users TO ROLE snowflake_monitor;
GRANT SELECT ON snowflake.account_usage.grants_to_roles TO ROLE snowflake_monitor;
-- Add other tables as needed

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE monitoring_wh TO ROLE snowflake_monitor;

-- Assign to user
GRANT ROLE snowflake_monitor TO USER snowflake_monitor_user;
```

## Network Security

### Connection Security
- **TLS Encryption**: All connections use TLS 1.2+
- **Certificate Validation**: Verify Snowflake certificates
- **Private Endpoints**: Use Snowflake Private Link if available

### Firewall Configuration
```bash
# Allow outbound HTTPS to Snowflake
# Account-specific endpoint
account.region.snowflakecomputing.com:443

# Regional endpoints may vary
*.snowflakecomputing.com:443
```

### Network Access Control
- **IP Whitelisting**: Configure Snowflake network policies
- **VPN Access**: Route through corporate VPN
- **Private Connectivity**: Use AWS PrivateLink, Azure Private Link, or GCP Private Service Connect

```sql
-- Example network policy
CREATE NETWORK POLICY monitoring_policy
    ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8')
    COMMENT = 'Restrict monitoring user access to corporate network';

-- Apply to user
ALTER USER snowflake_monitor_user SET NETWORK_POLICY = 'monitoring_policy';
```

## Query Security

### Read-Only Enforcement

The server implements multiple layers of read-only protection:

1. **Syntax Validation**:
```python
def is_read_only_query(query: str) -> bool:
    """Validate that a query is read-only"""
    query_upper = re.sub(r'--.*', '', query).upper().strip()
    
    # Blocked keywords
    dangerous_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
        'TRUNCATE', 'MERGE', 'COPY', 'GRANT', 'REVOKE'
    ]
    
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return False
    
    return query_upper.startswith(('SELECT', 'WITH', 'SHOW'))
```

2. **Schema Restriction**:
- Only SNOWFLAKE.ACCOUNT_USAGE schema accessible
- No access to user databases or sensitive schemas
- Cannot query system administration functions

3. **Resource Limits**:
```python
# Automatic query limits
MAX_ROWS = 10000
QUERY_TIMEOUT = 300  # 5 minutes
MAX_RESULT_SIZE = 100 * 1024 * 1024  # 100MB
```

### SQL Injection Prevention

- **Parameterized Queries**: Use proper parameter binding
- **Input Validation**: Sanitize user inputs
- **Query Templates**: Use pre-built, validated query templates

```python
# Safe query construction
def build_safe_query(table_name: str, days_back: int) -> str:
    # Validate table name against whitelist
    allowed_tables = ['QUERY_HISTORY', 'LOGIN_HISTORY', 'ACCESS_HISTORY']
    if table_name not in allowed_tables:
        raise ValueError(f"Table {table_name} not allowed")
    
    # Use parameterized query
    return f"""
    SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.{table_name}
    WHERE timestamp_column >= DATEADD(day, -{int(days_back)}, CURRENT_TIMESTAMP())
    LIMIT 1000
    """
```

## Data Privacy and Compliance

### Data Handling
- **Local Processing**: All data processing occurs locally
- **No External Transmission**: Query results stay within Claude Desktop
- **Session Isolation**: Each session is independent
- **No Persistent Storage**: Results are not cached permanently

### Compliance Considerations
- **GDPR**: Ensure proper data handling for EU users
- **SOX**: Maintain audit trails for financial data
- **HIPAA**: Additional controls for healthcare environments
- **Industry Standards**: Follow sector-specific requirements

### Audit Trail
```sql
-- Monitor server usage through ACCOUNT_USAGE
SELECT 
    user_name,
    query_text,
    start_time,
    execution_status,
    warehouse_name
FROM snowflake.account_usage.query_history
WHERE user_name = 'SNOWFLAKE_MONITOR_USER'
    AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

## Incident Response

### Security Event Detection

1. **Monitor Authentication Anomalies**:
```sql
-- Detect unusual login patterns for monitoring user
SELECT 
    event_timestamp,
    first_authentication_factor,
    reported_client_type,
    is_success
FROM snowflake.account_usage.login_history
WHERE user_name = 'SNOWFLAKE_MONITOR_USER'
    AND event_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC;
```

2. **Track Query Patterns**:
```sql
-- Monitor for suspicious query activity
SELECT 
    start_time,
    query_text,
    execution_status,
    error_message,
    bytes_scanned
FROM snowflake.account_usage.query_history
WHERE user_name = 'SNOWFLAKE_MONITOR_USER'
    AND (
        bytes_scanned > 10 * 1024 * 1024 * 1024  -- >10GB scanned
        OR total_elapsed_time > 300000  -- >5 minutes
        OR execution_status != 'SUCCESS'
    )
    AND start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP());
```

### Response Procedures

1. **Immediate Response**:
   - Disable compromised user account
   - Rotate authentication credentials
   - Review recent query history

2. **Investigation**:
   - Analyze authentication logs
   - Review query patterns
   - Check for data exfiltration

3. **Recovery**:
   - Create new monitoring user
   - Update server configuration
   - Implement additional controls

### Emergency Procedures

```sql
-- Emergency: Disable monitoring user
ALTER USER snowflake_monitor_user SET DISABLED = TRUE;

-- Emergency: Revoke all roles
REVOKE ROLE accountadmin FROM USER snowflake_monitor_user;

-- Emergency: Drop user if necessary
DROP USER snowflake_monitor_user;
```

## Security Monitoring

### Automated Alerts

Set up alerts for security events:

1. **Failed Authentication Attempts**:
```sql
-- Alert on multiple failed logins
SELECT COUNT(*) as failed_attempts
FROM snowflake.account_usage.login_history
WHERE user_name = 'SNOWFLAKE_MONITOR_USER'
    AND is_success = 'NO'
    AND event_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
HAVING COUNT(*) > 5;
```

2. **Unusual Query Patterns**:
```sql
-- Alert on high-volume queries
SELECT COUNT(*) as query_count
FROM snowflake.account_usage.query_history
WHERE user_name = 'SNOWFLAKE_MONITOR_USER'
    AND start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    AND bytes_scanned > 1024 * 1024 * 1024  -- >1GB
HAVING COUNT(*) > 10;
```

### Regular Security Reviews

1. **Weekly Reviews**:
   - Authentication method compliance
   - Query pattern analysis
   - Performance metrics

2. **Monthly Reviews**:
   - Role and privilege audit
   - Security configuration verification
   - Incident analysis

3. **Quarterly Reviews**:
   - Key rotation
   - Security policy updates
   - Compliance assessment

## Best Practices Summary

### Do's
✅ Use RSA key authentication for production
✅ Create dedicated monitoring users
✅ Implement network access controls
✅ Monitor query patterns and authentication
✅ Rotate credentials regularly
✅ Follow principle of least privilege
✅ Maintain audit trails
✅ Test security controls regularly

### Don'ts
❌ Hardcode credentials in source code
❌ Use ACCOUNTADMIN unless necessary
❌ Allow unrestricted network access
❌ Ignore security monitoring
❌ Share monitoring credentials
❌ Skip key rotation
❌ Bypass query validation
❌ Grant unnecessary privileges

## Security Checklist

### Initial Setup
- [ ] Create dedicated monitoring user
- [ ] Configure RSA key authentication
- [ ] Set up network access controls
- [ ] Implement minimum required roles
- [ ] Configure query timeouts and limits
- [ ] Set up audit logging

### Ongoing Operations
- [ ] Monitor authentication patterns
- [ ] Review query activity regularly
- [ ] Rotate credentials quarterly
- [ ] Update security configurations
- [ ] Test incident response procedures
- [ ] Maintain compliance documentation

### Incident Response
- [ ] Document response procedures
- [ ] Test emergency procedures
- [ ] Maintain contact information
- [ ] Regular security assessments
- [ ] Update security policies
- [ ] Train operational staff

## Conclusion

Security is paramount when accessing Snowflake's ACCOUNT_USAGE data. By following these guidelines, you can maintain a secure monitoring environment while providing valuable insights to your organization. Regular reviews and updates of security measures ensure ongoing protection against evolving threats.