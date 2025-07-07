"""
Security and access analysis tools for Snowflake
"""

import pandas as pd
import sys
import os
from typing import List, Optional
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.snowflake_connection import snowflake_conn

def analyze_user_authentication(users: List[str] = None, days_back: int = 30, check_both_methods: bool = True) -> str:
    """
    Analyze authentication methods for specified users.
    
    Args:
        users: List of usernames to analyze (None for all users)
        days_back: Number of days to look back (default: 30)
        check_both_methods: Whether to check for both RSA and password usage (default: True)
    
    Returns:
        Formatted analysis of user authentication methods and recommendations
    """
    try:
        # Build user filter
        user_filter = ""
        if users:
            user_list = ", ".join([f"'{user.upper()}'" for user in users])
            user_filter = f"AND USER_NAME IN ({user_list})"
        
        # Query to analyze authentication methods
        query = f"""
        WITH auth_summary AS (
            SELECT 
                USER_NAME,
                FIRST_AUTHENTICATION_FACTOR,
                MAX(EVENT_TIMESTAMP) as LAST_LOGIN,
                COUNT(*) as LOGIN_COUNT,
                COUNT(DISTINCT DATE(EVENT_TIMESTAMP)) as ACTIVE_DAYS
            FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
            WHERE EVENT_TIMESTAMP >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
                AND IS_SUCCESS = 'YES'
                {user_filter}
            GROUP BY USER_NAME, FIRST_AUTHENTICATION_FACTOR
        ),
        user_auth_pivot AS (
            SELECT 
                USER_NAME,
                MAX(CASE WHEN FIRST_AUTHENTICATION_FACTOR = 'PASSWORD' THEN LAST_LOGIN END) as LAST_PASSWORD_LOGIN,
                MAX(CASE WHEN FIRST_AUTHENTICATION_FACTOR = 'PASSWORD' THEN LOGIN_COUNT END) as PASSWORD_LOGIN_COUNT,
                MAX(CASE WHEN FIRST_AUTHENTICATION_FACTOR LIKE '%RSA%' THEN LAST_LOGIN END) as LAST_RSA_LOGIN,
                MAX(CASE WHEN FIRST_AUTHENTICATION_FACTOR LIKE '%RSA%' THEN LOGIN_COUNT END) as RSA_LOGIN_COUNT,
                SUM(LOGIN_COUNT) as TOTAL_LOGINS
            FROM auth_summary
            GROUP BY USER_NAME
        )
        SELECT 
            USER_NAME,
            LAST_PASSWORD_LOGIN,
            COALESCE(PASSWORD_LOGIN_COUNT, 0) as PASSWORD_LOGIN_COUNT,
            LAST_RSA_LOGIN,
            COALESCE(RSA_LOGIN_COUNT, 0) as RSA_LOGIN_COUNT,
            TOTAL_LOGINS,
            CASE 
                WHEN LAST_RSA_LOGIN IS NULL THEN 'Password Only'
                WHEN LAST_PASSWORD_LOGIN IS NULL THEN 'RSA Only'
                WHEN DATEDIFF(day, LAST_PASSWORD_LOGIN, CURRENT_TIMESTAMP()) > 7 
                    AND LAST_RSA_LOGIN > LAST_PASSWORD_LOGIN THEN 'Migrating to RSA'
                ELSE 'Both Methods Active'
            END as AUTH_STATUS
        FROM user_auth_pivot
        ORDER BY 
            CASE 
                WHEN LAST_RSA_LOGIN IS NULL THEN 0  -- Password only users first
                WHEN LAST_PASSWORD_LOGIN IS NOT NULL AND LAST_RSA_LOGIN IS NOT NULL THEN 1  -- Both methods
                ELSE 2  -- RSA only
            END,
            USER_NAME
        """
        
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No login data found for the specified users in the last {days_back} days."
        
        result = f"## User Authentication Analysis (Last {days_back} Days)\n\n"
        
        if users:
            result += f"**Analyzing {len(users)} specified users**\n\n"
        else:
            result += f"**Analyzing all {len(df)} active users**\n\n"
        
        # Summary statistics
        total_users = len(df)
        password_only = len(df[df['AUTH_STATUS'] == 'Password Only'])
        rsa_only = len(df[df['AUTH_STATUS'] == 'RSA Only'])
        both_active = len(df[df['AUTH_STATUS'] == 'Both Methods Active'])
        migrating = len(df[df['AUTH_STATUS'] == 'Migrating to RSA'])
        
        result += "### Summary:\n"
        result += f"- Total Users: {total_users}\n"
        result += f"- Password Only: {password_only} ({password_only/total_users*100:.1f}%)\n"
        result += f"- RSA Only: {rsa_only} ({rsa_only/total_users*100:.1f}%)\n"
        result += f"- Both Methods Active: {both_active} ({both_active/total_users*100:.1f}%)\n"
        result += f"- Migrating to RSA: {migrating} ({migrating/total_users*100:.1f}%)\n\n"
        
        # Security concerns
        if password_only > 0:
            result += f"⚠️ **Security Alert: {password_only} users still using password-only authentication**\n\n"
        
        if both_active > 0 and check_both_methods:
            result += f"⚡ **Note: {both_active} users actively using both authentication methods**\n\n"
        
        # Detailed user table
        result += "### User Details:\n\n"
        result += "| User | Status | Last Password | Last RSA | Password Logins | RSA Logins |\n"
        result += "|------|--------|---------------|----------|-----------------|------------|\n"
        
        for _, row in df.iterrows():
            user = row['USER_NAME']
            status = row['AUTH_STATUS']
            
            # Format dates
            pwd_date = row['LAST_PASSWORD_LOGIN'].strftime('%Y-%m-%d') if pd.notna(row['LAST_PASSWORD_LOGIN']) else 'Never'
            rsa_date = row['LAST_RSA_LOGIN'].strftime('%Y-%m-%d') if pd.notna(row['LAST_RSA_LOGIN']) else 'Never'
            
            pwd_count = int(row['PASSWORD_LOGIN_COUNT']) if pd.notna(row['PASSWORD_LOGIN_COUNT']) else 0
            rsa_count = int(row['RSA_LOGIN_COUNT']) if pd.notna(row['RSA_LOGIN_COUNT']) else 0
            
            # Add warning emoji for security concerns
            status_display = status
            if status == 'Password Only':
                status_display = f"⚠️ {status}"
            elif status == 'Both Methods Active':
                status_display = f"⚡ {status}"
            
            result += f"| {user} | {status_display} | {pwd_date} | {rsa_date} | {pwd_count} | {rsa_count} |\n"
        
        # Recommendations
        result += "\n### Recommendations:\n\n"
        
        if password_only > 0:
            result += "**High Priority - Password-Only Users:**\n"
            password_only_users = df[df['AUTH_STATUS'] == 'Password Only']['USER_NAME'].tolist()
            for user in password_only_users[:10]:  # Show first 10
                result += f"- {user}: Migrate to RSA key authentication\n"
            if len(password_only_users) > 10:
                result += f"- ... and {len(password_only_users) - 10} more users\n"
            result += "\n"
        
        if both_active > 0:
            result += "**Medium Priority - Both Methods Active:**\n"
            result += "- Consider disabling password authentication for users who have successfully adopted RSA\n"
            result += "- Monitor these users to ensure smooth transition to RSA-only\n\n"
        
        result += "**General Security Best Practices:**\n"
        result += "- Enforce RSA key authentication for all production users\n"
        result += "- Set up alerts for password-based login attempts\n"
        result += "- Regular security audits of authentication methods\n"
        result += "- Consider implementing MFA for additional security\n"
        
        return result
        
    except Exception as e:
        return f"Error analyzing user authentication: {str(e)}"

def audit_privilege_changes(days_back: int = 7, role_filter: str = None) -> str:
    """
    Track privilege and role changes in the account.
    
    Args:
        days_back: Number of days to look back (default: 7)
        role_filter: Optional role name to filter (e.g., 'ACCOUNTADMIN')
    
    Returns:
        Analysis of privilege changes and potential security concerns
    """
    try:
        role_condition = ""
        if role_filter:
            role_condition = f"AND (ROLE = '{role_filter.upper()}' OR NAME = '{role_filter.upper()}')"
        
        # Query for grant history
        query = f"""
        WITH grant_events AS (
            -- Grants to users
            SELECT 
                'USER' as GRANTEE_TYPE,
                GRANTEE_NAME,
                ROLE as GRANTED_OBJECT,
                'ROLE' as OBJECT_TYPE,
                GRANTED_BY,
                CREATED_ON as EVENT_TIME,
                'GRANT' as ACTION
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
            WHERE CREATED_ON >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
                AND DELETED_ON IS NULL
                {role_condition}
            
            UNION ALL
            
            -- Revokes from users (deleted grants)
            SELECT 
                'USER' as GRANTEE_TYPE,
                GRANTEE_NAME,
                ROLE as GRANTED_OBJECT,
                'ROLE' as OBJECT_TYPE,
                GRANTED_BY,
                DELETED_ON as EVENT_TIME,
                'REVOKE' as ACTION
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
            WHERE DELETED_ON >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
                AND DELETED_ON IS NOT NULL
                {role_condition}
            
            UNION ALL
            
            -- Grants to roles
            SELECT 
                'ROLE' as GRANTEE_TYPE,
                GRANTEE_NAME,
                PRIVILEGE || ' ON ' || GRANTED_ON as GRANTED_OBJECT,
                GRANTED_ON as OBJECT_TYPE,
                GRANTED_BY,
                CREATED_ON as EVENT_TIME,
                'GRANT' as ACTION
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
            WHERE CREATED_ON >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
                AND DELETED_ON IS NULL
                AND PRIVILEGE IN ('CREATE', 'OWNERSHIP', 'MANAGE GRANTS', 'IMPORTED PRIVILEGES')
        )
        SELECT 
            EVENT_TIME,
            ACTION,
            GRANTEE_TYPE,
            GRANTEE_NAME,
            GRANTED_OBJECT,
            OBJECT_TYPE,
            GRANTED_BY
        FROM grant_events
        ORDER BY EVENT_TIME DESC
        """
        
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            return f"No privilege changes found in the last {days_back} days."
        
        result = f"## Privilege Change Audit (Last {days_back} Days)\n\n"
        
        # Summary
        total_changes = len(df)
        grants = len(df[df['ACTION'] == 'GRANT'])
        revokes = len(df[df['ACTION'] == 'REVOKE'])
        unique_grantees = df['GRANTEE_NAME'].nunique()
        unique_grantors = df['GRANTED_BY'].nunique()
        
        result += "### Summary:\n"
        result += f"- Total Changes: {total_changes}\n"
        result += f"- Grants: {grants}\n"
        result += f"- Revokes: {revokes}\n"
        result += f"- Unique Grantees: {unique_grantees}\n"
        result += f"- Unique Grantors: {unique_grantors}\n\n"
        
        # Check for sensitive role grants
        sensitive_roles = ['ACCOUNTADMIN', 'SECURITYADMIN', 'SYSADMIN']
        sensitive_grants = df[df['GRANTED_OBJECT'].str.upper().isin(sensitive_roles)]
        
        if not sensitive_grants.empty:
            result += "### ⚠️ Sensitive Role Assignments:\n\n"
            for _, row in sensitive_grants.iterrows():
                result += f"- {row['ACTION']}: **{row['GRANTED_OBJECT']}** to {row['GRANTEE_TYPE']} "
                result += f"'{row['GRANTEE_NAME']}' by {row['GRANTED_BY']} "
                result += f"on {row['EVENT_TIME'].strftime('%Y-%m-%d %H:%M')}\n"
            result += "\n"
        
        # Recent changes table
        result += "### Recent Privilege Changes:\n\n"
        result += "| Time | Action | Grantee | Object | Granted By |\n"
        result += "|------|--------|---------|--------|------------|\n"
        
        for _, row in df.head(20).iterrows():
            time_str = row['EVENT_TIME'].strftime('%Y-%m-%d %H:%M')
            action = row['ACTION']
            grantee = f"{row['GRANTEE_TYPE']}: {row['GRANTEE_NAME']}"
            obj = row['GRANTED_OBJECT'][:50] + '...' if len(row['GRANTED_OBJECT']) > 50 else row['GRANTED_OBJECT']
            
            # Highlight sensitive changes
            if any(role in row['GRANTED_OBJECT'].upper() for role in sensitive_roles):
                action = f"**{action}**"
                obj = f"**{obj}**"
            
            result += f"| {time_str} | {action} | {grantee} | {obj} | {row['GRANTED_BY']} |\n"
        
        if len(df) > 20:
            result += f"\n*Showing 20 of {len(df)} total changes*\n"
        
        # Analysis
        result += "\n### Security Analysis:\n\n"
        
        # Check for unusual patterns
        if unique_grantors == 1:
            result += f"- All grants made by single user: {df['GRANTED_BY'].iloc[0]}\n"
        
        # Check for rapid privilege escalation
        user_grants = df[df['GRANTEE_TYPE'] == 'USER'].groupby('GRANTEE_NAME').size()
        rapid_grants = user_grants[user_grants > 3]
        if not rapid_grants.empty:
            result += f"- Rapid privilege accumulation detected for {len(rapid_grants)} users\n"
            for user, count in rapid_grants.items():
                result += f"  - {user}: {count} new privileges\n"
        
        # Recommendations
        result += "\n### Recommendations:\n"
        result += "- Review all sensitive role assignments (ACCOUNTADMIN, SECURITYADMIN)\n"
        result += "- Implement approval workflow for privilege changes\n"
        result += "- Set up alerts for unauthorized privilege escalation\n"
        result += "- Regular audit of user privileges and role memberships\n"
        result += "- Follow principle of least privilege\n"
        
        return result
        
    except Exception as e:
        return f"Error auditing privilege changes: {str(e)}"

def detect_unusual_access_patterns(days_back: int = 7, sensitivity_level: str = "medium") -> str:
    """
    Identify unusual data access patterns that might indicate security issues.
    
    Args:
        days_back: Number of days to look back (default: 7)
        sensitivity_level: Detection sensitivity - 'low', 'medium', 'high' (default: 'medium')
    
    Returns:
        Analysis of unusual access patterns and potential security concerns
    """
    try:
        # Set thresholds based on sensitivity
        thresholds = {
            'low': {'hour_threshold': 2, 'volume_multiplier': 10, 'new_object_days': 90},
            'medium': {'hour_threshold': 4, 'volume_multiplier': 5, 'new_object_days': 30},
            'high': {'hour_threshold': 8, 'volume_multiplier': 3, 'new_object_days': 7}
        }
        
        config = thresholds.get(sensitivity_level, thresholds['medium'])
        
        # Query for access patterns
        query = f"""
        WITH user_access_stats AS (
            -- Get user access patterns
            SELECT 
                USER_NAME,
                DATE(QUERY_START_TIME) as ACCESS_DATE,
                HOUR(QUERY_START_TIME) as ACCESS_HOUR,
                COUNT(DISTINCT QUERY_ID) as QUERY_COUNT,
                COUNT(DISTINCT BASE_OBJECTS_ACCESSED) as OBJECTS_ACCESSED,
                SUM(ROWS_PRODUCED) as TOTAL_ROWS,
                COUNT(DISTINCT DATABASE_NAME) as DATABASES_ACCESSED
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
            WHERE QUERY_START_TIME >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
            GROUP BY USER_NAME, DATE(QUERY_START_TIME), HOUR(QUERY_START_TIME)
        ),
        user_baseline AS (
            -- Calculate baseline behavior
            SELECT 
                USER_NAME,
                AVG(QUERY_COUNT) as AVG_QUERIES,
                STDDEV(QUERY_COUNT) as STDDEV_QUERIES,
                AVG(OBJECTS_ACCESSED) as AVG_OBJECTS,
                AVG(TOTAL_ROWS) as AVG_ROWS
            FROM user_access_stats
            GROUP BY USER_NAME
        ),
        anomalies AS (
            -- Detect anomalies
            SELECT 
                s.USER_NAME,
                s.ACCESS_DATE,
                s.ACCESS_HOUR,
                s.QUERY_COUNT,
                s.OBJECTS_ACCESSED,
                s.TOTAL_ROWS,
                s.DATABASES_ACCESSED,
                b.AVG_QUERIES,
                b.AVG_ROWS,
                CASE 
                    WHEN s.ACCESS_HOUR < 6 OR s.ACCESS_HOUR > 22 THEN 'Unusual Hours'
                    WHEN s.QUERY_COUNT > b.AVG_QUERIES + ({config['volume_multiplier']} * b.STDDEV_QUERIES) THEN 'High Query Volume'
                    WHEN s.TOTAL_ROWS > b.AVG_ROWS * {config['volume_multiplier']} THEN 'High Data Volume'
                    WHEN s.DATABASES_ACCESSED > 3 THEN 'Multiple Database Access'
                    ELSE 'Normal'
                END as ANOMALY_TYPE
            FROM user_access_stats s
            JOIN user_baseline b ON s.USER_NAME = b.USER_NAME
        ),
        new_object_access AS (
            -- Find access to newly created objects
            SELECT 
                ah.USER_NAME,
                ah.OBJECT_NAME,
                ah.QUERY_START_TIME,
                t.CREATED as OBJECT_CREATED
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah
            JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLES t 
                ON ah.OBJECT_NAME = t.TABLE_CATALOG || '.' || t.TABLE_SCHEMA || '.' || t.TABLE_NAME
            WHERE ah.QUERY_START_TIME >= DATEADD(day, -{days_back}, CURRENT_TIMESTAMP())
                AND t.CREATED >= DATEADD(day, -{config['new_object_days']}, CURRENT_TIMESTAMP())
                AND ah.QUERY_START_TIME < DATEADD(hour, {config['hour_threshold']}, t.CREATED)
        )
        SELECT * FROM (
            SELECT 
                USER_NAME,
                ACCESS_DATE,
                ACCESS_HOUR,
                ANOMALY_TYPE,
                QUERY_COUNT,
                OBJECTS_ACCESSED,
                TOTAL_ROWS,
                DATABASES_ACCESSED
            FROM anomalies
            WHERE ANOMALY_TYPE != 'Normal'
            
            UNION ALL
            
            SELECT 
                USER_NAME,
                DATE(QUERY_START_TIME) as ACCESS_DATE,
                HOUR(QUERY_START_TIME) as ACCESS_HOUR,
                'New Object Access' as ANOMALY_TYPE,
                1 as QUERY_COUNT,
                1 as OBJECTS_ACCESSED,
                0 as TOTAL_ROWS,
                1 as DATABASES_ACCESSED
            FROM new_object_access
        )
        ORDER BY ACCESS_DATE DESC, USER_NAME
        """
        
        df = snowflake_conn.execute_query(query)
        
        result = f"## Unusual Access Pattern Detection (Last {days_back} Days)\n"
        result += f"**Sensitivity Level:** {sensitivity_level.capitalize()}\n\n"
        
        if df.empty:
            result += "No unusual access patterns detected with current sensitivity settings.\n"
            return result
        
        # Summary of anomalies
        anomaly_counts = df['ANOMALY_TYPE'].value_counts()
        unique_users = df['USER_NAME'].nunique()
        
        result += "### Summary:\n"
        result += f"- Total Anomalies Detected: {len(df)}\n"
        result += f"- Affected Users: {unique_users}\n\n"
        
        result += "### Anomaly Breakdown:\n"
        for anomaly_type, count in anomaly_counts.items():
            emoji = {
                'Unusual Hours': '🌙',
                'High Query Volume': '📊',
                'High Data Volume': '💾',
                'Multiple Database Access': '🗄️',
                'New Object Access': '🆕'
            }.get(anomaly_type, '❓')
            result += f"- {emoji} {anomaly_type}: {count} occurrences\n"
        
        result += "\n### Detailed Findings:\n\n"
        
        # Group by user and anomaly type
        user_anomalies = df.groupby(['USER_NAME', 'ANOMALY_TYPE']).size().reset_index(name='OCCURRENCE_COUNT')
        
        # Highlight high-risk users
        high_risk_users = user_anomalies.groupby('USER_NAME').size()
        high_risk_users = high_risk_users[high_risk_users >= 3].index.tolist()
        
        if high_risk_users:
            result += f"#### ⚠️ High Risk Users (3+ anomaly types):\n"
            for user in high_risk_users:
                user_data = df[df['USER_NAME'] == user]
                result += f"\n**{user}:**\n"
                for anomaly in user_data['ANOMALY_TYPE'].unique():
                    count = len(user_data[user_data['ANOMALY_TYPE'] == anomaly])
                    result += f"- {anomaly}: {count} occurrences\n"
        
        # Recent anomalies table
        result += "\n#### Recent Anomalies:\n\n"
        result += "| Date | User | Anomaly Type | Details |\n"
        result += "|------|------|--------------|----------|\n"
        
        for _, row in df.head(20).iterrows():
            date_str = row['ACCESS_DATE'].strftime('%Y-%m-%d')
            details = ""
            
            if row['ANOMALY_TYPE'] == 'Unusual Hours':
                details = f"Hour: {row['ACCESS_HOUR']:02d}:00"
            elif row['ANOMALY_TYPE'] == 'High Query Volume':
                details = f"Queries: {row['QUERY_COUNT']}"
            elif row['ANOMALY_TYPE'] == 'High Data Volume':
                details = f"Rows: {row['TOTAL_ROWS']:,}"
            elif row['ANOMALY_TYPE'] == 'Multiple Database Access':
                details = f"DBs: {row['DATABASES_ACCESSED']}"
            
            result += f"| {date_str} | {row['USER_NAME']} | {row['ANOMALY_TYPE']} | {details} |\n"
        
        if len(df) > 20:
            result += f"\n*Showing 20 of {len(df)} total anomalies*\n"
        
        # Recommendations
        result += "\n### Recommendations:\n"
        result += "- Investigate high-risk users with multiple anomaly types\n"
        result += "- Review access patterns during unusual hours\n"
        result += "- Set up real-time alerts for anomalous behavior\n"
        result += "- Implement data access monitoring dashboards\n"
        result += "- Consider implementing row access policies for sensitive data\n"
        result += f"- Adjust sensitivity level (current: {sensitivity_level}) based on false positive rate\n"
        
        return result
        
    except Exception as e:
        return f"Error detecting unusual access patterns: {str(e)}"