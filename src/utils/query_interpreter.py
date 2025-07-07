"""
Query result interpretation helper for AI-powered analysis
"""

import pandas as pd
from typing import List, Dict, Any

class QueryInterpreter:
    """Helper class to interpret and summarize query results"""
    
    def summarize_results(self, df: pd.DataFrame, query_type: str) -> str:
        """
        Generate human-readable summary based on query type and results.
        
        Args:
            df: Query results DataFrame
            query_type: Type of query (authentication, performance, cost, etc.)
        
        Returns:
            Summary text
        """
        if df.empty:
            return "No data to summarize."
        
        summary = []
        
        # Basic statistics
        summary.append(f"Analyzed {len(df):,} records")
        
        # Type-specific summaries
        if query_type == 'authentication':
            if 'USER_NAME' in df.columns:
                summary.append(f"Found {df['USER_NAME'].nunique()} unique users")
            if 'FIRST_AUTHENTICATION_FACTOR' in df.columns:
                auth_methods = df['FIRST_AUTHENTICATION_FACTOR'].value_counts()
                summary.append(f"Authentication methods: {dict(auth_methods)}")
        
        elif query_type == 'performance':
            if 'EXECUTION_TIME' in df.columns:
                exec_time_sec = df['EXECUTION_TIME'] / 1000  # Convert to seconds
                summary.append(f"Average execution: {exec_time_sec.mean():.2f}s")
                summary.append(f"Max execution: {exec_time_sec.max():.2f}s")
            if 'WAREHOUSE_NAME' in df.columns:
                summary.append(f"Warehouses involved: {df['WAREHOUSE_NAME'].nunique()}")
        
        elif query_type == 'cost':
            if 'CREDITS_USED' in df.columns:
                total_credits = df['CREDITS_USED'].sum()
                summary.append(f"Total credits: {total_credits:.2f}")
                if 'WAREHOUSE_NAME' in df.columns:
                    top_warehouse = df.groupby('WAREHOUSE_NAME')['CREDITS_USED'].sum().idxmax()
                    summary.append(f"Highest consumer: {top_warehouse}")
        
        elif query_type == 'security':
            if 'GRANTEE_NAME' in df.columns:
                summary.append(f"Users/roles affected: {df['GRANTEE_NAME'].nunique()}")
            if 'ACTION' in df.columns and df['ACTION'].isin(['GRANT', 'REVOKE']).any():
                grants = len(df[df['ACTION'] == 'GRANT'])
                revokes = len(df[df['ACTION'] == 'REVOKE'])
                summary.append(f"Grants: {grants}, Revokes: {revokes}")
        
        return " | ".join(summary)
    
    def suggest_actions(self, df: pd.DataFrame, context: str) -> List[str]:
        """
        Suggest actionable next steps based on results and context.
        
        Args:
            df: Query results DataFrame
            context: Query context or description
        
        Returns:
            List of suggested actions
        """
        suggestions = []
        
        if df.empty:
            suggestions.append("Consider expanding search criteria or checking data latency")
            return suggestions
        
        context_lower = context.lower()
        
        # Context-based suggestions
        if 'authentication' in context_lower or 'login' in context_lower:
            if 'FIRST_AUTHENTICATION_FACTOR' in df.columns:
                if 'PASSWORD' in df['FIRST_AUTHENTICATION_FACTOR'].values:
                    suggestions.append("Migrate password-only users to RSA key authentication")
                if df['FIRST_AUTHENTICATION_FACTOR'].nunique() > 1:
                    suggestions.append("Standardize authentication methods across users")
        
        if 'performance' in context_lower or 'slow' in context_lower:
            if 'EXECUTION_TIME' in df.columns:
                slow_threshold = 60000  # 1 minute in milliseconds
                slow_queries = df[df['EXECUTION_TIME'] > slow_threshold]
                if not slow_queries.empty:
                    suggestions.append(f"Optimize {len(slow_queries)} queries taking over 1 minute")
                    suggestions.append("Consider Query Acceleration Service for long-running queries")
        
        if 'cost' in context_lower or 'credit' in context_lower:
            if 'CREDITS_USED' in df.columns:
                total_credits = df['CREDITS_USED'].sum()
                if total_credits > 100:  # Arbitrary threshold
                    suggestions.append("Review warehouse sizing to reduce credit consumption")
                    suggestions.append("Implement auto-suspend policies for idle warehouses")
        
        if 'security' in context_lower or 'access' in context_lower:
            suggestions.append("Set up alerts for privilege escalation")
            suggestions.append("Conduct regular access reviews")
            if 'ROLE' in df.columns and 'ACCOUNTADMIN' in df['ROLE'].values:
                suggestions.append("Review ACCOUNTADMIN role assignments")
        
        # General suggestions
        if len(df) > 1000:
            suggestions.append("Consider creating summary views for large result sets")
        
        return suggestions
    
    def format_for_display(self, df: pd.DataFrame, max_rows: int = 20) -> str:
        """
        Format DataFrame for clean CLI display.
        
        Args:
            df: DataFrame to format
            max_rows: Maximum rows to display
        
        Returns:
            Formatted string representation
        """
        if df.empty:
            return "No data to display."
        
        # Limit columns width for better display
        pd.set_option('display.max_colwidth', 50)
        pd.set_option('display.max_columns', 10)
        
        # Create display dataframe
        if len(df) > max_rows:
            display_df = pd.concat([
                df.head(int(max_rows/2)),
                pd.DataFrame([['...' for _ in df.columns]], columns=df.columns),
                df.tail(int(max_rows/2))
            ])
            result = f"Showing {max_rows} of {len(df)} rows (truncated):\n\n"
        else:
            display_df = df
            result = ""
        
        result += display_df.to_string(index=False)
        
        # Reset pandas options
        pd.reset_option('display.max_colwidth')
        pd.reset_option('display.max_columns')
        
        return result