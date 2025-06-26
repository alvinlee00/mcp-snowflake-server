#!/usr/bin/env python3
"""
Test individual Snowflake optimization tools to diagnose empty result issues
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tools.performance import analyze_slow_queries
from tools.costs import analyze_warehouse_costs
from utils.snowflake_connection import snowflake_conn

def test_connection():
    """Test basic connection to Snowflake"""
    print("=" * 50)
    print("TESTING BASIC CONNECTION")
    print("=" * 50)
    
    try:
        # Try a simple query to verify connection
        query = "SELECT CURRENT_TIMESTAMP() as test_time, CURRENT_USER() as test_user, CURRENT_ROLE() as test_role"
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            print("❌ Connection test failed - no results returned")
            return False
        else:
            print("✅ Connection successful!")
            print(f"Current time: {df.iloc[0]['TEST_TIME']}")
            print(f"Current user: {df.iloc[0]['TEST_USER']}")
            print(f"Current role: {df.iloc[0]['TEST_ROLE']}")
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False

def test_account_usage_access():
    """Test access to ACCOUNT_USAGE schema"""
    print("\n" + "=" * 50)
    print("TESTING ACCOUNT_USAGE ACCESS")
    print("=" * 50)
    
    try:
        # Try to query ACCOUNT_USAGE tables
        test_queries = [
            ("QUERY_HISTORY table", "SELECT COUNT(*) as query_count FROM snowflake.account_usage.query_history WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())"),
            ("WAREHOUSE_METERING_HISTORY table", "SELECT COUNT(*) as warehouse_count FROM snowflake.account_usage.warehouse_metering_history WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())"),
            ("WAREHOUSES table", "SELECT COUNT(*) as warehouse_def_count FROM snowflake.account_usage.warehouses"),
        ]
        
        for table_name, query in test_queries:
            try:
                df = snowflake_conn.execute_query(query)
                if df.empty:
                    print(f"❌ {table_name}: No access or query failed")
                else:
                    count = df.iloc[0].iloc[0]  # First column of first row
                    print(f"✅ {table_name}: {count} records found")
                    
            except Exception as e:
                print(f"❌ {table_name}: Error - {str(e)}")
                
    except Exception as e:
        print(f"❌ General ACCOUNT_USAGE access error: {str(e)}")

def test_slow_queries():
    """Test the find_slow_queries function with extended time period"""
    print("\n" + "=" * 50)
    print("TESTING SLOW QUERIES (168 hours / 1 week)")
    print("=" * 50)
    
    try:
        result = analyze_slow_queries(hours_back=168, limit=10)
        print("Result:")
        print(result)
        
        if "No queries found" in result:
            print("\n❌ No slow queries found - checking if this is a data or permissions issue...")
            
            # Try a broader query to see if there's ANY query history
            try:
                query = "SELECT COUNT(*) as total_queries FROM snowflake.account_usage.query_history WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())"
                df = snowflake_conn.execute_query(query)
                total_queries = df.iloc[0]['TOTAL_QUERIES']
                print(f"Total queries in last 30 days: {total_queries}")
                
                if total_queries == 0:
                    print("❌ No query history at all - this could indicate:")
                    print("  - Very new account with no activity")
                    print("  - Permission issues")
                    print("  - ACCOUNT_USAGE data latency (can be up to 45 minutes)")
                else:
                    print("✅ Query history exists, but no slow queries in the specified period")
                    
            except Exception as e:
                print(f"❌ Error checking total query count: {str(e)}")
        else:
            print("✅ Slow queries analysis returned data")
            
    except Exception as e:
        print(f"❌ Error in slow queries analysis: {str(e)}")

def test_warehouse_costs():
    """Test the warehouse_cost_analysis function for 30 days"""
    print("\n" + "=" * 50)
    print("TESTING WAREHOUSE COST ANALYSIS (30 days)")
    print("=" * 50)
    
    try:
        result = analyze_warehouse_costs(days_back=30)
        print("Result:")
        print(result)
        
        if "No warehouse usage data found" in result:
            print("\n❌ No warehouse usage data found - investigating...")
            
            # Check if warehouses exist at all
            try:
                query = "SELECT COUNT(*) as warehouse_count FROM snowflake.account_usage.warehouses"
                df = snowflake_conn.execute_query(query)
                warehouse_count = df.iloc[0]['WAREHOUSE_COUNT']
                print(f"Total warehouses defined: {warehouse_count}")
                
                # Check metering history
                query = "SELECT COUNT(*) as metering_records FROM snowflake.account_usage.warehouse_metering_history WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())"
                df = snowflake_conn.execute_query(query)
                metering_records = df.iloc[0]['METERING_RECORDS']
                print(f"Metering records in last 30 days: {metering_records}")
                
                if warehouse_count == 0:
                    print("❌ No warehouses defined in account")
                elif metering_records == 0:
                    print("❌ No warehouse activity in the last 30 days")
                    print("  This could indicate:")
                    print("  - No queries have been run")
                    print("  - All warehouses have been suspended")
                    print("  - Data latency in ACCOUNT_USAGE")
                else:
                    print("✅ Warehouses and activity exist, issue may be in the query logic")
                    
            except Exception as e:
                print(f"❌ Error investigating warehouse data: {str(e)}")
        else:
            print("✅ Warehouse cost analysis returned data")
            
    except Exception as e:
        print(f"❌ Error in warehouse cost analysis: {str(e)}")

def test_simple_account_usage_query():
    """Test a very simple ACCOUNT_USAGE query"""
    print("\n" + "=" * 50)
    print("TESTING SIMPLE ACCOUNT_USAGE QUERY")
    print("=" * 50)
    
    try:
        # Try the simplest possible query
        query = """
        SELECT 
            COUNT(*) as total_queries,
            MIN(start_time) as earliest_query,
            MAX(start_time) as latest_query
        FROM snowflake.account_usage.query_history 
        """
        
        df = snowflake_conn.execute_query(query)
        
        if df.empty:
            print("❌ Simple ACCOUNT_USAGE query returned no results")
        else:
            print("✅ Simple ACCOUNT_USAGE query successful:")
            print(f"Total queries ever: {df.iloc[0]['TOTAL_QUERIES']}")
            print(f"Earliest query: {df.iloc[0]['EARLIEST_QUERY']}")
            print(f"Latest query: {df.iloc[0]['LATEST_QUERY']}")
            
            # Additional diagnostic info
            if df.iloc[0]['TOTAL_QUERIES'] == 0:
                print("\n❌ Account has no query history at all")
                print("This suggests:")
                print("  - Brand new account")
                print("  - No queries have ever been executed")
                print("  - Severe permissions issue")
            else:
                print(f"\n✅ Account has query history, checking recent activity...")
                
                # Check recent activity
                recent_query = """
                SELECT COUNT(*) as recent_queries
                FROM snowflake.account_usage.query_history 
                WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                """
                
                df_recent = snowflake_conn.execute_query(recent_query)
                recent_count = df_recent.iloc[0]['RECENT_QUERIES']
                print(f"Queries in last 7 days: {recent_count}")
                
                if recent_count == 0:
                    print("❌ No recent query activity - this explains empty optimization reports")
                else:
                    print("✅ Recent activity exists - optimization tools should return data")
                    
    except Exception as e:
        print(f"❌ Error in simple ACCOUNT_USAGE query: {str(e)}")

def main():
    """Run all diagnostic tests"""
    print("🔍 SNOWFLAKE OPTIMIZATION TOOLS DIAGNOSTIC")
    print("This will test each optimization tool individually to diagnose empty result issues\n")
    
    # Test 1: Basic connection
    if not test_connection():
        print("❌ Cannot proceed - basic connection failed")
        return
    
    # Test 2: ACCOUNT_USAGE access
    test_account_usage_access()
    
    # Test 3: Simple ACCOUNT_USAGE query
    test_simple_account_usage_query()
    
    # Test 4: Slow queries analysis
    test_slow_queries()
    
    # Test 5: Warehouse cost analysis
    test_warehouse_costs()
    
    print("\n" + "=" * 50)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 50)
    print("Review the results above to determine:")
    print("1. Whether connection is working")
    print("2. Whether ACCOUNT_USAGE permissions are correct")
    print("3. Whether tables contain data")
    print("4. Whether the issue is permissions, empty tables, or something else")

if __name__ == "__main__":
    main()