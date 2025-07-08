import os
import snowflake.connector
from dotenv import load_dotenv
from typing import Optional
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

load_dotenv()

class SnowflakeConnection:
    def __init__(self):
        self.connection = None
        self.dynamic_account = None
        self.dynamic_user = None
        self.dynamic_warehouse = None
        self.dynamic_role = None

    def _load_private_key(self, private_key_path: str, passphrase: Optional[str] = None):
        """Load RSA private key from file"""
        with open(private_key_path, 'rb') as key_file:
            private_key = load_pem_private_key(
                key_file.read(),
                password=passphrase.encode() if passphrase else None,
            )
        
        # Convert to DER format for Snowflake
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return private_key_der

    def _get_connection_params(self):
        """Get connection parameters, preferring dynamic values over environment variables"""
        return {
            'account': self.dynamic_account or os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': self.dynamic_user or os.getenv('SNOWFLAKE_USER'),
            'warehouse': self.dynamic_warehouse or os.getenv('SNOWFLAKE_WAREHOUSE') or 'COMPUTE_WH',
            'role': self.dynamic_role or os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
            'database': 'SNOWFLAKE',
            'schema': 'ACCOUNT_USAGE'
        }

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Create a connection to Snowflake using dynamic parameters or environment variables"""
        if self.connection is None:
            params = self._get_connection_params()
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            
            if private_key_path and os.path.exists(private_key_path):
                # Use RSA key authentication
                passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
                private_key_der = self._load_private_key(private_key_path, passphrase)
                
                self.connection = snowflake.connector.connect(
                    account=params['account'],
                    user=params['user'],
                    private_key=private_key_der,
                    warehouse=params['warehouse'],
                    role=params['role'],
                    database=params['database'],
                    schema=params['schema']
                )
            else:
                # Fall back to password authentication
                self.connection = snowflake.connector.connect(
                    account=params['account'],
                    user=params['user'],
                    password=os.getenv('SNOWFLAKE_PASSWORD'),
                    warehouse=params['warehouse'],
                    role=params['role'],
                    database=params['database'],
                    schema=params['schema']
                )
        return self.connection

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=columns)
        finally:
            cursor.close()

    def close(self):
        """Close the connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def set_account_parameters(self, account: str, user: str = None, warehouse: str = None, role: str = None):
        """Set dynamic account parameters and close existing connection"""
        self.dynamic_account = account
        self.dynamic_user = user
        self.dynamic_warehouse = warehouse
        self.dynamic_role = role
        
        # Close existing connection to force reconnection with new parameters
        self.close()
        
        return f"Account parameters updated: {account}"

    def test_connection(self) -> bool:
        """Test the connection to Snowflake and return True if successful"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            # Close the failed connection
            self.close()
            return False

    def get_current_account_info(self) -> str:
        """Get information about the current connection parameters"""
        params = self._get_connection_params()
        
        info = []
        info.append(f"Account: {params['account']}")
        info.append(f"User: {params['user']}")
        info.append(f"Warehouse: {params['warehouse']}")
        info.append(f"Role: {params['role']}")
        info.append(f"Database: {params['database']}")
        info.append(f"Schema: {params['schema']}")
        
        # Check if connection is active
        if self.connection:
            info.append("Status: Connected")
        else:
            info.append("Status: Not connected")
        
        return "\n".join(info)

    def get_credit_price(self) -> float:
        """Get the credit price from environment variables"""
        return float(os.getenv('SNOWFLAKE_CREDIT_PRICE', '4.00'))

# Global connection instance
snowflake_conn = SnowflakeConnection()