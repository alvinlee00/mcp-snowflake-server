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

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Create a connection to Snowflake using environment variables"""
        if self.connection is None:
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            
            if private_key_path and os.path.exists(private_key_path):
                # Use RSA key authentication
                passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
                private_key_der = self._load_private_key(private_key_path, passphrase)
                
                self.connection = snowflake.connector.connect(
                    account=os.getenv('SNOWFLAKE_ACCOUNT'),
                    user=os.getenv('SNOWFLAKE_USER'),
                    private_key=private_key_der,
                    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                    role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
                    database='SNOWFLAKE',
                    schema='ACCOUNT_USAGE'
                )
            else:
                # Fall back to password authentication
                self.connection = snowflake.connector.connect(
                    account=os.getenv('SNOWFLAKE_ACCOUNT'),
                    user=os.getenv('SNOWFLAKE_USER'),
                    password=os.getenv('SNOWFLAKE_PASSWORD'),
                    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                    role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
                    database='SNOWFLAKE',
                    schema='ACCOUNT_USAGE'
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

    def get_credit_price(self) -> float:
        """Get the credit price from environment variables"""
        return float(os.getenv('SNOWFLAKE_CREDIT_PRICE', '4.00'))

# Global connection instance
snowflake_conn = SnowflakeConnection()