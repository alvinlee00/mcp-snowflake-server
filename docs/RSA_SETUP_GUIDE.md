# RSA Key Authentication Setup Guide

## Why Use RSA Keys?

✅ **More Secure**: No passwords in config files  
✅ **Recommended by Snowflake**: Industry best practice  
✅ **Better for Production**: Easier credential rotation  
✅ **Audit Friendly**: Key-based authentication is traceable  

## 🔑 Step 1: Generate RSA Key Pair

### Option A: Without Passphrase (Simpler)
```bash
# Generate private key (no passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Option B: With Passphrase (More Secure)
```bash
# Generate private key with passphrase
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8

# You'll be prompted for a passphrase - remember this!

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

## 📋 Step 2: Get Your Public Key Content

```bash
# Display public key content (copy this)
cat rsa_key.pub
```

You'll see something like:
```
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2Z5QG3...very long string...XwIDAQAB
-----END PUBLIC KEY-----
```

**Copy only the content between the BEGIN/END lines** (not including the header/footer).

## 🔧 Step 3: Add Public Key to Snowflake

### Method A: Snowflake Web UI
1. Log into Snowflake Web UI
2. Click your username (top right) → "My Profile"
3. Go to "MFA & Password" tab
4. In "Public Keys" section, click "+"
5. Paste your public key content
6. Click "Add Key"

### Method B: SQL Command
```sql
-- Connect as ACCOUNTADMIN or user admin
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2Z5QG3...XwIDAQAB';
```

## ⚙️ Step 4: Configure Your MCP Server

### For .env file:
```env
SNOWFLAKE_ACCOUNT=abc12345.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_PRIVATE_KEY_PATH=/Users/yourusername/path/to/rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your-passphrase-if-you-used-one
SNOWFLAKE_CREDIT_PRICE=4.00
```

### For Claude Desktop config:
```json
{
  "mcpServers": {
    "snowflake-optimizer": {
      "command": "/Users/80937841/mcp-snowflake-server/venv/bin/python",
      "args": ["/Users/80937841/mcp-snowflake-server/src/server.py"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "abc12345.snowflakecomputing.com",
        "SNOWFLAKE_USER": "your_username",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_ROLE": "ACCOUNTADMIN",
        "SNOWFLAKE_PRIVATE_KEY_PATH": "/Users/yourusername/path/to/rsa_key.p8",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "your-passphrase-if-you-used-one",
        "SNOWFLAKE_CREDIT_PRICE": "4.00"
      }
    }
  }
}
```

## 🧪 Step 5: Test Your Setup

### Test with Python script:
```python
# Save as test_rsa.py
import os
from src.utils.snowflake_connection import snowflake_conn

# Test connection
try:
    df = snowflake_conn.execute_query("SELECT CURRENT_USER(), CURRENT_ROLE()")
    print("✅ RSA Authentication successful!")
    print(f"Connected as: {df.iloc[0, 0]}")
    print(f"Using role: {df.iloc[0, 1]}")
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

### Test from command line:
```bash
cd /Users/80937841/mcp-snowflake-server
source venv/bin/activate
python test_rsa.py
```

## 🔐 Security Best Practices

### File Permissions
```bash
# Secure your private key
chmod 600 rsa_key.p8

# Make sure only you can read it
ls -la rsa_key.p8
# Should show: -rw------- 1 yourusername staff
```

### Key Storage
- Store keys outside your project directory
- Never commit private keys to git
- Use a secure location like `~/.ssh/` or `~/.snowflake/`

### Example secure setup:
```bash
# Create secure directory
mkdir -p ~/.snowflake
chmod 700 ~/.snowflake

# Move keys there
mv rsa_key.p8 ~/.snowflake/
mv rsa_key.pub ~/.snowflake/

# Update your .env
SNOWFLAKE_PRIVATE_KEY_PATH=/Users/yourusername/.snowflake/rsa_key.p8
```

## 🔄 Key Rotation

To rotate keys:
1. Generate new key pair
2. Add new public key to Snowflake user
3. Test with new key
4. Remove old public key
5. Delete old private key

```sql
-- Add new key (you can have multiple)
ALTER USER your_username SET RSA_PUBLIC_KEY_2='new_public_key_content';

-- Remove old key after testing
ALTER USER your_username UNSET RSA_PUBLIC_KEY;

-- Promote key 2 to primary
ALTER USER your_username SET RSA_PUBLIC_KEY = $RSA_PUBLIC_KEY_2;
ALTER USER your_username UNSET RSA_PUBLIC_KEY_2;
```

## 🚨 Troubleshooting

### "Invalid private key"
- Check file path is correct
- Verify passphrase if you used one
- Ensure key is in PKCS#8 format (.p8)

### "User not found" or "Authentication failed"
- Verify public key is added to correct user
- Check user has necessary permissions
- Ensure account URL is correct

### "Permission denied" on key file
```bash
# Fix permissions
chmod 600 /path/to/rsa_key.p8
```

### Test public key in Snowflake:
```sql
-- Check if key is properly set
DESCRIBE USER your_username;
-- Look for RSA_PUBLIC_KEY field
```

## 💡 Pro Tips

1. **Generate keys on your local machine**, not on shared systems
2. **Use different keys** for different environments (dev/prod)
3. **Set up key rotation schedule** (every 90 days)
4. **Monitor key usage** in Snowflake logs
5. **Keep public key backup** in secure password manager

Your RSA authentication is now set up! This is much more secure than password authentication. 🔐✅