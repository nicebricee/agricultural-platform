#!/usr/bin/env python3
"""
Quick setup script to encrypt credentials and test connections.
Run this after filling in your .env file with plain text credentials.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

def main():
    print("🔐 Agricultural Data Platform - Credential Encryption Setup")
    print("=" * 60)
    
    # Change to backend directory
    backend_dir = Path(__file__).parent.parent
    os.chdir(backend_dir)
    
    from scripts.manage_credentials import CredentialManager
    
    manager = CredentialManager('../.env')
    
    print("\n1️⃣  Generating encryption key and encrypting credentials...")
    encryption_key = manager.generate_new_key()
    
    print("\n2️⃣  Encrypting your sensitive credentials...")
    encrypted = manager.encrypt_credentials(encryption_key)
    
    print("\n3️⃣  Testing connections with encrypted credentials...")
    success = manager.test_connection(encryption_key)
    
    if success:
        print("\n✅ Setup complete! Your credentials are now encrypted.")
        print(f"\n🔑 Your encryption key: {encryption_key}")
        print("\n⚠️  IMPORTANT SECURITY NOTES:")
        print("   1. Save the encryption key above in a secure location")
        print("   2. Do NOT commit the .env file to git")
        print("   3. The encryption key is required to run the application")
        print("\n📝 To start the application:")
        print("   cd backend")
        print("   python -m app.main")
    else:
        print("\n⚠️  Some connections failed. Please check your credentials.")
        print("   You can re-run this script after fixing the credentials.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())