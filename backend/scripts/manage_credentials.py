#!/usr/bin/env python3
"""
Credential management script for encrypting/decrypting sensitive environment variables.
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv, set_key

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.encryption import (
    CredentialEncryptor, 
    generate_encryption_key,
    encrypt_env_value,
    decrypt_env_value
)


# List of sensitive variables to encrypt
SENSITIVE_VARS = [
    'SUPABASE_ANON_KEY',
    'SUPABASE_SERVICE_KEY',
    'NEO4J_PASSWORD',
    'OPENAI_API_KEY',
    'GITLAB_ACCESS_TOKEN',
    'SECRET_KEY',
    'API_KEY'
]


class CredentialManager:
    """Manages encryption and decryption of credentials in .env file."""
    
    def __init__(self, env_path: str = '.env'):
        self.env_path = Path(env_path).resolve()
        self.env_dir = self.env_path.parent
        load_dotenv(self.env_path)
    
    def generate_new_key(self) -> str:
        """Generate a new encryption key."""
        key = generate_encryption_key()
        print(f"🔐 Generated new encryption key: {key}")
        print("\n⚠️  IMPORTANT: Save this key securely!")
        print("You'll need it to decrypt your credentials.")
        return key
    
    def encrypt_credentials(self, encryption_key: Optional[str] = None) -> Dict[str, str]:
        """
        Encrypt all sensitive credentials in the .env file.
        
        Args:
            encryption_key: The encryption key to use. If None, generates a new one.
            
        Returns:
            Dictionary of encrypted values
        """
        if not encryption_key:
            encryption_key = self.generate_new_key()
            # Save the key to .env
            set_key(str(self.env_path), 'ENCRYPTION_KEY', encryption_key)
            set_key(str(self.env_path), 'ENCRYPTION_ENABLED', 'true')
        
        encrypted_values = {}
        
        print("\n🔒 Encrypting sensitive credentials...")
        
        for var_name in SENSITIVE_VARS:
            value = os.getenv(var_name)
            
            if value and not value.startswith('ENC:'):  # Skip if already encrypted
                try:
                    # Encrypt the value
                    encrypted = encrypt_env_value(value, encryption_key)
                    encrypted_with_prefix = f"ENC:{encrypted}"
                    
                    # Save encrypted value
                    encrypted_var_name = f"{var_name}_ENCRYPTED"
                    set_key(str(self.env_path), encrypted_var_name, encrypted_with_prefix)
                    
                    # Clear the plain text value
                    set_key(str(self.env_path), var_name, '')
                    
                    encrypted_values[var_name] = encrypted_with_prefix
                    print(f"  ✅ Encrypted {var_name}")
                    
                except Exception as e:
                    print(f"  ❌ Failed to encrypt {var_name}: {e}")
        
        print(f"\n✅ Encrypted {len(encrypted_values)} credentials")
        return encrypted_values
    
    def decrypt_credentials(self, encryption_key: str) -> Dict[str, str]:
        """
        Decrypt all encrypted credentials for viewing.
        
        Args:
            encryption_key: The encryption key to use for decryption
            
        Returns:
            Dictionary of decrypted values
        """
        decrypted_values = {}
        
        print("\n🔓 Decrypting credentials...")
        
        for var_name in SENSITIVE_VARS:
            encrypted_var_name = f"{var_name}_ENCRYPTED"
            encrypted_value = os.getenv(encrypted_var_name)
            
            if encrypted_value and encrypted_value.startswith('ENC:'):
                try:
                    # Remove the ENC: prefix and decrypt
                    encrypted_data = encrypted_value[4:]
                    decrypted = decrypt_env_value(encrypted_data, encryption_key)
                    decrypted_values[var_name] = decrypted
                    print(f"  ✅ Decrypted {var_name}")
                    
                except Exception as e:
                    print(f"  ❌ Failed to decrypt {var_name}: {e}")
        
        return decrypted_values
    
    def test_connection(self, encryption_key: Optional[str] = None):
        """Test database connections with encrypted credentials."""
        import asyncio
        from app.core.config import Settings
        from app.core.database import DatabaseManager
        
        print("\n🧪 Testing database connections...")
        
        # Set encryption key in environment if provided
        if encryption_key:
            os.environ['ENCRYPTION_KEY'] = encryption_key
            os.environ['ENCRYPTION_ENABLED'] = 'true'
        
        async def test():
            # Initialize settings with encryption
            settings = Settings()
            
            # Test database connections
            db_manager = DatabaseManager()
            success = await db_manager.initialize()
            
            if success:
                # Test Supabase
                supabase_health = await db_manager.check_supabase_health()
                print(f"  {'✅' if supabase_health else '❌'} Supabase connection: {'healthy' if supabase_health else 'failed'}")
                
                # Test Neo4j
                neo4j_health = await db_manager.check_neo4j_health()
                print(f"  {'✅' if neo4j_health else '❌'} Neo4j connection: {'healthy' if neo4j_health else 'failed'}")
                
                # Test OpenAI key exists
                openai_configured = bool(settings.openai_api_key)
                print(f"  {'✅' if openai_configured else '❌'} OpenAI API key: {'configured' if openai_configured else 'missing'}")
            
            await db_manager.close()
            return success
        
        return asyncio.run(test())
    
    def rotate_key(self, old_key: str) -> str:
        """
        Rotate the encryption key by decrypting with old key and re-encrypting with new key.
        
        Args:
            old_key: The current encryption key
            
        Returns:
            The new encryption key
        """
        print("\n🔄 Rotating encryption key...")
        
        # First decrypt all values
        decrypted = self.decrypt_credentials(old_key)
        
        # Generate new key
        new_key = generate_encryption_key()
        
        # Re-encrypt with new key
        for var_name, value in decrypted.items():
            encrypted = encrypt_env_value(value, new_key)
            encrypted_with_prefix = f"ENC:{encrypted}"
            encrypted_var_name = f"{var_name}_ENCRYPTED"
            set_key(str(self.env_path), encrypted_var_name, encrypted_with_prefix)
        
        # Update the key in .env
        set_key(str(self.env_path), 'ENCRYPTION_KEY', new_key)
        
        print(f"✅ Key rotation complete. New key: {new_key}")
        return new_key


def main():
    """Main CLI interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage encrypted credentials')
    parser.add_argument('--env', default='.env', help='Path to .env file')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Generate key command
    subparsers.add_parser('generate-key', help='Generate a new encryption key')
    
    # Encrypt command
    encrypt_parser = subparsers.add_parser('encrypt', help='Encrypt credentials')
    encrypt_parser.add_argument('--key', help='Encryption key (generates new if not provided)')
    
    # Decrypt command
    decrypt_parser = subparsers.add_parser('decrypt', help='Decrypt and show credentials')
    decrypt_parser.add_argument('--key', required=True, help='Encryption key')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test database connections')
    test_parser.add_argument('--key', help='Encryption key if using encrypted credentials')
    
    # Rotate key command
    rotate_parser = subparsers.add_parser('rotate', help='Rotate encryption key')
    rotate_parser.add_argument('--old-key', required=True, help='Current encryption key')
    
    args = parser.parse_args()
    
    # Change to backend directory
    backend_dir = Path(__file__).parent.parent
    os.chdir(backend_dir)
    
    manager = CredentialManager(args.env)
    
    if args.command == 'generate-key':
        manager.generate_new_key()
    
    elif args.command == 'encrypt':
        manager.encrypt_credentials(args.key)
    
    elif args.command == 'decrypt':
        decrypted = manager.decrypt_credentials(args.key)
        print("\n📋 Decrypted values:")
        for name, value in decrypted.items():
            # Show partial value for security
            if len(value) > 20:
                masked_value = f"{value[:10]}...{value[-10:]}"
            else:
                masked_value = value[:5] + "..." if len(value) > 5 else "***"
            print(f"  {name}: {masked_value}")
    
    elif args.command == 'test':
        success = manager.test_connection(args.key)
        sys.exit(0 if success else 1)
    
    elif args.command == 'rotate':
        manager.rotate_key(args.old_key)
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()