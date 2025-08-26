"""
Encryption utilities for securing sensitive credentials.
Uses AES-256-GCM for encryption with PBKDF2 key derivation.
"""

import os
import base64
import json
from typing import Optional, Dict, Any
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidTag
import secrets


class CredentialEncryptor:
    """Handles encryption and decryption of sensitive credentials."""
    
    def __init__(self, master_key: Optional[str] = None):
        """
        Initialize the encryptor with a master key.
        
        Args:
            master_key: Base64-encoded master key. If None, generates a new one.
        """
        if master_key:
            self.master_key = base64.b64decode(master_key)
        else:
            self.master_key = self._generate_master_key()
    
    def _generate_master_key(self) -> bytes:
        """Generate a new 256-bit master key."""
        return secrets.token_bytes(32)
    
    def get_master_key_b64(self) -> str:
        """Get the master key as a base64-encoded string."""
        return base64.b64encode(self.master_key).decode('utf-8')
    
    def _derive_key(self, salt: bytes, iterations: int = 100000) -> bytes:
        """
        Derive an encryption key from the master key using PBKDF2.
        
        Args:
            salt: Salt for key derivation
            iterations: Number of iterations for PBKDF2
            
        Returns:
            32-byte derived key
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=iterations,
            backend=default_backend()
        )
        return kdf.derive(self.master_key)
    
    def encrypt(self, plaintext: str) -> Dict[str, str]:
        """
        Encrypt a plaintext string using AES-256-GCM.
        
        Args:
            plaintext: The string to encrypt
            
        Returns:
            Dictionary containing encrypted data, salt, nonce, and tag
        """
        # Generate random salt and nonce
        salt = secrets.token_bytes(16)
        nonce = secrets.token_bytes(12)
        
        # Derive key from master key
        key = self._derive_key(salt)
        
        # Create cipher
        cipher = Cipher(
            algorithms.AES(key),
            modes.GCM(nonce),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        
        # Encrypt the data
        ciphertext = encryptor.update(plaintext.encode('utf-8')) + encryptor.finalize()
        
        # Return encrypted data with metadata
        return {
            'ciphertext': base64.b64encode(ciphertext).decode('utf-8'),
            'salt': base64.b64encode(salt).decode('utf-8'),
            'nonce': base64.b64encode(nonce).decode('utf-8'),
            'tag': base64.b64encode(encryptor.tag).decode('utf-8')
        }
    
    def decrypt(self, encrypted_data: Dict[str, str]) -> str:
        """
        Decrypt data encrypted with encrypt().
        
        Args:
            encrypted_data: Dictionary containing ciphertext, salt, nonce, and tag
            
        Returns:
            Decrypted plaintext string
            
        Raises:
            InvalidTag: If the authentication tag is invalid (data tampered)
            ValueError: If the encrypted data is malformed
        """
        try:
            # Decode from base64
            ciphertext = base64.b64decode(encrypted_data['ciphertext'])
            salt = base64.b64decode(encrypted_data['salt'])
            nonce = base64.b64decode(encrypted_data['nonce'])
            tag = base64.b64decode(encrypted_data['tag'])
            
            # Derive the same key
            key = self._derive_key(salt)
            
            # Create cipher for decryption
            cipher = Cipher(
                algorithms.AES(key),
                modes.GCM(nonce, tag),
                backend=default_backend()
            )
            decryptor = cipher.decryptor()
            
            # Decrypt the data
            plaintext = decryptor.update(ciphertext) + decryptor.finalize()
            
            return plaintext.decode('utf-8')
            
        except InvalidTag:
            raise ValueError("Invalid encryption tag - data may have been tampered with")
        except Exception as e:
            raise ValueError(f"Failed to decrypt data: {str(e)}")
    
    def encrypt_credential(self, credential: str) -> str:
        """
        Encrypt a credential and return it as a single base64 string.
        
        Args:
            credential: The credential to encrypt
            
        Returns:
            Base64-encoded string containing all encrypted data
        """
        encrypted_data = self.encrypt(credential)
        # Combine all parts into a single JSON string
        combined = json.dumps(encrypted_data)
        # Encode the JSON as base64 for storage
        return base64.b64encode(combined.encode('utf-8')).decode('utf-8')
    
    def decrypt_credential(self, encrypted_credential: str) -> str:
        """
        Decrypt a credential from a single base64 string.
        
        Args:
            encrypted_credential: Base64-encoded encrypted credential
            
        Returns:
            Decrypted credential string
        """
        try:
            # Decode from base64
            combined = base64.b64decode(encrypted_credential).decode('utf-8')
            # Parse JSON
            encrypted_data = json.loads(combined)
            # Decrypt
            return self.decrypt(encrypted_data)
        except Exception as e:
            raise ValueError(f"Failed to decrypt credential: {str(e)}")


def generate_encryption_key() -> str:
    """Generate a new encryption key for the .env file."""
    encryptor = CredentialEncryptor()
    return encryptor.get_master_key_b64()


def encrypt_env_value(value: str, key: str) -> str:
    """
    Encrypt a single environment variable value.
    
    Args:
        value: The value to encrypt
        key: The encryption key (base64-encoded)
        
    Returns:
        Encrypted value as a base64 string
    """
    encryptor = CredentialEncryptor(key)
    return encryptor.encrypt_credential(value)


def decrypt_env_value(encrypted_value: str, key: str) -> str:
    """
    Decrypt a single environment variable value.
    
    Args:
        encrypted_value: The encrypted value (base64 string)
        key: The encryption key (base64-encoded)
        
    Returns:
        Decrypted value
    """
    encryptor = CredentialEncryptor(key)
    return encryptor.decrypt_credential(encrypted_value)


# Helper functions for testing
def test_encryption():
    """Test the encryption/decryption functionality."""
    # Generate a key
    key = generate_encryption_key()
    print(f"Generated encryption key: {key}")
    
    # Test data
    test_credential = "sk-test123456789abcdef"
    
    # Encrypt
    encrypted = encrypt_env_value(test_credential, key)
    print(f"Encrypted credential: {encrypted[:50]}...")
    
    # Decrypt
    decrypted = decrypt_env_value(encrypted, key)
    print(f"Decrypted credential: {decrypted}")
    
    # Verify
    assert decrypted == test_credential, "Decryption failed!"
    print("✅ Encryption test passed!")
    
    return True


if __name__ == "__main__":
    test_encryption()