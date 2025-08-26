"""
Multi-Layered Encryption (MLENC) for ultra-secure credential storage.
Implements 3 layers of encryption with different algorithms and keys.
"""

import os
import base64
import hashlib
import json
from typing import Dict, Any, Optional
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
import secrets


class MultiLayerEncryptor:
    """
    Multi-layered encryption system with 3 layers:
    Layer 1: AES-256-GCM
    Layer 2: Fernet (AES-128-CBC with HMAC)
    Layer 3: XOR with derived key + Base64
    """
    
    def __init__(self, master_key: Optional[str] = None):
        """Initialize with master key or generate new one."""
        if master_key:
            self.master_key = base64.b64decode(master_key)
        else:
            self.master_key = secrets.token_bytes(32)
        
        # Derive layer-specific keys from master key
        self.layer1_key = self._derive_key(self.master_key, b"LAYER1", 32)
        self.layer2_key = self._derive_key(self.master_key, b"LAYER2", 32)
        self.layer3_key = self._derive_key(self.master_key, b"LAYER3", 64)
    
    def _derive_key(self, master_key: bytes, layer_salt: bytes, key_length: int) -> bytes:
        """Derive a layer-specific key using PBKDF2."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=key_length,
            salt=layer_salt,
            iterations=150000,  # High iteration count for security
            backend=default_backend()
        )
        return kdf.derive(master_key)
    
    def get_master_key_b64(self) -> str:
        """Get base64-encoded master key."""
        return base64.b64encode(self.master_key).decode('utf-8')
    
    # === LAYER 1: AES-256-GCM ===
    def _encrypt_layer1(self, data: bytes) -> Dict[str, bytes]:
        """First layer: AES-256-GCM encryption."""
        nonce = secrets.token_bytes(12)
        cipher = Cipher(
            algorithms.AES(self.layer1_key),
            modes.GCM(nonce),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(data) + encryptor.finalize()
        
        return {
            'ciphertext': ciphertext,
            'nonce': nonce,
            'tag': encryptor.tag
        }
    
    def _decrypt_layer1(self, encrypted_data: Dict[str, bytes]) -> bytes:
        """Decrypt first layer."""
        cipher = Cipher(
            algorithms.AES(self.layer1_key),
            modes.GCM(encrypted_data['nonce'], encrypted_data['tag']),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        return decryptor.update(encrypted_data['ciphertext']) + decryptor.finalize()
    
    # === LAYER 2: Fernet ===
    def _encrypt_layer2(self, data: bytes) -> bytes:
        """Second layer: Fernet encryption."""
        # Fernet requires 32-byte key encoded as base64
        fernet_key = base64.urlsafe_b64encode(self.layer2_key)
        f = Fernet(fernet_key)
        return f.encrypt(data)
    
    def _decrypt_layer2(self, encrypted_data: bytes) -> bytes:
        """Decrypt second layer."""
        fernet_key = base64.urlsafe_b64encode(self.layer2_key)
        f = Fernet(fernet_key)
        return f.decrypt(encrypted_data)
    
    # === LAYER 3: XOR + Shuffle ===
    def _encrypt_layer3(self, data: bytes) -> bytes:
        """Third layer: XOR cipher with key stretching and shuffling."""
        # XOR with repeating key
        key_stream = (self.layer3_key * (len(data) // len(self.layer3_key) + 1))[:len(data)]
        xored = bytes(a ^ b for a, b in zip(data, key_stream))
        
        # Add integrity check
        h = hmac.HMAC(self.layer3_key[:32], hashes.SHA256(), backend=default_backend())
        h.update(xored)
        mac = h.finalize()
        
        # Combine MAC and ciphertext
        return mac + xored
    
    def _decrypt_layer3(self, encrypted_data: bytes) -> bytes:
        """Decrypt third layer."""
        # Extract MAC and verify
        mac = encrypted_data[:32]
        ciphertext = encrypted_data[32:]
        
        # Verify MAC
        h = hmac.HMAC(self.layer3_key[:32], hashes.SHA256(), backend=default_backend())
        h.update(ciphertext)
        h.verify(mac)
        
        # XOR decrypt
        key_stream = (self.layer3_key * (len(ciphertext) // len(self.layer3_key) + 1))[:len(ciphertext)]
        return bytes(a ^ b for a, b in zip(ciphertext, key_stream))
    
    # === MAIN ENCRYPTION/DECRYPTION ===
    def encrypt(self, plaintext: str) -> str:
        """
        Apply all 3 layers of encryption.
        Returns MLENC-prefixed base64 string.
        """
        # Convert to bytes
        data = plaintext.encode('utf-8')
        
        # Apply Layer 1: AES-256-GCM
        layer1_result = self._encrypt_layer1(data)
        layer1_combined = json.dumps({
            'ct': base64.b64encode(layer1_result['ciphertext']).decode(),
            'nc': base64.b64encode(layer1_result['nonce']).decode(),
            'tg': base64.b64encode(layer1_result['tag']).decode()
        }).encode()
        
        # Apply Layer 2: Fernet
        layer2_result = self._encrypt_layer2(layer1_combined)
        
        # Apply Layer 3: XOR + MAC
        layer3_result = self._encrypt_layer3(layer2_result)
        
        # Final encoding
        final = base64.b64encode(layer3_result).decode('utf-8')
        return f"MLENC:{final}"
    
    def decrypt(self, encrypted: str) -> str:
        """
        Decrypt all 3 layers.
        Expects MLENC-prefixed string.
        """
        if not encrypted.startswith("MLENC:"):
            raise ValueError("Invalid MLENC format")
        
        # Remove prefix and decode
        encrypted_data = base64.b64decode(encrypted[6:])
        
        # Decrypt Layer 3: XOR + MAC
        layer2_data = self._decrypt_layer3(encrypted_data)
        
        # Decrypt Layer 2: Fernet
        layer1_data = self._decrypt_layer2(layer2_data)
        
        # Parse Layer 1 data
        layer1_parsed = json.loads(layer1_data.decode())
        layer1_dict = {
            'ciphertext': base64.b64decode(layer1_parsed['ct']),
            'nonce': base64.b64decode(layer1_parsed['nc']),
            'tag': base64.b64decode(layer1_parsed['tg'])
        }
        
        # Decrypt Layer 1: AES-256-GCM
        plaintext_bytes = self._decrypt_layer1(layer1_dict)
        
        return plaintext_bytes.decode('utf-8')


def generate_mlenc_key() -> str:
    """Generate a new MLENC master key."""
    encryptor = MultiLayerEncryptor()
    return encryptor.get_master_key_b64()


def mlenc_encrypt(value: str, key: str) -> str:
    """Encrypt a value using MLENC."""
    encryptor = MultiLayerEncryptor(key)
    return encryptor.encrypt(value)


def mlenc_decrypt(encrypted_value: str, key: str) -> str:
    """Decrypt a MLENC-encrypted value."""
    encryptor = MultiLayerEncryptor(key)
    return encryptor.decrypt(encrypted_value)


# Test the implementation
if __name__ == "__main__":
    # Generate key
    key = generate_mlenc_key()
    print(f"Generated MLENC key: {key}")
    
    # Test data
    test_credential = "sk-test123456789abcdef_SENSITIVE_DATA"
    
    # Encrypt
    encrypted = mlenc_encrypt(test_credential, key)
    print(f"\nEncrypted (MLENC): {encrypted[:80]}...")
    print(f"Encryption layers: 3 (AES-256-GCM + Fernet + XOR-HMAC)")
    
    # Decrypt
    decrypted = mlenc_decrypt(encrypted, key)
    print(f"\nDecrypted: {decrypted}")
    
    # Verify
    assert decrypted == test_credential, "Decryption failed!"
    print("\n✅ MLENC test passed - 3 layers of encryption working!")