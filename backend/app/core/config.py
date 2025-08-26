"""
Configuration management for the Agricultural Data Platform.
Supports encrypted credentials for enhanced security.
"""

from typing import Optional, List
from pydantic_settings import BaseSettings
from pydantic import Field, validator
import os
from pathlib import Path
import sys

# Add parent directory to path for encryption module
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from app.core.encryption import decrypt_env_value
except ImportError:
    decrypt_env_value = None


class Settings(BaseSettings):
    """Application settings loaded from environment variables with encryption support."""
    
    # Encryption Configuration
    encryption_enabled: bool = Field(default=False, env="ENCRYPTION_ENABLED")
    encryption_key: Optional[str] = Field(default=None, env="ENCRYPTION_KEY")
    encryption_method: str = Field(default="AES", env="ENCRYPTION_METHOD")
    mlenc_key: Optional[str] = Field(default=None, env="MLENC_KEY")
    
    # Backend Configuration
    backend_port: int = Field(default=8000, env="BACKEND_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    debug: bool = Field(default=True, env="DEBUG")
    
    # Supabase Configuration
    supabase_url: Optional[str] = Field(default=None, env="SUPABASE_URL")
    supabase_anon_key: Optional[str] = Field(default=None, env="SUPABASE_ANON_KEY")
    supabase_service_key: Optional[str] = Field(default=None, env="SUPABASE_SERVICE_KEY")
    
    # Neo4j Configuration
    neo4j_uri: Optional[str] = Field(default=None, env="NEO4J_URI")
    neo4j_username: Optional[str] = Field(default=None, env="NEO4J_USERNAME")
    neo4j_password: Optional[str] = Field(default=None, env="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="neo4j", env="NEO4J_DATABASE")
    
    # OpenAI Configuration
    openai_api_key: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4-turbo-preview", env="OPENAI_MODEL")
    openai_max_tokens: int = Field(default=3000, env="OPENAI_MAX_TOKENS")
    
    # Frontend Configuration
    frontend_url: str = Field(default="http://localhost:3000", env="FRONTEND_URL")
    
    # API Configuration
    api_rate_limit: int = Field(default=60, env="API_RATE_LIMIT")
    max_query_length: int = Field(default=500, env="MAX_QUERY_LENGTH")
    max_results: int = Field(default=50, env="MAX_RESULTS")
    query_timeout: int = Field(default=5, env="QUERY_TIMEOUT")
    
    # Caching Configuration
    cache_ttl: int = Field(default=300, env="CACHE_TTL")
    enable_cache: bool = Field(default=True, env="ENABLE_CACHE")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(default="json", env="LOG_FORMAT")
    
    # GitLab Configuration
    gitlab_repo_url: str = Field(
        default="https://gitlab.com/geojensen/0ntology.com", 
        env="GITLAB_REPO_URL"
    )
    gitlab_access_token: Optional[str] = Field(default=None, env="GITLAB_ACCESS_TOKEN")
    gitlab_runner_tags: str = Field(default="linux", env="GITLAB_RUNNER_TAGS")
    
    # Data Loading Configuration
    sample_farms_count: int = Field(default=1000, env="SAMPLE_FARMS_COUNT")
    sample_suppliers_count: int = Field(default=50, env="SAMPLE_SUPPLIERS_COUNT")
    equipment_per_farm: int = Field(default=5, env="EQUIPMENT_PER_FARM")
    auto_seed_data: bool = Field(default=False, env="AUTO_SEED_DATA")
    
    # Security Configuration
    secret_key: Optional[str] = Field(default=None, env="SECRET_KEY")
    cors_origins: str = Field(
        default="http://localhost:3000,http://localhost:8000",
        env="CORS_ORIGINS"
    )
    enable_api_auth: bool = Field(default=False, env="ENABLE_API_AUTH")
    api_key: Optional[str] = Field(default=None, env="API_KEY")
    
    @validator("encryption_enabled", pre=True)
    def parse_encryption_enabled(cls, v):
        if isinstance(v, str):
            return v.lower().strip("'\"") == "true"
        return v
    
    @validator("encryption_key", pre=True)
    def parse_encryption_key(cls, v):
        if isinstance(v, str):
            return v.strip("'\"")
        return v
    
    
    @validator("gitlab_runner_tags", pre=True)
    def parse_runner_tags(cls, v):
        if isinstance(v, str):
            return v
        return ",".join(v)
    
    @validator("secret_key", pre=True, always=True)
    def generate_secret_key(cls, v):
        if not v:
            import secrets
            return secrets.token_hex(32)
        return v
    
    class Config:
        env_file = "../.env"  # Pydantic loads relative to working directory (backend/)
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    def __init__(self, **kwargs):
        """Initialize settings with support for encrypted values."""
        # CRITICAL: Clear ALL potentially cached environment variables
        # This prevents Claude Code and system-level env pollution
        import os
        problematic_vars = [
            'OPENAI_API_KEY', 'OPENAI_MODEL', 'OPENAI_MAX_TOKENS',
            'SUPABASE_URL', 'SUPABASE_ANON_KEY', 'SUPABASE_SERVICE_KEY',
            'NEO4J_URI', 'NEO4J_USERNAME', 'NEO4J_PASSWORD'
        ]
        
        for var in problematic_vars:
            if var in os.environ:
                print(f"WARNING: Clearing cached {var} from environment")
                del os.environ[var]
        
        # Force correct max_tokens
        os.environ['OPENAI_MAX_TOKENS'] = '3000'
        
        super().__init__(**kwargs)
        
        # Force max_tokens after loading
        self.openai_max_tokens = 3000
        
        # Decrypt credentials based on encryption method
        if self.encryption_method == "MLENC" and self.mlenc_key:
            self._decrypt_mlenc_credentials()
        elif self.encryption_enabled and self.encryption_key and decrypt_env_value:
            self._decrypt_credentials()
    
    def _decrypt_mlenc_credentials(self):
        """Decrypt MLENC-encrypted credentials."""
        try:
            from app.core.mlenc import mlenc_decrypt
        except ImportError:
            print("Warning: MLENC module not available")
            return
        
        # List of potentially encrypted fields
        encrypted_fields = [
            'supabase_anon_key',
            'supabase_service_key',
            'neo4j_password',
            'openai_api_key',
            'gitlab_access_token',
            'secret_key',
            'api_key'
        ]
        
        for field_name in encrypted_fields:
            value = getattr(self, field_name, None)
            if value and isinstance(value, str) and value.startswith('MLENC:'):
                try:
                    decrypted = mlenc_decrypt(value, self.mlenc_key)
                    setattr(self, field_name, decrypted)
                except Exception as e:
                    print(f"Warning: Failed to decrypt {field_name}: {e}")
    
    def _decrypt_credential(self, encrypted_name: str, target_attr: str):
        """Decrypt a single credential if it exists in encrypted form."""
        encrypted_value = os.getenv(encrypted_name)
        if encrypted_value and encrypted_value.startswith('ENC:'):
            try:
                # Remove ENC: prefix and decrypt
                encrypted_data = encrypted_value[4:]
                decrypted = decrypt_env_value(encrypted_data, self.encryption_key)
                setattr(self, target_attr, decrypted)
            except Exception as e:
                print(f"Warning: Failed to decrypt {encrypted_name}: {e}")
    
    def _decrypt_credentials(self):
        """Decrypt all encrypted credentials (old method)."""
        # Check if values are directly encrypted
        encrypted_fields = [
            'supabase_anon_key',
            'supabase_service_key',
            'neo4j_password',
            'openai_api_key',
            'gitlab_access_token',
            'secret_key',
            'api_key'
        ]
        
        for field_name in encrypted_fields:
            value = getattr(self, field_name, None)
            if value and isinstance(value, str) and value.startswith('ENC:'):
                try:
                    encrypted_data = value[4:]
                    decrypted = decrypt_env_value(encrypted_data, self.encryption_key)
                    setattr(self, field_name, decrypted)
                except Exception as e:
                    print(f"Warning: Failed to decrypt {field_name}: {e}")
        
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment.lower() == "development"
    
    def validate_database_config(self) -> bool:
        """Validate that required database configurations are present."""
        supabase_configured = bool(self.supabase_url and self.supabase_anon_key)
        neo4j_configured = bool(
            self.neo4j_uri and self.neo4j_username and self.neo4j_password
        )
        return supabase_configured and neo4j_configured
    
    def validate_ai_config(self) -> bool:
        """Validate that AI service configuration is present."""
        return bool(self.openai_api_key)


# Create global settings instance
settings = Settings()


# Validate configuration on startup
def validate_configuration():
    """Validate that all required configuration is present."""
    errors = []
    
    if not settings.validate_database_config():
        if not settings.supabase_url:
            errors.append("SUPABASE_URL is not configured")
        if not settings.supabase_anon_key:
            errors.append("SUPABASE_ANON_KEY is not configured")
        if not settings.neo4j_uri:
            errors.append("NEO4J_URI is not configured")
        if not settings.neo4j_username:
            errors.append("NEO4J_USERNAME is not configured")
        if not settings.neo4j_password:
            errors.append("NEO4J_PASSWORD is not configured")
    
    if not settings.validate_ai_config():
        errors.append("OPENAI_API_KEY is not configured")
    
    if errors and settings.is_production:
        error_message = "Configuration errors found:\n" + "\n".join(errors)
        raise ValueError(error_message)
    elif errors:
        print("Warning: Configuration issues found (ignored in development):")
        for error in errors:
            print(f"  - {error}")
    
    return len(errors) == 0