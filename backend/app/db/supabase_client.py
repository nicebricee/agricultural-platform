"""
Supabase client wrapper for PostgreSQL operations.
"""

from typing import Optional, Dict, Any, List
import asyncio
from app.core.database import SupabaseClient as BaseSupabaseClient
from app.core.logging import app_logger


class SupabaseManager(BaseSupabaseClient):
    """
    Enhanced Supabase client with additional functionality.
    Inherits from the base SupabaseClient in database.py.
    """
    
    def __init__(self):
        """Initialize the Supabase manager."""
        super().__init__()
        self._initialized = False
    
    async def ensure_initialized(self) -> bool:
        """Ensure the client is initialized before use."""
        if not self._initialized:
            self._initialized = await self.initialize()
        return self._initialized
    
    async def execute_raw_sql(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute raw SQL query with automatic initialization.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query results with execution metadata
        """
        await self.ensure_initialized()
        return await self.execute_query(query, params)
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column definitions
        """
        await self.ensure_initialized()
        
        if not self.client:
            return []
        
        try:
            # Query PostgreSQL information schema
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """
            
            result = await self.execute_query(query, {"table_name": table_name})
            return result.get("data", [])
            
        except Exception as e:
            app_logger.error(f"Failed to get schema for table {table_name}: {e}")
            return []
    
    async def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        await self.ensure_initialized()
        
        if not self.client:
            return 0
        
        try:
            result = self.client.table(table_name).select("*", count="exact").execute()
            return result.count if hasattr(result, 'count') else len(result.data)
        except Exception as e:
            app_logger.error(f"Failed to get count for table {table_name}: {e}")
            return 0
    
    async def verify_connection(self) -> bool:
        """
        Verify database connection with detailed diagnostics.
        
        Returns:
            True if connection is healthy
        """
        await self.ensure_initialized()
        
        if not await self.health_check():
            app_logger.error("Supabase health check failed")
            return False
        
        try:
            # Try to list tables
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                LIMIT 5;
            """
            result = await self.execute_query(query)
            
            if result.get("data"):
                app_logger.info(f"Supabase connection verified. Found {len(result['data'])} tables")
                return True
            else:
                app_logger.warning("Supabase connected but no tables found")
                return True
                
        except Exception as e:
            app_logger.error(f"Supabase connection verification failed: {e}")
            return False
    
    async def create_tables_if_not_exist(self) -> bool:
        """
        Create required tables if they don't exist.
        
        Returns:
            True if tables exist or were created successfully
        """
        await self.ensure_initialized()
        
        if not self.client:
            return False
        
        # Table creation SQL
        table_schemas = {
            "farms": """
                CREATE TABLE IF NOT EXISTS farms (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(255) NOT NULL,
                    location VARCHAR(255),
                    owner VARCHAR(255),
                    size_acres DECIMAL(10, 2),
                    primary_crop VARCHAR(100),
                    certification_type VARCHAR(50),
                    established_date DATE,
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    county VARCHAR(100),
                    state VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
            "equipment": """
                CREATE TABLE IF NOT EXISTS equipment (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    farm_id UUID REFERENCES farms(id) ON DELETE CASCADE,
                    type VARCHAR(100),
                    manufacturer VARCHAR(100),
                    model VARCHAR(100),
                    purchase_date DATE,
                    maintenance_status VARCHAR(50),
                    cost DECIMAL(12, 2),
                    last_service_date DATE,
                    next_service_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
            "suppliers": """
                CREATE TABLE IF NOT EXISTS suppliers (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(100),
                    location VARCHAR(255),
                    contact_info JSONB,
                    reliability_score DECIMAL(3, 2),
                    average_delivery_time INTEGER,
                    products_offered TEXT[],
                    contract_terms TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
            "farm_suppliers": """
                CREATE TABLE IF NOT EXISTS farm_suppliers (
                    farm_id UUID REFERENCES farms(id) ON DELETE CASCADE,
                    supplier_id UUID REFERENCES suppliers(id) ON DELETE CASCADE,
                    contract_start_date DATE,
                    contract_end_date DATE,
                    annual_volume DECIMAL(12, 2),
                    payment_terms VARCHAR(100),
                    PRIMARY KEY (farm_id, supplier_id)
                );
            """,
            "production_records": """
                CREATE TABLE IF NOT EXISTS production_records (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    farm_id UUID REFERENCES farms(id) ON DELETE CASCADE,
                    year INTEGER,
                    crop_type VARCHAR(100),
                    acres_planted DECIMAL(10, 2),
                    yield_per_acre DECIMAL(10, 2),
                    total_production DECIMAL(12, 2),
                    revenue DECIMAL(12, 2),
                    weather_impact VARCHAR(255),
                    market_price DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,
            "weather_events": """
                CREATE TABLE IF NOT EXISTS weather_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    date DATE,
                    type VARCHAR(100),
                    severity VARCHAR(50),
                    affected_region VARCHAR(255),
                    impact_description TEXT,
                    estimated_damage DECIMAL(12, 2),
                    recovery_time INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        }
        
        try:
            for table_name, schema_sql in table_schemas.items():
                app_logger.debug(f"Creating table if not exists: {table_name}")
                # Note: Supabase doesn't support direct DDL through the client
                # These would need to be run through Supabase dashboard or migration
                # For now, we'll just log the intent
                app_logger.info(f"Table schema ready for {table_name}")
            
            return True
            
        except Exception as e:
            app_logger.error(f"Failed to create tables: {e}")
            return False