"""
Neo4j client wrapper for graph database operations.
"""

from typing import Optional, Dict, Any, List
import asyncio
from app.core.database import Neo4jClient as BaseNeo4jClient
from app.core.logging import app_logger


class Neo4jManager(BaseNeo4jClient):
    """
    Enhanced Neo4j client with additional functionality.
    Inherits from the base Neo4jClient in database.py.
    """
    
    def __init__(self):
        """Initialize the Neo4j manager."""
        super().__init__()
        self._initialized = False
    
    async def ensure_initialized(self) -> bool:
        """Ensure the driver is initialized before use."""
        if not self._initialized:
            self._initialized = await self.initialize()
        return self._initialized
    
    async def execute_cypher(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute Cypher query with automatic initialization.
        
        Args:
            query: Cypher query string
            params: Query parameters
            
        Returns:
            Query results with execution metadata
        """
        await self.ensure_initialized()
        return await self.execute_query(query, params)
    
    async def get_node_count(self, label: str = None) -> int:
        """
        Get count of nodes with optional label filter.
        
        Args:
            label: Optional node label to filter by
            
        Returns:
            Number of nodes
        """
        await self.ensure_initialized()
        
        if not self.driver:
            return 0
        
        try:
            if label:
                query = f"MATCH (n:{label}) RETURN count(n) as count"
            else:
                query = "MATCH (n) RETURN count(n) as count"
            
            result = await self.execute_query(query)
            if result["data"] and len(result["data"]) > 0:
                return result["data"][0].get("count", 0)
            return 0
            
        except Exception as e:
            app_logger.error(f"Failed to get node count: {e}")
            return 0
    
    async def get_relationship_count(self, rel_type: str = None) -> int:
        """
        Get count of relationships with optional type filter.
        
        Args:
            rel_type: Optional relationship type to filter by
            
        Returns:
            Number of relationships
        """
        await self.ensure_initialized()
        
        if not self.driver:
            return 0
        
        try:
            if rel_type:
                query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
            else:
                query = "MATCH ()-[r]->() RETURN count(r) as count"
            
            result = await self.execute_query(query)
            if result["data"] and len(result["data"]) > 0:
                return result["data"][0].get("count", 0)
            return 0
            
        except Exception as e:
            app_logger.error(f"Failed to get relationship count: {e}")
            return 0
    
    async def get_schema(self) -> Dict[str, Any]:
        """
        Get database schema information.
        
        Returns:
            Schema information including node labels and relationship types
        """
        await self.ensure_initialized()
        
        if not self.driver:
            return {}
        
        try:
            schema = {
                "node_labels": [],
                "relationship_types": [],
                "property_keys": [],
                "constraints": [],
                "indexes": []
            }
            
            # Get node labels
            labels_query = "CALL db.labels() YIELD label RETURN collect(label) as labels"
            labels_result = await self.execute_query(labels_query)
            if labels_result["data"]:
                schema["node_labels"] = labels_result["data"][0].get("labels", [])
            
            # Get relationship types
            rels_query = "CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) as types"
            rels_result = await self.execute_query(rels_query)
            if rels_result["data"]:
                schema["relationship_types"] = rels_result["data"][0].get("types", [])
            
            # Get property keys
            props_query = "CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) as keys"
            props_result = await self.execute_query(props_query)
            if props_result["data"]:
                schema["property_keys"] = props_result["data"][0].get("keys", [])
            
            return schema
            
        except Exception as e:
            app_logger.error(f"Failed to get schema: {e}")
            return {}
    
    async def verify_connection(self) -> bool:
        """
        Verify database connection with detailed diagnostics.
        
        Returns:
            True if connection is healthy
        """
        await self.ensure_initialized()
        
        if not await self.health_check():
            app_logger.error("Neo4j health check failed")
            return False
        
        try:
            # Get database info
            query = """
                CALL dbms.components() 
                YIELD name, versions, edition 
                RETURN name, versions[0] as version, edition
            """
            result = await self.execute_query(query)
            
            if result.get("data"):
                info = result["data"][0]
                app_logger.info(
                    f"Neo4j connection verified. "
                    f"Version: {info.get('version', 'unknown')}, "
                    f"Edition: {info.get('edition', 'unknown')}"
                )
                return True
            else:
                app_logger.warning("Neo4j connected but couldn't retrieve version info")
                return True
                
        except Exception as e:
            app_logger.error(f"Neo4j connection verification failed: {e}")
            return False
    
    async def create_constraints_and_indexes(self) -> bool:
        """
        Create constraints and indexes for optimal performance.
        
        Returns:
            True if constraints and indexes were created successfully
        """
        await self.ensure_initialized()
        
        if not self.driver:
            return False
        
        constraints_and_indexes = [
            # Unique constraints
            "CREATE CONSTRAINT farm_id_unique IF NOT EXISTS FOR (f:Farm) REQUIRE f.id IS UNIQUE",
            "CREATE CONSTRAINT supplier_id_unique IF NOT EXISTS FOR (s:Supplier) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT equipment_id_unique IF NOT EXISTS FOR (e:Equipment) REQUIRE e.id IS UNIQUE",
            
            # Indexes for common queries
            "CREATE INDEX farm_name_index IF NOT EXISTS FOR (f:Farm) ON (f.name)",
            "CREATE INDEX farm_location_index IF NOT EXISTS FOR (f:Farm) ON (f.location)",
            "CREATE INDEX farm_crop_index IF NOT EXISTS FOR (f:Farm) ON (f.primary_crop)",
            "CREATE INDEX supplier_name_index IF NOT EXISTS FOR (s:Supplier) ON (s.name)",
            "CREATE INDEX equipment_type_index IF NOT EXISTS FOR (e:Equipment) ON (e.type)",
            "CREATE INDEX weather_date_index IF NOT EXISTS FOR (w:WeatherEvent) ON (w.date)",
            
            # Composite indexes
            "CREATE INDEX farm_state_county_index IF NOT EXISTS FOR (f:Farm) ON (f.state, f.county)",
        ]
        
        try:
            for constraint_or_index in constraints_and_indexes:
                try:
                    app_logger.debug(f"Creating: {constraint_or_index[:50]}...")
                    await self.execute_query(constraint_or_index)
                except Exception as e:
                    # Some constraints might already exist
                    app_logger.debug(f"Constraint/index creation note: {e}")
            
            app_logger.info("Neo4j constraints and indexes configured")
            return True
            
        except Exception as e:
            app_logger.error(f"Failed to create constraints and indexes: {e}")
            return False
    
    async def clear_database(self) -> bool:
        """
        Clear all nodes and relationships from the database.
        WARNING: This will delete all data!
        
        Returns:
            True if database was cleared successfully
        """
        await self.ensure_initialized()
        
        if not self.driver:
            return False
        
        try:
            # Delete all relationships first, then nodes
            app_logger.warning("Clearing all data from Neo4j database...")
            
            # Delete in batches to avoid memory issues
            batch_query = """
                MATCH (n)
                WITH n LIMIT 10000
                DETACH DELETE n
                RETURN count(n) as deleted
            """
            
            total_deleted = 0
            while True:
                result = await self.execute_query(batch_query)
                if result["data"] and len(result["data"]) > 0:
                    deleted = result["data"][0].get("deleted", 0)
                    total_deleted += deleted
                    if deleted == 0:
                        break
                else:
                    break
            
            app_logger.info(f"Cleared {total_deleted} nodes from Neo4j database")
            return True
            
        except Exception as e:
            app_logger.error(f"Failed to clear database: {e}")
            return False
    
    async def get_graph_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the graph database.
        
        Returns:
            Dictionary with various statistics
        """
        await self.ensure_initialized()
        
        if not self.driver:
            return {}
        
        try:
            stats = {}
            
            # Get node counts by label
            node_labels_query = """
                CALL db.labels() YIELD label
                CALL {
                    WITH label
                    RETURN label, size([(n) WHERE label IN labels(n) | n]) AS count
                }
                RETURN label, count
                ORDER BY count DESC
            """
            
            node_stats = await self.execute_query(node_labels_query)
            stats["nodes_by_label"] = {
                record["label"]: record["count"] 
                for record in node_stats.get("data", [])
            }
            
            # Get relationship counts by type
            rel_types_query = """
                CALL db.relationshipTypes() YIELD relationshipType
                CALL {
                    WITH relationshipType
                    MATCH ()-[r]->() WHERE type(r) = relationshipType
                    RETURN relationshipType, count(r) AS count
                }
                RETURN relationshipType, count
                ORDER BY count DESC
            """
            
            rel_stats = await self.execute_query(rel_types_query)
            stats["relationships_by_type"] = {
                record["relationshipType"]: record["count"] 
                for record in rel_stats.get("data", [])
            }
            
            # Get total counts
            stats["total_nodes"] = sum(stats["nodes_by_label"].values())
            stats["total_relationships"] = sum(stats["relationships_by_type"].values())
            
            # Get database size estimate
            size_query = """
                CALL dbms.queryJmx('org.neo4j:instance=kernel#0,name=Store file sizes') 
                YIELD attributes 
                RETURN attributes.TotalStoreSize.value as size
            """
            
            try:
                size_result = await self.execute_query(size_query)
                if size_result["data"]:
                    stats["database_size_bytes"] = size_result["data"][0].get("size", 0)
            except:
                stats["database_size_bytes"] = None
            
            return stats
            
        except Exception as e:
            app_logger.error(f"Failed to get graph statistics: {e}")
            return {}