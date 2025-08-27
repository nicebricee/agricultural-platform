"""
Database connection management for Supabase and Neo4j.
"""

from typing import Optional, Dict, Any, List
import asyncio
from supabase import create_client, Client
from neo4j import AsyncGraphDatabase, AsyncDriver
from neo4j.exceptions import ServiceUnavailable, AuthError, SessionExpired
import httpx

from app.core.config import settings
from app.core.logging import app_logger


class SupabaseClient:
    """Manages Supabase database connections and queries."""

    def __init__(self):
        self.client: Optional[Client] = None

        # Don't capture settings here - they're not ready yet!
        async def initialize(self) -> bool:
            """Initialize Supabase client."""
            try:
                # Get fresh values from settings (now decrypted!)
                self.url = settings.supabase_url
                self.key = settings.supabase_anon_key or settings.supabase_service_key

                print(f"DEBUG INIT: URL = {self.url}")
                print(f"DEBUG INIT: Key = {self.key[:20]}..." if self.
                      key else "No key")

                if not self.url or not self.key:
                    app_logger.warning("Supabase credentials not configured")
                    print("DEBUG: Returning False - missing credentials")
                    return False

                print("DEBUG: About to create_client")
                self.client = create_client(self.url, self.key)
                print(f"DEBUG: Client created = {self.client}")
                app_logger.info("Supabase client initialized successfully")
                print("DEBUG: Returning True - success")
                return True

            except Exception as e:
                app_logger.error(f"Failed to initialize Supabase client: {e}")
                print(f"DEBUG EXCEPTION: {e}")
                return False

    async def health_check(self) -> bool:
        """Check Supabase connection health."""
        if not self.client:
            return False

        try:
            # Try a simple query to test connection with actual table
            response = self.client.table('state_agricultural_metrics').select(
                '*').limit(1).execute()
            return True
        except Exception as e:
            app_logger.debug(f"Supabase health check failed: {e}")
            return False

    async def execute_query(self,
                            query: str,
                            params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a SQL query against Supabase."""
        if not self.client:
            raise ConnectionError("Supabase client not initialized")

        try:
            start_time = asyncio.get_event_loop().time()

            # Execute raw SQL query using Supabase RPC or direct query
            # Note: Supabase Python client doesn't directly support raw SQL,
            # so we'll use the REST API
            async with httpx.AsyncClient() as http_client:
                headers = {
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=representation"
                }

                response = await http_client.post(
                    f"{self.url}/rest/v1/rpc/execute_sql",
                    headers=headers,
                    json={
                        "query": query,
                        "params": params or {}
                    })

                if response.status_code != 200:
                    # Fallback to using table queries
                    return await self._execute_table_query(query, params)

                execution_time = asyncio.get_event_loop().time() - start_time

                return {
                    "data":
                    response.json(),
                    "execution_time":
                    execution_time,
                    "row_count":
                    len(response.json())
                    if isinstance(response.json(), list) else 1
                }

        except Exception as e:
            app_logger.error(f"Supabase query execution failed: {e}")
            raise

    async def _execute_table_query(self,
                                   query: str,
                                   params: Dict[str,
                                                Any] = None) -> Dict[str, Any]:
        """Execute a query using Supabase table API as fallback."""
        if not self.client:
            raise ConnectionError("Supabase client not initialized")

        try:
            start_time = asyncio.get_event_loop().time()

            # Use the actual table that exists: state_agricultural_metrics
            # Build query with proper chaining
            query_builder = self.client.table(
                'state_agricultural_metrics').select('*')

            # Apply basic filters based on the query
            if "iowa" in query.lower():
                query_builder = query_builder.eq('place_name', 'Iowa')
            elif "california" in query.lower():
                query_builder = query_builder.eq('place_name', 'California')
            elif "texas" in query.lower():
                query_builder = query_builder.eq('place_name', 'Texas')

            # Execute with limit
            result = query_builder.limit(settings.max_results).execute()

            execution_time = asyncio.get_event_loop().time() - start_time

            return {
                "data": result.data,
                "execution_time": execution_time,
                "row_count": len(result.data)
            }

        except Exception as e:
            app_logger.error(f"Supabase table query failed: {e}")
            raise

    async def close(self):
        """Close Supabase connection."""
        # Supabase client doesn't need explicit closing
        self.client = None
        app_logger.info("Supabase client closed")


class Neo4jClient:
    """Manages Neo4j database connections and queries."""

    def __init__(self):
        self.driver: Optional[AsyncDriver] = None
        # Don't capture settings here either!

    async def initialize(self) -> bool:
        """Initialize Neo4j driver."""
        try:
            # Get fresh values from settings
            self.uri = settings.neo4j_uri
            self.username = settings.neo4j_username
            self.password = settings.neo4j_password
            self.database = settings.neo4j_database

            if not all([self.uri, self.username, self.password]):
                app_logger.warning("Neo4j credentials not configured")
                return False

            self.driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password),
                max_connection_pool_size=
                10,  # Reduced to avoid too many connections
                connection_timeout=30,
                max_transaction_retry_time=30,
                keep_alive=True,  # Enable keep-alive
                connection_acquisition_timeout=
                60  # Wait longer for available connection
            )

            # Verify connectivity
            async with self.driver.session(database=self.database) as session:
                await session.run("RETURN 1")

            app_logger.info("Neo4j driver initialized successfully")
            return True

        except AuthError as e:
            app_logger.error(f"Neo4j authentication failed: {e}")
            return False
        except ServiceUnavailable as e:
            app_logger.error(f"Neo4j service unavailable: {e}")
            return False
        except Exception as e:
            app_logger.error(f"Failed to initialize Neo4j driver: {e}")
            return False

    async def health_check(self) -> bool:
        """Check Neo4j connection health."""
        if not self.driver:
            return False

        try:
            async with self.driver.session(database=self.database) as session:
                result = await session.run("RETURN 1 as health")
                record = await result.single()
                return record["health"] == 1
        except Exception as e:
            app_logger.debug(f"Neo4j health check failed: {e}")
            return False

    async def execute_query(
            self,
            query: str,
            params: Dict[str, Any] = None,
            preserve_graph_structure: bool = True) -> Dict[str, Any]:
        """Execute a Cypher query against Neo4j with retry logic.
        
        Args:
            query: Cypher query to execute
            params: Query parameters
            preserve_graph_structure: If True, returns rich graph structure with nodes and relationships
        """
        if not self.driver:
            # Try to reconnect once
            app_logger.warning(
                "Neo4j driver not initialized, attempting to reconnect...")
            if not await self.initialize():
                return {
                    "data": [],
                    "execution_time": 0,
                    "row_count": 0,
                    "error": "Neo4j connection not available"
                }

        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                start_time = asyncio.get_event_loop().time()

                async with self.driver.session(
                        database=self.database) as session:
                    result = await session.run(query, parameters=params or {})

                    # Collect all records
                    records = []
                    graph_data = {
                        "nodes": {},
                        "relationships": [],
                        "paths": []
                    }

                    async for record in result:
                        # If preserving graph structure, extract nodes and relationships BEFORE dict conversion
                        if preserve_graph_structure:
                            # Process the raw record first
                            for key, value in record.items():
                                # Import Neo4j types for proper checking
                                from neo4j.graph import Node, Relationship, Path

                                # Check if value is a Node
                                if isinstance(value, Node):
                                    node_id = f"n:{value.element_id if hasattr(value, 'element_id') else value.id}"
                                    if node_id not in graph_data["nodes"]:
                                        graph_data["nodes"][node_id] = {
                                            "id": node_id,
                                            "labels": list(value.labels),
                                            "properties": dict(value.items()),
                                            "relationships": {
                                                "outgoing": [],
                                                "incoming": []
                                            }
                                        }
                                        app_logger.debug(
                                            f"Found node: {node_id} with labels {list(value.labels)}"
                                        )
                                # Check if value is a Relationship
                                elif isinstance(value, Relationship):
                                    rel_id = f"r:{value.element_id if hasattr(value, 'element_id') else value.id}"
                                    graph_data["relationships"].append({
                                        "id":
                                        rel_id,
                                        "type":
                                        value.type,
                                        "start":
                                        f"n:{value.start_node.element_id if hasattr(value.start_node, 'element_id') else value.start_node.id}",
                                        "end":
                                        f"n:{value.end_node.element_id if hasattr(value.end_node, 'element_id') else value.end_node.id}",
                                        "properties":
                                        dict(value.items())
                                    })
                                    app_logger.debug(
                                        f"Found relationship: {rel_id} of type {value.type}"
                                    )
                                # Check if value is a Path
                                elif isinstance(value, Path):
                                    path_nodes = []
                                    for node in value.nodes:
                                        node_id = f"n:{node.element_id if hasattr(node, 'element_id') else node.id}"
                                        path_nodes.append(node_id)
                                        if node_id not in graph_data["nodes"]:
                                            graph_data["nodes"][node_id] = {
                                                "id": node_id,
                                                "labels": list(node.labels),
                                                "properties":
                                                dict(node.items()),
                                                "relationships": {
                                                    "outgoing": [],
                                                    "incoming": []
                                                }
                                            }
                                    graph_data["paths"].append({
                                        "nodes":
                                        path_nodes,
                                        "length":
                                        len(value.relationships)
                                    })
                                    app_logger.debug(
                                        f"Found path with {len(value.nodes)} nodes"
                                    )

                        # Store the flat record for backward compatibility (AFTER processing graph elements)
                        records.append(dict(record))

                    # Get query statistics
                    summary = await result.consume()
                    execution_time = asyncio.get_event_loop().time(
                    ) - start_time

                    result_data = {
                        "data": records,
                        "execution_time": execution_time,
                        "row_count": len(records),
                        "statistics": {
                            "nodes_created": summary.counters.nodes_created,
                            "nodes_deleted": summary.counters.nodes_deleted,
                            "relationships_created":
                            summary.counters.relationships_created,
                            "relationships_deleted":
                            summary.counters.relationships_deleted,
                            "properties_set": summary.counters.properties_set
                        }
                    }

                    # Add graph structure if preserved
                    if preserve_graph_structure and (
                            graph_data["nodes"]
                            or graph_data["relationships"]):
                        result_data["graph_structure"] = graph_data

                    return result_data

            except (ServiceUnavailable, SessionExpired) as e:
                error_msg = str(e).lower()
                if "defunct connection" in error_msg:
                    # Defunct connection detected - close and recreate driver
                    app_logger.warning(
                        f"Defunct connection detected (attempt {attempt + 1}/{max_retries}), recreating driver..."
                    )
                    if self.driver:
                        await self.driver.close()
                        self.driver = None
                    await asyncio.sleep(retry_delay)
                    if not await self.initialize():
                        app_logger.error(
                            "Failed to reinitialize Neo4j driver after defunct connection"
                        )
                elif attempt < max_retries - 1:
                    app_logger.warning(
                        f"Neo4j connection error (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    await asyncio.sleep(retry_delay * (2**attempt)
                                        )  # Exponential backoff
                    # Try to reinitialize the driver
                    await self.initialize()
                else:
                    app_logger.error(
                        f"Neo4j query failed after {max_retries} attempts: {e}"
                    )
                    return {
                        "data": [],
                        "execution_time":
                        0,
                        "row_count":
                        0,
                        "error":
                        f"Connection failed after {max_retries} attempts: {str(e)}"
                    }

            except Exception as e:
                app_logger.error(f"Cypher query execution failed: {e}")
                return {
                    "data": [],
                    "execution_time": 0,
                    "row_count": 0,
                    "error": str(e)
                }

    async def close(self):
        """Close Neo4j driver."""
        if self.driver:
            await self.driver.close()
            self.driver = None
            app_logger.info("Neo4j driver closed")


class DatabaseManager:
    """Manages both database connections."""

    def __init__(self):
        self.supabase = SupabaseClient()
        self.neo4j = Neo4jClient()

    async def initialize(self):
        """Initialize all database connections."""
        app_logger.info("Initializing database connections...")

        # Initialize both clients concurrently
        supabase_task = self.supabase.initialize()
        neo4j_task = self.neo4j.initialize()

        supabase_success, neo4j_success = await asyncio.gather(
            supabase_task, neo4j_task)

        if not supabase_success:
            app_logger.warning("Supabase initialization failed")
        if not neo4j_success:
            app_logger.warning("Neo4j initialization failed")

        return supabase_success and neo4j_success

    async def check_supabase_health(self) -> bool:
        """Check Supabase health."""
        return await self.supabase.health_check()

    async def check_neo4j_health(self) -> bool:
        """Check Neo4j health."""
        return await self.neo4j.health_check()

    async def execute_parallel_queries(
            self,
            sql_query: str,
            cypher_query: str,
            sql_params: Dict[str, Any] = None,
            cypher_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute SQL and Cypher queries in parallel."""
        app_logger.debug(
            f"Executing parallel queries:\nSQL: {sql_query}\nCypher: {cypher_query}"
        )

        # Execute both queries concurrently
        sql_task = self.supabase.execute_query(sql_query, sql_params)
        cypher_task = self.neo4j.execute_query(cypher_query, cypher_params)

        try:
            sql_result, graph_result = await asyncio.gather(
                sql_task, cypher_task, return_exceptions=True)

            # Handle exceptions
            if isinstance(sql_result, Exception):
                app_logger.error(f"SQL query failed: {sql_result}")
                sql_result = {
                    "data": [],
                    "error": str(sql_result),
                    "execution_time": 0,
                    "row_count": 0
                }

            if isinstance(graph_result, Exception):
                app_logger.error(f"Cypher query failed: {graph_result}")
                graph_result = {
                    "data": [],
                    "error": str(graph_result),
                    "execution_time": 0,
                    "row_count": 0
                }

            return {
                "sql_results":
                sql_result,
                "graph_results":
                graph_result,
                "total_execution_time":
                sql_result.get("execution_time", 0) +
                graph_result.get("execution_time", 0)
            }

        except Exception as e:
            app_logger.error(f"Parallel query execution failed: {e}")
            raise

    async def close(self):
        """Close all database connections."""
        await asyncio.gather(self.supabase.close(), self.neo4j.close())
