"""
Unit tests for the Cypher query generation service.
"""

import pytest
from app.services.cypher_query_generator import CypherQueryGenerator


@pytest.fixture
def cypher_generator():
    """Create a Cypher query generator instance."""
    return CypherQueryGenerator()


class TestCypherQueryGenerator:
    """Test suite for Cypher query generation functionality."""
    
    @pytest.mark.asyncio
    async def test_general_query_generation(self, cypher_generator):
        """Test generation of a general Cypher query."""
        keywords = ["corn", "production", "iowa"]
        result = await cypher_generator.generate("Show corn production in Iowa", keywords)
        
        assert "cypher" in result
        assert result["query_type"] == "general"
        assert "State" in result["nodes_involved"]
        assert "Measurement" in result["nodes_involved"]
        
        cypher = result["cypher"].lower()
        assert "match" in cypher
        assert "state" in cypher
        assert "measurement" in cypher
        assert "where" in cypher
        assert "return" in cypher
        assert "limit" in cypher
    
    @pytest.mark.asyncio
    async def test_impact_query_generation(self, cypher_generator):
        """Test generation of impact analysis query."""
        keywords = ["impact", "neighbor", "states"]
        result = await cypher_generator.generate(
            "What's the impact on neighboring states?", 
            keywords
        )
        
        assert result["query_type"] == "impact_analysis"
        cypher = result["cypher"].lower()
        assert "state" in cypher
        assert "borders" in cypher
        assert "match" in cypher
    
    @pytest.mark.asyncio
    async def test_relationship_query_generation(self, cypher_generator):
        """Test generation of relationship exploration query."""
        keywords = ["states", "connected", "regions", "related"]
        result = await cypher_generator.generate(
            "Show how states are connected to regions",
            keywords
        )
        
        cypher = result["cypher"].lower()
        assert "match" in cypher
        assert "path" in cypher or "-[" in cypher
    
    @pytest.mark.asyncio
    async def test_pattern_matching(self, cypher_generator):
        """Test pattern matching in Cypher queries."""
        keywords = ["states", "measurements", "has"]
        result = await cypher_generator.generate(
            "Find states that have measurements",
            keywords
        )
        
        cypher = result["cypher"]
        # Should contain relationship pattern
        assert "-[" in cypher and "]-" in cypher
        assert "State" in cypher or "state" in cypher.lower()
        assert "Measurement" in cypher or "measurement" in cypher.lower()
    
    @pytest.mark.asyncio
    async def test_graph_traversal(self, cypher_generator):
        """Test graph traversal query generation."""
        keywords = ["states", "borders", "connected"]
        result = await cypher_generator.generate(
            "Find connected states through borders",
            keywords
        )
        
        cypher = result["cypher"].lower()
        # Should have path traversal
        assert "match" in cypher
        assert "state" in cypher
    
    @pytest.mark.asyncio
    async def test_node_identification(self, cypher_generator):
        """Test correct identification of node types."""
        # Test State nodes
        keywords = ["states", "iowa", "california"]
        result = await cypher_generator.generate("test", keywords)
        assert "State" in result["nodes_involved"]
        
        # Test Region nodes
        keywords = ["region", "midwest", "regional"]
        result = await cypher_generator.generate("test", keywords)
        assert "Region" in result["nodes_involved"]
        
        # Test Climate nodes
        keywords = ["climate", "weather", "temperature"]
        result = await cypher_generator.generate("test", keywords)
        assert "Climate" in result["nodes_involved"]
        
        # Test AgriculturalBelt nodes
        keywords = ["corn belt", "wheat belt", "belt"]
        result = await cypher_generator.generate("test", keywords)
        assert "AgriculturalBelt" in result["nodes_involved"]
        
        # Test Year nodes
        keywords = ["year", "annual", "yearly"]
        result = await cypher_generator.generate("test", keywords)
        assert "Year" in result["nodes_involved"]
    
    @pytest.mark.asyncio
    async def test_aggregation_query(self, cypher_generator):
        """Test aggregation in Cypher queries."""
        keywords = ["count", "states", "total", "how", "many"]
        result = await cypher_generator.generate(
            "How many states are there?",
            keywords
        )
        
        cypher = result["cypher"].lower()
        assert "count(" in cypher or "sum(" in cypher or "avg(" in cypher
        assert "return" in cypher
    
    @pytest.mark.asyncio
    async def test_optional_match(self, cypher_generator):
        """Test OPTIONAL MATCH generation."""
        keywords = ["states", "regions", "climate"]
        result = await cypher_generator.generate(
            "Show states with their regions and climate",
            keywords
        )
        
        cypher = result["cypher"]
        assert "OPTIONAL MATCH" in cypher or "optional match" in cypher.lower()
    
    @pytest.mark.asyncio
    async def test_where_clause_generation(self, cypher_generator):
        """Test WHERE clause generation."""
        keywords = ["organic", "farms", "iowa"]
        result = await cypher_generator.generate(
            "Find organic farms in Iowa",
            keywords
        )
        
        cypher = result["cypher"].lower()
        assert "where" in cypher
        assert "organic" in cypher or "certification" in cypher
    
    @pytest.mark.asyncio
    async def test_limit_enforcement(self, cypher_generator):
        """Test that query results are limited."""
        keywords = ["farms", "all"]
        result = await cypher_generator.generate("Show all farms", keywords, limit=25)
        
        cypher = result["cypher"]
        assert "LIMIT 25" in cypher or "limit 25" in cypher.lower()
        assert result["limit"] == 25
    
    @pytest.mark.asyncio
    async def test_cypher_injection_prevention(self, cypher_generator):
        """Test that Cypher injection attempts are handled."""
        keywords = ["'; MATCH (n) DETACH DELETE n; //", "farms"]
        result = await cypher_generator.generate(
            "Show farms'; MATCH (n) DETACH DELETE n; //",
            keywords
        )
        
        cypher = result["cypher"]
        # Check that dangerous Cypher is not executed
        assert "DELETE" not in cypher.upper() or "'" not in cypher
    
    @pytest.mark.asyncio
    async def test_path_query_generation(self, cypher_generator):
        """Test path finding query generation."""
        keywords = ["shortest", "path", "farm", "market"]
        result = await cypher_generator.generate(
            "Find shortest path from farm to market",
            keywords
        )
        
        cypher = result["cypher"].lower()
        assert "path" in cypher
        assert "match" in cypher
    
    @pytest.mark.asyncio
    async def test_collect_aggregation(self, cypher_generator):
        """Test COLLECT aggregation function."""
        keywords = ["farms", "crops", "list", "all"]
        result = await cypher_generator.generate(
            "List all crops for each farm",
            keywords
        )
        
        cypher = result["cypher"].lower()
        assert "collect(" in cypher or "collect distinct" in cypher
    
    @pytest.mark.asyncio
    async def test_comparison_query(self, cypher_generator):
        """Test comparison query generation."""
        keywords = ["compare", "iowa", "california"]
        result = await cypher_generator.generate(
            "Compare Iowa vs California",
            keywords
        )
        
        assert result["query_type"] == "comparison"
        cypher = result["cypher"].lower()
        assert "state" in cypher
    
    @pytest.mark.asyncio
    async def test_location_based_query(self, cypher_generator):
        """Test location-based query generation."""
        keywords = ["farms", "near", "iowa", "location"]
        result = await cypher_generator.generate(
            "Find farms near Iowa",
            keywords
        )
        
        assert result["query_type"] == "location_based"
        cypher = result["cypher"].lower()
        assert "iowa" in cypher
        assert "state" in cypher or "location" in cypher
    
    @pytest.mark.asyncio
    async def test_empty_keywords(self, cypher_generator):
        """Test handling of empty keywords."""
        result = await cypher_generator.generate("test query", [])
        
        assert "cypher" in result
        assert "State" in result["nodes_involved"]  # Should include State
        assert "Measurement" in result["nodes_involved"]  # Should include Measurement
    
    def test_explain_query(self, cypher_generator):
        """Test query explanation generation."""
        cypher = """
            MATCH (s:State)-[:HAS_MEASUREMENT]->(m:Measurement)
            WHERE s.name = 'Iowa'
            OPTIONAL MATCH (s)-[:IN_REGION]->(r:Region)
            WITH s, COUNT(m) as measurement_count, COLLECT(r.name) as regions
            RETURN s.name, measurement_count, regions
            ORDER BY measurement_count DESC
            LIMIT 50
        """
        
        explanation = cypher_generator.explain_query(cypher)
        
        assert "graph" in explanation.lower() or "pattern" in explanation.lower()
        assert "equipment" in explanation.lower()
        assert "optional" in explanation.lower() or "supplier" in explanation.lower()
    
    @pytest.mark.asyncio
    async def test_multi_hop_traversal(self, cypher_generator):
        """Test multi-hop relationship traversal."""
        keywords = ["farms", "suppliers", "chain", "impact", "3"]
        result = await cypher_generator.generate(
            "Show supply chain impact within 3 hops",
            keywords
        )
        
        cypher = result["cypher"]
        # Should contain bounded path traversal
        assert "*1..3" in cypher or "[*..3]" in cypher or "1..3" in cypher
    
    @pytest.mark.asyncio
    async def test_node_property_filtering(self, cypher_generator):
        """Test filtering by node properties."""
        keywords = ["farms", "corn", "500", "acres", "organic"]
        result = await cypher_generator.generate(
            "Find organic corn farms over 500 acres",
            keywords
        )
        
        cypher = result["cypher"].lower()
        # Should filter on multiple properties
        assert "corn" in cypher
        assert "organic" in cypher or "certification" in cypher
    
    @pytest.mark.asyncio
    async def test_relationship_properties(self, cypher_generator):
        """Test queries involving relationship properties."""
        keywords = ["farms", "suppliers", "contract", "2023"]
        result = await cypher_generator.generate(
            "Show farm supplier contracts from 2023",
            keywords
        )
        
        cypher = result["cypher"]
        # Should query relationship properties
        assert "-[" in cypher and "]-" in cypher
        assert "SUPPLIES" in cypher or "supplies" in cypher.lower()