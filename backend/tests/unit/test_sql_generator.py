"""
Unit tests for the SQL query generation service.
"""

import pytest
from app.services.sql_query_generator import SQLQueryGenerator


@pytest.fixture
def sql_generator():
    """Create a SQL query generator instance."""
    return SQLQueryGenerator()


class TestSQLQueryGenerator:
    """Test suite for SQL query generation functionality."""
    
    @pytest.mark.asyncio
    async def test_general_query_generation(self, sql_generator):
        """Test generation of a general SQL query."""
        keywords = ["corn", "farms", "iowa"]
        result = await sql_generator.generate("Show corn farms in Iowa", keywords)
        
        assert "sql" in result
        assert result["query_type"] == "general"
        assert "farms" in result["tables_used"]
        
        sql = result["sql"].lower()
        assert "select" in sql
        assert "from farms" in sql
        assert "where" in sql
        assert "limit" in sql
    
    @pytest.mark.asyncio
    async def test_impact_query_generation(self, sql_generator):
        """Test generation of impact analysis query."""
        keywords = ["drought", "impact", "corn", "production"]
        result = await sql_generator.generate(
            "What's the impact of drought on corn production?", 
            keywords
        )
        
        assert result["query_type"] == "impact_analysis"
        sql = result["sql"].lower()
        assert "weather_impact" in sql or "weather_events" in sql
        assert "join" in sql
    
    @pytest.mark.asyncio
    async def test_trend_query_generation(self, sql_generator):
        """Test generation of trend analysis query."""
        keywords = ["trend", "production", "corn", "years"]
        result = await sql_generator.generate(
            "Show production trends over the years",
            keywords
        )
        
        assert result["query_type"] == "trend_analysis"
        sql = result["sql"].lower()
        assert "avg(" in sql or "sum(" in sql
        assert "group by" in sql
        assert "order by" in sql
    
    @pytest.mark.asyncio
    async def test_comparison_query_generation(self, sql_generator):
        """Test generation of comparison query."""
        keywords = ["compare", "organic", "conventional", "farms"]
        result = await sql_generator.generate(
            "Compare organic versus conventional farms",
            keywords
        )
        
        assert result["query_type"] == "comparison"
        sql = result["sql"].lower()
        assert "certification_type" in sql
        assert "group by" in sql
    
    @pytest.mark.asyncio
    async def test_ranking_query_generation(self, sql_generator):
        """Test generation of ranking query."""
        keywords = ["best", "farms", "revenue", "top"]
        result = await sql_generator.generate(
            "Show the top farms by revenue",
            keywords
        )
        
        assert result["query_type"] == "ranking"
        sql = result["sql"].lower()
        assert "order by" in sql
        assert "desc" in sql
    
    @pytest.mark.asyncio
    async def test_location_query_generation(self, sql_generator):
        """Test generation of location-based query."""
        keywords = ["farms", "near", "iowa", "location"]
        result = await sql_generator.generate(
            "Find farms near Iowa",
            keywords
        )
        
        assert result["query_type"] == "location_based"
        sql = result["sql"].lower()
        assert "iowa" in sql
        assert "state" in sql or "location" in sql
    
    @pytest.mark.asyncio
    async def test_aggregation_query_generation(self, sql_generator):
        """Test generation of aggregation query."""
        keywords = ["how", "many", "farms", "count", "organic"]
        result = await sql_generator.generate(
            "How many organic farms are there?",
            keywords
        )
        
        assert result["query_type"] == "aggregation"
        sql = result["sql"].lower()
        assert "count(" in sql
        assert "group by" in sql
    
    @pytest.mark.asyncio
    async def test_table_identification(self, sql_generator):
        """Test correct identification of tables to query."""
        # Test farms table
        keywords = ["farms", "owners", "location"]
        result = await sql_generator.generate("test", keywords)
        assert "farms" in result["tables_used"]
        
        # Test equipment table
        keywords = ["tractors", "maintenance", "equipment"]
        result = await sql_generator.generate("test", keywords)
        assert "equipment" in result["tables_used"]
        
        # Test suppliers table
        keywords = ["suppliers", "delivery", "vendor"]
        result = await sql_generator.generate("test", keywords)
        assert "suppliers" in result["tables_used"]
        
        # Test production table
        keywords = ["yield", "harvest", "production"]
        result = await sql_generator.generate("test", keywords)
        assert "production_records" in result["tables_used"]
        
        # Test weather table
        keywords = ["drought", "flood", "weather"]
        result = await sql_generator.generate("test", keywords)
        assert "weather_events" in result["tables_used"]
    
    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, sql_generator):
        """Test that SQL injection attempts are handled."""
        keywords = ["'; DROP TABLE farms; --", "farms"]
        result = await sql_generator.generate(
            "Show farms'; DROP TABLE farms; --",
            keywords
        )
        
        sql = result["sql"]
        # Check that dangerous SQL is escaped or parameterized
        assert "DROP TABLE" not in sql.upper() or "%" in sql
    
    @pytest.mark.asyncio
    async def test_query_limit_enforcement(self, sql_generator):
        """Test that query results are limited."""
        keywords = ["farms", "all"]
        result = await sql_generator.generate("Show all farms", keywords, limit=25)
        
        sql = result["sql"]
        assert "LIMIT 25" in sql or "limit 25" in sql.lower()
        assert result["limit"] == 25
    
    @pytest.mark.asyncio
    async def test_join_logic(self, sql_generator):
        """Test that appropriate JOINs are generated."""
        keywords = ["farms", "equipment", "suppliers"]
        result = await sql_generator.generate(
            "Show farms with their equipment and suppliers",
            keywords
        )
        
        sql = result["sql"].lower()
        assert "join" in sql or "left join" in sql
        assert "equipment" in sql
        assert "farm_suppliers" in sql or "suppliers" in sql
    
    @pytest.mark.asyncio
    async def test_empty_keywords(self, sql_generator):
        """Test handling of empty keywords."""
        result = await sql_generator.generate("test query", [])
        
        assert "sql" in result
        assert result["tables_used"] == ["farms"]  # Should default to farms
    
    @pytest.mark.asyncio
    async def test_spatial_query_generation(self, sql_generator):
        """Test generation of spatial queries."""
        keywords = ["farms", "within", "50", "miles", "location"]
        result = await sql_generator.generate(
            "Find farms within 50 miles",
            keywords
        )
        
        sql = result["sql"].lower()
        # Should include spatial functions
        assert "st_distance" in sql or "distance" in sql
    
    def test_explain_query(self, sql_generator):
        """Test query explanation generation."""
        sql = """
            SELECT f.*, COUNT(e.id) as equipment_count
            FROM farms f
            LEFT JOIN equipment e ON f.id = e.farm_id
            WHERE f.state = 'Iowa'
            GROUP BY f.id
            ORDER BY equipment_count DESC
            LIMIT 50
        """
        
        explanation = sql_generator.explain_query(sql)
        
        assert "aggregates" in explanation.lower()
        assert "equipment" in explanation.lower()
        assert "filter" in explanation.lower() or "condition" in explanation.lower()
        assert "sorted" in explanation.lower() or "order" in explanation.lower()
    
    @pytest.mark.asyncio
    async def test_weather_impact_query(self, sql_generator):
        """Test weather impact query generation."""
        keywords = ["drought", "impact", "farms", "weather"]
        result = await sql_generator.generate(
            "Show drought impact on farms",
            keywords
        )
        
        sql = result["sql"].lower()
        assert "weather" in sql
        assert "join" in sql
        assert "severity" in sql or "impact" in sql
    
    @pytest.mark.asyncio
    async def test_production_trend_query(self, sql_generator):
        """Test production trend query generation."""
        keywords = ["trend", "corn", "production", "5", "years"]
        result = await sql_generator.generate(
            "Show corn production trend for last 5 years",
            keywords
        )
        
        sql = result["sql"].lower()
        assert "production_records" in sql
        assert "year" in sql
        assert "avg(" in sql or "sum(" in sql
        assert "group by" in sql
    
    @pytest.mark.asyncio
    async def test_organic_comparison_query(self, sql_generator):
        """Test organic vs conventional comparison query."""
        keywords = ["organic", "conventional", "compare", "yield"]
        result = await sql_generator.generate(
            "Compare organic vs conventional farm yields",
            keywords
        )
        
        sql = result["sql"].lower()
        assert "certification_type" in sql
        assert "organic" in sql
        assert "conventional" in sql
        assert "avg(" in sql or "group by" in sql