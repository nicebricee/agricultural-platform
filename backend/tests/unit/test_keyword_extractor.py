"""
Unit tests for the keyword extraction service.
"""

import pytest
from app.services.keyword_extractor import KeywordExtractor


@pytest.fixture
def keyword_extractor():
    """Create a keyword extractor instance."""
    return KeywordExtractor()


class TestKeywordExtractor:
    """Test suite for keyword extraction functionality."""
    
    @pytest.mark.asyncio
    async def test_basic_extraction(self, keyword_extractor):
        """Test basic keyword extraction from natural language."""
        query = "Show me all corn farms in Iowa"
        keywords = await keyword_extractor.extract(query)
        
        assert "corn" in keywords
        assert "farms" in keywords
        assert "iowa" in keywords
        assert len(keywords) <= 10
    
    @pytest.mark.asyncio
    async def test_agricultural_terms_prioritized(self, keyword_extractor):
        """Test that agricultural domain terms are prioritized."""
        query = "Which farms have tractors and harvesters?"
        keywords = await keyword_extractor.extract(query)
        
        # Agricultural terms should appear first
        assert "farms" in keywords[:3]
        assert "tractor" in keywords or "tractors" in keywords
        assert "harvester" in keywords or "harvesters" in keywords
    
    @pytest.mark.asyncio
    async def test_stop_words_removed(self, keyword_extractor):
        """Test that stop words are filtered out."""
        query = "The farms in the area with the best equipment"
        keywords = await keyword_extractor.extract(query)
        
        # Stop words should not be in keywords
        assert "the" not in keywords
        assert "in" not in keywords
        assert "with" not in keywords
        
        # Content words should be present
        assert "farms" in keywords
        assert "equipment" in keywords
    
    @pytest.mark.asyncio
    async def test_numbers_extracted(self, keyword_extractor):
        """Test that numbers and years are extracted."""
        query = "Show 2023 production data for farms with over 500 acres"
        keywords = await keyword_extractor.extract(query)
        
        assert "2023" in keywords
        assert "500" in keywords
    
    @pytest.mark.asyncio
    async def test_query_intent_detection(self, keyword_extractor):
        """Test that query intent is detected."""
        query = "What's the impact of drought on corn production?"
        keywords = await keyword_extractor.extract(query)
        
        assert "impact" in keywords
        assert "drought" in keywords
        assert "corn" in keywords
        assert "production" in keywords
    
    @pytest.mark.asyncio
    async def test_max_keywords_limit(self, keyword_extractor):
        """Test that keyword count doesn't exceed maximum."""
        query = """
            Show me all farms with corn, wheat, soybeans, cotton, rice, 
            barley, oats, hay, alfalfa, vegetables, fruits, and grain 
            that have tractors, harvesters, planters, sprayers, and equipment
        """
        keywords = await keyword_extractor.extract(query, max_keywords=10)
        
        assert len(keywords) == 10
    
    @pytest.mark.asyncio
    async def test_empty_query(self, keyword_extractor):
        """Test handling of empty query."""
        keywords = await keyword_extractor.extract("")
        assert keywords == []
    
    @pytest.mark.asyncio
    async def test_special_characters(self, keyword_extractor):
        """Test handling of special characters."""
        query = "Farms with $1,000,000+ revenue & 100% organic certification!"
        keywords = await keyword_extractor.extract(query)
        
        assert "farms" in keywords
        assert "revenue" in keywords
        assert "organic" in keywords
        assert "certification" in keywords
    
    def test_identify_impact_query(self, keyword_extractor):
        """Test identification of impact analysis queries."""
        query = "What's the impact of fertilizer prices on farm profits?"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "impact_analysis"
    
    def test_identify_trend_query(self, keyword_extractor):
        """Test identification of trend analysis queries."""
        query = "Show me the trend in corn production over time"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "trend_analysis"
    
    def test_identify_comparison_query(self, keyword_extractor):
        """Test identification of comparison queries."""
        query = "Compare organic versus conventional farming methods"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "comparison"
    
    def test_identify_prediction_query(self, keyword_extractor):
        """Test identification of prediction queries."""
        query = "Predict next year's corn yield based on weather patterns"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "prediction"
    
    def test_identify_ranking_query(self, keyword_extractor):
        """Test identification of ranking queries."""
        query = "What are the top 10 most productive farms?"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "ranking"
    
    def test_identify_location_query(self, keyword_extractor):
        """Test identification of location-based queries."""
        query = "Where are the nearest grain elevators?"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "location_based"
    
    def test_identify_aggregation_query(self, keyword_extractor):
        """Test identification of aggregation queries."""
        query = "How many farms use organic certification?"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "aggregation"
    
    def test_identify_general_query(self, keyword_extractor):
        """Test identification of general queries."""
        query = "Tell me about farming in the midwest"
        query_type = keyword_extractor.identify_query_type(query)
        assert query_type == "general"
    
    @pytest.mark.asyncio
    async def test_agricultural_locations(self, keyword_extractor):
        """Test extraction of agricultural location terms."""
        query = "Show farms in Iowa, California, Texas, Nebraska, and Kansas"
        keywords = await keyword_extractor.extract(query)
        
        assert "iowa" in keywords
        assert "california" in keywords
        assert "texas" in keywords
        assert "nebraska" in keywords
        assert "kansas" in keywords
    
    @pytest.mark.asyncio
    async def test_equipment_terms(self, keyword_extractor):
        """Test extraction of equipment-related terms."""
        query = "Farms with John Deere tractors needing maintenance"
        keywords = await keyword_extractor.extract(query)
        
        assert "farms" in keywords
        assert "tractor" in keywords or "tractors" in keywords
        assert "maintenance" in keywords
    
    @pytest.mark.asyncio
    async def test_weather_terms(self, keyword_extractor):
        """Test extraction of weather-related terms."""
        query = "Impact of drought and flood on crop yield"
        keywords = await keyword_extractor.extract(query)
        
        assert "impact" in keywords
        assert "drought" in keywords
        assert "flood" in keywords
        assert "crop" in keywords
        assert "yield" in keywords
    
    @pytest.mark.asyncio
    async def test_economic_terms(self, keyword_extractor):
        """Test extraction of economic terms."""
        query = "Farms with high revenue and low costs"
        keywords = await keyword_extractor.extract(query)
        
        assert "farms" in keywords
        assert "revenue" in keywords
        assert "cost" in keywords or "costs" in keywords