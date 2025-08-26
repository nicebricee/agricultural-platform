"""
Keyword extraction service for natural language queries.
"""

import re
from typing import List, Set
import nltk
from app.core.logging import app_logger

# Download required NLTK data (run once)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)


class KeywordExtractor:
    """Extracts meaningful keywords from natural language queries."""
    
    def __init__(self):
        """Initialize the keyword extractor."""
        # English stop words
        from nltk.corpus import stopwords
        self.stop_words = set(stopwords.words('english'))
        
        # Agricultural domain-specific terms to prioritize
        self.agricultural_terms = {
            # Crops
            'corn', 'wheat', 'soybean', 'rice', 'cotton', 'barley', 'oats',
            'hay', 'alfalfa', 'sugarcane', 'vegetables', 'fruits', 'grain',
            
            # Farm operations
            'farm', 'farms', 'farmer', 'agriculture', 'harvest', 'planting',
            'irrigation', 'cultivation', 'crop', 'yield', 'production',
            
            # Equipment
            'tractor', 'harvester', 'planter', 'sprayer', 'equipment',
            'machinery', 'implements', 'maintenance', 'repair',
            
            # Supply chain
            'supplier', 'distributor', 'elevator', 'storage', 'transport',
            'logistics', 'supply', 'chain', 'market', 'buyer', 'seller',
            
            # Environmental
            'drought', 'flood', 'weather', 'climate', 'soil', 'water',
            'rainfall', 'temperature', 'season', 'environmental',
            
            # Economic
            'price', 'cost', 'revenue', 'profit', 'subsidy', 'insurance',
            'loan', 'credit', 'investment', 'economic', 'financial',
            
            # Certifications
            'organic', 'certified', 'sustainable', 'gmo', 'conventional',
            'certification', 'standard', 'regulation', 'compliance',
            
            # Locations
            'county', 'state', 'region', 'area', 'zone', 'district',
            'iowa', 'california', 'texas', 'nebraska', 'kansas'
        }
        
        # Query patterns that indicate specific intents
        self.query_patterns = {
            'impact': r'\b(affect|impact|influence|consequence)\b',
            'trend': r'\b(trend|pattern|change|growth|decline)\b',
            'comparison': r'\b(compare|versus|vs|difference|better|worse)\b',
            'location': r'\b(where|location|region|area|near|nearby)\b',
            'quantity': r'\b(how many|count|number|amount|total)\b',
            'quality': r'\b(best|worst|top|bottom|reliable|quality)\b',
            'prediction': r'\b(predict|forecast|future|will|expect)\b',
            'relationship': r'\b(related|connected|linked|associated)\b'
        }
    
    async def extract(self, query: str, max_keywords: int = 10) -> List[str]:
        """
        Extract keywords from a natural language query.
        
        Args:
            query: The natural language query
            max_keywords: Maximum number of keywords to return
            
        Returns:
            List of extracted keywords
        """
        app_logger.debug(f"Extracting keywords from: {query}")
        
        # Convert to lowercase
        query_lower = query.lower()
        
        # Remove punctuation except hyphens
        query_clean = re.sub(r'[^\w\s\-]', ' ', query_lower)
        
        # Tokenize
        words = query_clean.split()
        
        # Extract keywords
        keywords = []
        keyword_set = set()
        
        # First, add agricultural domain terms
        for word in words:
            if word in self.agricultural_terms and word not in keyword_set:
                keywords.append(word)
                keyword_set.add(word)
        
        # Then add non-stop words
        for word in words:
            if (word not in self.stop_words and 
                word not in keyword_set and 
                len(word) > 2):
                keywords.append(word)
                keyword_set.add(word)
        
        # Add intent indicators
        for intent, pattern in self.query_patterns.items():
            if re.search(pattern, query_lower):
                if intent not in keyword_set:
                    keywords.append(intent)
                    keyword_set.add(intent)
        
        # Extract numbers and years
        numbers = re.findall(r'\b\d{4}\b|\b\d+\b', query)
        for num in numbers[:2]:  # Limit to 2 numbers
            if num not in keyword_set:
                keywords.append(num)
                keyword_set.add(num)
        
        # Limit to max_keywords
        result = keywords[:max_keywords]
        
        app_logger.info(f"Extracted {len(result)} keywords: {result}")
        return result
    
    def identify_query_type(self, query: str) -> str:
        """
        Identify the type of query based on patterns.
        
        Args:
            query: The natural language query
            
        Returns:
            Query type identifier
        """
        query_lower = query.lower()
        
        # Check for specific query types (order matters - more specific first)
        if re.search(r'\b(predict|forecast|future)\b', query_lower):
            return 'prediction'
        elif re.search(r'\b(impact|affect|consequence)\b', query_lower):
            return 'impact_analysis'
        elif re.search(r'\b(trends?|patterns?|over time)\b', query_lower):
            return 'trend_analysis'
        elif re.search(r'\b(compare|versus|vs)\b', query_lower):
            return 'comparison'
        elif re.search(r'\b(best|worst|top|most|least)\b', query_lower):
            return 'ranking'
        elif re.search(r'\b(where|location|near|within|miles|km|nearby)\b', query_lower):
            return 'location_based'
        elif re.search(r'\b(how many|count|number)\b', query_lower):
            return 'aggregation'
        else:
            return 'general'