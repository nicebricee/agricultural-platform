"""
SQL query generator for Supabase PostgreSQL database.
Converts natural language keywords to optimized SQL queries.
"""

from typing import List, Dict, Any, Optional
from app.core.logging import app_logger
from app.services.keyword_extractor import KeywordExtractor


class SQLQueryGenerator:
    """Generates SQL queries for Supabase based on keywords and query type."""
    
    def __init__(self):
        """Initialize the SQL query generator."""
        self.keyword_extractor = KeywordExtractor()
        
        # Table schemas for reference
        self.schemas = {
            'state_agricultural_metrics': {
                'columns': [
                    'id', 'place_dcid', 'place_name', 'year', 'metric_type',
                    'value', 'source', 'created_at'
                ],
                'relationships': []
            },
            'farms': {
                'columns': [
                    'id', 'name', 'location', 'owner', 'size_acres',
                    'primary_crop', 'certification_type', 'established_date',
                    'latitude', 'longitude', 'county', 'state'
                ],
                'relationships': ['equipment', 'suppliers', 'production_records']
            },
            'equipment': {
                'columns': [
                    'id', 'farm_id', 'type', 'manufacturer', 'model',
                    'purchase_date', 'maintenance_status', 'cost',
                    'last_service_date', 'next_service_date'
                ],
                'relationships': ['farms']
            },
            'suppliers': {
                'columns': [
                    'id', 'name', 'type', 'location', 'contact_info',
                    'reliability_score', 'average_delivery_time',
                    'products_offered', 'contract_terms'
                ],
                'relationships': ['farm_suppliers']
            },
            'farm_suppliers': {
                'columns': [
                    'farm_id', 'supplier_id', 'contract_start_date',
                    'contract_end_date', 'annual_volume', 'payment_terms'
                ],
                'relationships': ['farms', 'suppliers']
            },
            'production_records': {
                'columns': [
                    'id', 'farm_id', 'year', 'crop_type', 'acres_planted',
                    'yield_per_acre', 'total_production', 'revenue',
                    'weather_impact', 'market_price'
                ],
                'relationships': ['farms']
            },
            'weather_events': {
                'columns': [
                    'id', 'date', 'type', 'severity', 'affected_region',
                    'impact_description', 'estimated_damage', 'recovery_time'
                ],
                'relationships': []
            }
        }
    
    def _sanitize_input(self, text: str) -> str:
        """Sanitize input to prevent SQL injection."""
        # Remove dangerous SQL keywords and special characters
        dangerous_patterns = [
            'DELETE', 'DROP', 'TRUNCATE', 'UPDATE', 'INSERT', 'ALTER',
            'EXEC', 'EXECUTE', ';', '--', '/*', '*/', 'xp_', 'sp_'
        ]
        sanitized = text
        for pattern in dangerous_patterns:
            sanitized = sanitized.replace(pattern, '')
            sanitized = sanitized.replace(pattern.lower(), '')
        # Escape single quotes
        sanitized = sanitized.replace("'", "''")
        return sanitized
    
    async def generate(
        self,
        query: str,
        keywords: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Generate SQL query from natural language input.
        
        Args:
            query: Natural language query
            keywords: Optional pre-extracted keywords
            limit: Maximum number of results
            
        Returns:
            Dictionary with SQL query and metadata
        """
        # Extract keywords if not provided
        if not keywords:
            keywords = await self.keyword_extractor.extract(query)
        
        # Identify query type
        query_type = self.keyword_extractor.identify_query_type(query)
        
        app_logger.info(f"Generating SQL for query type: {query_type}")
        
        # Generate appropriate SQL based on query type
        if query_type == 'impact_analysis':
            sql = self._generate_impact_query(keywords, limit)
        elif query_type == 'trend_analysis':
            sql = self._generate_trend_query(keywords, limit)
        elif query_type == 'comparison':
            sql = self._generate_comparison_query(keywords, limit)
        elif query_type == 'ranking':
            sql = self._generate_ranking_query(keywords, limit)
        elif query_type == 'location_based':
            sql = self._generate_location_query(keywords, limit)
        elif query_type == 'aggregation':
            sql = self._generate_aggregation_query(keywords, limit)
        else:
            sql = self._generate_general_query(keywords, limit)
        
        return {
            'sql': sql,
            'query_type': query_type,
            'keywords': keywords,
            'tables_used': self._identify_tables(keywords),
            'limit': limit
        }
    
    def _identify_tables(self, keywords: List[str]) -> List[str]:
        """Identify which tables to query based on keywords."""
        tables = []
        keyword_lower = [k.lower() for k in keywords]
        
        # Check for farm-related keywords
        if any(k in ['farm', 'farms', 'farmer', 'owner', 'location', 'crop', 'certification']
               for k in keyword_lower):
            tables.append('farms')
        
        # Check for equipment keywords
        if any(k in ['equipment', 'tractor', 'harvester', 'machinery', 'maintenance']
               for k in keyword_lower):
            tables.append('equipment')
        
        # Check for supplier keywords
        if any(k in ['supplier', 'supply', 'distributor', 'vendor', 'delivery']
               for k in keyword_lower):
            tables.append('suppliers')
        
        # Check for production keywords
        if any(k in ['production', 'yield', 'harvest', 'revenue', 'profit']
               for k in keyword_lower):
            tables.append('production_records')
        
        # Check for weather keywords
        if any(k in ['weather', 'drought', 'flood', 'storm', 'climate']
               for k in keyword_lower):
            tables.append('weather_events')
        
        # Default to farms if no specific table identified
        if not tables:
            tables = ['farms']
        
        return tables
    
    def _generate_impact_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for impact analysis."""
        # Example: Impact of drought on corn production
        if 'drought' in keywords or 'weather' in keywords:
            return f"""
                SELECT 
                    f.name as farm_name,
                    f.primary_crop,
                    pr.year,
                    pr.yield_per_acre,
                    pr.weather_impact,
                    pr.revenue,
                    we.type as weather_event,
                    we.severity,
                    we.estimated_damage
                FROM farms f
                JOIN production_records pr ON f.id = pr.farm_id
                LEFT JOIN weather_events we ON 
                    pr.year = EXTRACT(YEAR FROM we.date) AND
                    f.county = we.affected_region
                WHERE pr.weather_impact IS NOT NULL
                ORDER BY we.severity DESC, pr.revenue DESC
                LIMIT {limit}
            """
        else:
            return self._generate_general_query(keywords, limit)
    
    def _generate_trend_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for trend analysis."""
        # Check if we're analyzing production trends
        tables = self._identify_tables(keywords)
        
        if 'production_records' in tables:
            return f"""
                SELECT 
                    pr.year,
                    pr.crop_type,
                    COUNT(DISTINCT pr.farm_id) as farm_count,
                    AVG(pr.yield_per_acre) as avg_yield,
                    SUM(pr.total_production) as total_production,
                    AVG(pr.revenue) as avg_revenue
                FROM production_records pr
                JOIN farms f ON pr.farm_id = f.id
                GROUP BY pr.year, pr.crop_type
                ORDER BY pr.year DESC, total_production DESC
                LIMIT {limit}
            """
        else:
            # Fallback to state metrics with aggregation
            where_conditions = []
            for keyword in keywords:
                if keyword.lower() in ['iowa', 'california', 'texas', 'nebraska', 'kansas']:
                    where_conditions.append(f"LOWER(place_name) = LOWER('{keyword}')")
            
            where_clause = " OR ".join(where_conditions) if where_conditions else "1=1"
            
            return f"""
                SELECT 
                    place_name,
                    year,
                    metric_type,
                    AVG(value) as avg_value,
                    COUNT(*) as data_points
                FROM state_agricultural_metrics
                WHERE ({where_clause})
                    AND year >= EXTRACT(YEAR FROM CURRENT_DATE) - 10
                GROUP BY place_name, year, metric_type
                ORDER BY year DESC, avg_value DESC
                LIMIT {limit}
            """
    
    def _generate_comparison_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for comparisons."""
        # Example: Compare organic vs conventional farms
        if 'organic' in keywords or 'conventional' in keywords:
            return f"""
                SELECT 
                    f.certification_type,
                    COUNT(*) as farm_count,
                    AVG(f.size_acres) as avg_size,
                    AVG(pr.yield_per_acre) as avg_yield,
                    AVG(pr.revenue) as avg_revenue
                FROM farms f
                LEFT JOIN production_records pr ON f.id = pr.farm_id
                WHERE f.certification_type IN ('organic', 'conventional')
                GROUP BY f.certification_type
                ORDER BY avg_revenue DESC
                LIMIT {limit}
            """
        else:
            return self._generate_general_query(keywords, limit)
    
    def _generate_ranking_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for rankings."""
        # Rank farms by performance metrics
        return f"""
            SELECT 
                f.name as farm_name,
                f.location,
                f.primary_crop,
                f.size_acres,
                AVG(pr.yield_per_acre) as avg_yield,
                SUM(pr.revenue) as total_revenue,
                COUNT(e.id) as equipment_count
            FROM farms f
            LEFT JOIN production_records pr ON f.id = pr.farm_id
            LEFT JOIN equipment e ON f.id = e.farm_id
            GROUP BY f.id, f.name, f.location, f.primary_crop, f.size_acres
            ORDER BY total_revenue DESC NULLS LAST
            LIMIT {limit}
        """
    
    def _generate_location_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for location-based searches."""
        # Check for distance-based keywords
        has_distance = any(k.lower() in ['miles', 'km', 'within', 'near', 'nearby'] for k in keywords)
        
        # Look for any location names (not just hardcoded states)
        location_keywords = []
        for keyword in keywords:
            # Any capitalized word or known state could be a location
            if (keyword[0].isupper() and len(keyword) > 2) or keyword.lower() in ['iowa', 'california', 'texas', 'nebraska', 'kansas', 'illinois', 'ohio', 'missouri']:
                location_keywords.append(keyword)
        
        if has_distance:
            # Distance-based search
            distance = 50  # Default to 50 miles
            for keyword in keywords:
                if keyword.isdigit():
                    distance = int(keyword)
                    break
            
            if location_keywords:
                # Distance from a specific location
                location = self._sanitize_input(location_keywords[0])
                return f"""
                    SELECT 
                        f.name as farm_name,
                        f.location,
                        f.county,
                        f.state,
                        f.primary_crop,
                        f.size_acres
                    FROM farms f
                    WHERE (LOWER(f.state) = LOWER('{location}')
                       OR LOWER(f.county) LIKE LOWER('%{location}%')
                       OR LOWER(f.location) LIKE LOWER('%{location}%'))
                    ORDER BY f.size_acres DESC
                    LIMIT {limit}
                """
            else:
                # General spatial query - find farms within distance of each other
                return f"""
                    SELECT 
                        f1.name as farm_name,
                        f1.location,
                        f1.county,
                        f1.state,
                        f1.primary_crop,
                        f1.size_acres,
                        ST_Distance(
                            ST_MakePoint(f1.longitude, f1.latitude)::geography,
                            ST_MakePoint(f2.longitude, f2.latitude)::geography
                        ) / 1609.344 as distance_miles
                    FROM farms f1, farms f2
                    WHERE f1.id != f2.id
                        AND ST_DWithin(
                            ST_MakePoint(f1.longitude, f1.latitude)::geography,
                            ST_MakePoint(f2.longitude, f2.latitude)::geography,
                            {distance * 1609.344}  -- Convert miles to meters
                        )
                    ORDER BY distance_miles ASC
                    LIMIT {limit}
                """
        elif location_keywords:
            # Location-based search by state/county
            location = self._sanitize_input(location_keywords[0])
            return f"""
                SELECT 
                    f.name,
                    f.location,
                    f.county,
                    f.state,
                    f.primary_crop,
                    f.size_acres
                FROM farms f
                WHERE LOWER(f.state) = LOWER('{location}')
                   OR LOWER(f.county) LIKE LOWER('%{location}%')
                   OR LOWER(f.location) LIKE LOWER('%{location}%')
                ORDER BY f.size_acres DESC
                LIMIT {limit}
            """
        else:
            return self._generate_general_query(keywords, limit)
    
    def _generate_aggregation_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for aggregations and counts."""
        return f"""
            SELECT 
                f.state,
                f.primary_crop,
                COUNT(DISTINCT f.id) as farm_count,
                SUM(f.size_acres) as total_acres,
                AVG(f.size_acres) as avg_farm_size,
                COUNT(DISTINCT s.id) as supplier_count,
                COUNT(DISTINCT e.id) as equipment_count
            FROM farms f
            LEFT JOIN farm_suppliers fs ON f.id = fs.farm_id
            LEFT JOIN suppliers s ON fs.supplier_id = s.id
            LEFT JOIN equipment e ON f.id = e.farm_id
            GROUP BY f.state, f.primary_crop
            ORDER BY farm_count DESC
            LIMIT {limit}
        """
    
    def _generate_general_query(self, keywords: List[str], limit: int) -> str:
        """Generate a general search query."""
        # Identify which tables to use based on keywords
        tables = self._identify_tables(keywords)
        
        # If multiple tables are involved, create JOINs
        if len(tables) > 1 and 'farms' in tables:
            select_fields = ["f.id", "f.name", "f.location", "f.primary_crop", "f.size_acres"]
            from_clause = "farms f"
            
            if 'equipment' in tables:
                from_clause += " LEFT JOIN equipment e ON f.id = e.farm_id"
                select_fields.append("COUNT(DISTINCT e.id) as equipment_count")
            
            if 'suppliers' in tables:
                from_clause += " LEFT JOIN farm_suppliers fs ON f.id = fs.farm_id"
                from_clause += " LEFT JOIN suppliers s ON fs.supplier_id = s.id"
                select_fields.append("COUNT(DISTINCT s.id) as supplier_count")
            
            if 'production_records' in tables:
                from_clause += " LEFT JOIN production_records pr ON f.id = pr.farm_id"
                select_fields.append("AVG(pr.yield_per_acre) as avg_yield")
            
            where_conditions = []
            for keyword in keywords[:5]:
                if keyword.lower() not in ['impact', 'trend', 'comparison', 'location', 'quantity']:
                    where_conditions.append(
                        f"(LOWER(f.name) LIKE LOWER('%{keyword}%') OR "
                        f"LOWER(f.location) LIKE LOWER('%{keyword}%'))"
                    )
            
            where_clause = " OR ".join(where_conditions) if where_conditions else "1=1"
            
            return f"""
                SELECT {', '.join(select_fields)}
                FROM {from_clause}
                WHERE {where_clause}
                GROUP BY f.id, f.name, f.location, f.primary_crop, f.size_acres
                ORDER BY f.size_acres DESC
                LIMIT {limit}
            """
        # If farms table is identified, use it
        elif 'farms' in tables:
            where_conditions = []
            for keyword in keywords[:5]:  # Limit to 5 keywords
                if keyword.lower() not in ['impact', 'trend', 'comparison', 'location', 'quantity']:
                    where_conditions.append(
                        f"(LOWER(f.name) LIKE LOWER('%{keyword}%') OR "
                        f"LOWER(f.location) LIKE LOWER('%{keyword}%') OR "
                        f"LOWER(f.primary_crop) LIKE LOWER('%{keyword}%') OR "
                        f"LOWER(f.state) LIKE LOWER('%{keyword}%'))"
                    )
            
            where_clause = " OR ".join(where_conditions) if where_conditions else "1=1"
            
            return f"""
                SELECT 
                    f.id,
                    f.name,
                    f.location,
                    f.primary_crop,
                    f.size_acres,
                    f.state,
                    f.county
                FROM farms f
                WHERE {where_clause}
                ORDER BY f.size_acres DESC
                LIMIT {limit}
            """
        else:
            # Fallback to state_agricultural_metrics for general data
            where_conditions = []
            for keyword in keywords[:5]:  # Limit to 5 keywords
                if keyword.lower() not in ['impact', 'trend', 'comparison', 'location', 'quantity']:
                    where_conditions.append(
                        f"(LOWER(place_name) LIKE LOWER('%{keyword}%') OR "
                        f"LOWER(metric_type) LIKE LOWER('%{keyword}%'))"
                    )
            
            where_clause = " OR ".join(where_conditions) if where_conditions else "1=1"
            
            return f"""
                SELECT 
                    place_name,
                    year,
                    metric_type,
                    value,
                    source
                FROM state_agricultural_metrics
                WHERE {where_clause}
                ORDER BY year DESC, value DESC
                LIMIT {limit}
            """
    
    def explain_query(self, sql: str) -> str:
        """
        Generate a human-readable explanation of the SQL query.
        
        Args:
            sql: The SQL query to explain
            
        Returns:
            Human-readable explanation
        """
        explanation = "This query "
        
        if "AVG(" in sql or "SUM(" in sql or "COUNT(" in sql:
            explanation += "aggregates data "
        
        if "JOIN" in sql:
            if "production_records" in sql:
                explanation += "including production history "
            if "equipment" in sql:
                explanation += "including equipment information "
            if "suppliers" in sql:
                explanation += "including supplier relationships "
            if "weather_events" in sql:
                explanation += "including weather impact data "
        
        if "WHERE" in sql and "WHERE 1=1" not in sql:
            explanation += "with specific filtering conditions "
        
        if "GROUP BY" in sql:
            explanation += "grouped by key attributes "
        
        if "ORDER BY" in sql:
            if "DESC" in sql:
                explanation += "sorted in descending order "
            else:
                explanation += "sorted in ascending order "
        
        return explanation.strip()