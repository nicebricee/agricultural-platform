"""
Cypher query generator for Neo4j graph database.
Converts natural language keywords to optimized Cypher queries.
"""

from typing import List, Dict, Any, Optional
from app.core.logging import app_logger
from app.services.keyword_extractor import KeywordExtractor
from app.services.relationship_builder import RelationshipBuilder


class CypherQueryGenerator:
    """Generates Cypher queries for Neo4j based on keywords and query type."""
    
    def __init__(self):
        """Initialize the Cypher query generator."""
        self.keyword_extractor = KeywordExtractor()
        self.relationship_builder = RelationshipBuilder()
        
        # Node types and their properties - Based on actual Neo4j schema
        self.node_types = {
            'State': {
                'properties': [
                    'name', 'abbreviation', 'fips_code', 'population',
                    'area_sq_miles', 'capital', 'largest_city'
                ]
            },
            'Measurement': {
                'properties': [
                    'metric_type', 'year', 'value', 'unit',
                    'source', 'confidence_level'
                ]
            },
            'Region': {
                'properties': [
                    'name', 'type', 'states_count'
                ]
            },
            'Climate': {
                'properties': [
                    'name', 'type', 'avg_temperature', 'avg_rainfall'
                ]
            },
            'AgriculturalBelt': {
                'properties': [
                    'name', 'primary_crops', 'states_included'
                ]
            },
            'Year': {
                'properties': [
                    'year', 'is_census_year', 'major_events'
                ]
            }
        }
        
        # Relationship types - Based on actual Neo4j schema
        self.relationship_types = {
            'HAS_MEASUREMENT': 'State has agricultural measurement',
            'IN_REGION': 'State is in geographic region',
            'HAS_CLIMATE': 'State has climate type',
            'IN_BELT': 'State is in agricultural belt',
            'BORDERS': 'State borders another state',
            'IN_YEAR': 'Measurement recorded in year',
            'COMPARED_TO': 'Measurement compared to another',
            'INFLUENCES': 'Climate influences measurements',
            'CORRELATES_WITH': 'Measurements correlate with each other'
        }
    
    def _sanitize_input(self, text: str) -> str:
        """Sanitize input to prevent Cypher injection."""
        # Remove dangerous Cypher keywords and special characters
        dangerous_patterns = [
            'DELETE', 'DETACH', 'DROP', 'CREATE', 'MERGE', 'SET',
            'REMOVE', ';', '//', '/*', '*/', '--'
        ]
        sanitized = text
        for pattern in dangerous_patterns:
            sanitized = sanitized.replace(pattern, '')
            sanitized = sanitized.replace(pattern.lower(), '')
        # Escape single quotes
        sanitized = sanitized.replace("'", "\\'")
        return sanitized
    
    async def generate(
        self,
        query: str,
        keywords: Optional[List[str]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Generate Cypher query from natural language input.
        
        Args:
            query: Natural language query
            keywords: Optional pre-extracted keywords
            limit: Maximum number of results
            
        Returns:
            Dictionary with Cypher query and metadata
        """
        # Extract keywords if not provided
        if not keywords:
            keywords = await self.keyword_extractor.extract(query)
        
        # Identify query type
        query_type = self.keyword_extractor.identify_query_type(query)
        
        app_logger.info(f"Generating Cypher for query type: {query_type}")
        app_logger.info(f"Keywords extracted: {keywords}")
        
        # Generate appropriate Cypher based on query type
        if query_type == 'impact_analysis':
            cypher = self._generate_impact_query(keywords, limit)
        elif query_type == 'trend_analysis':
            cypher = self._generate_trend_query(keywords, limit)
        elif query_type == 'comparison':
            cypher = self._generate_comparison_query(keywords, limit)
        elif query_type == 'relationship':
            cypher = self._generate_relationship_query(keywords, limit)
        elif query_type == 'location_based':
            cypher = self._generate_location_query(keywords, limit)
        elif query_type == 'aggregation':
            cypher = self._generate_aggregation_query(keywords, limit)
        else:
            cypher = self._generate_general_query(keywords, limit)
        
        app_logger.info(f"Generated Cypher query: {cypher[:500]}...")  # Log first 500 chars
        
        return {
            'cypher': cypher,
            'query_type': query_type,
            'keywords': keywords,
            'nodes_involved': self._identify_nodes(keywords),
            'limit': limit
        }
    
    def _identify_nodes(self, keywords: List[str]) -> List[str]:
        """Identify which node types to query based on keywords."""
        nodes = []
        keyword_lower = [k.lower() for k in keywords]
        
        # Always include State and Measurement as primary nodes
        nodes.append('State')
        nodes.append('Measurement')
        
        # Check for region keywords
        if any(k in ['region', 'regional', 'midwest', 'south', 'west', 'northeast']
               for k in keyword_lower):
            nodes.append('Region')
        
        # Check for climate keywords
        if any(k in ['climate', 'weather', 'temperature', 'rainfall']
               for k in keyword_lower):
            nodes.append('Climate')
        
        # Check for agricultural belt keywords
        if any(k in ['belt', 'corn belt', 'wheat belt', 'cotton belt']
               for k in keyword_lower):
            nodes.append('AgriculturalBelt')
        
        # Check for year/time keywords
        if any(k in ['year', 'annual', 'yearly', 'trends', 'time']
               for k in keyword_lower):
            nodes.append('Year')
        
        return nodes
    
    def _generate_impact_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for impact analysis using state relationships."""
        # Check for multi-hop keywords
        hop_count = 3  # default
        for keyword in keywords:
            if keyword.isdigit():
                hop_count = int(keyword)
                break
        
        if any(k.lower() in ['chain', 'hops', 'path'] for k in keywords):
            # Multi-hop traversal query
            return f"""
                MATCH path = (s1:State)-[:BORDERS*1..{hop_count}]->(s2:State)
                WHERE s1 <> s2
                WITH s1, s2, path, length(path) as hops
                RETURN s1.name as origin_state,
                       s2.name as connected_state,
                       hops as connection_distance,
                       [node in nodes(path) | node.name] as path_states
                ORDER BY hops ASC
                LIMIT {limit}
            """
        else:
            # Standard impact analysis
            return f"""
                MATCH (s1:State)-[b:BORDERS]->(s2:State)
                MATCH (s1)-[r1:HAS_MEASUREMENT]->(m1:Measurement)
                MATCH (s2)-[r2:HAS_MEASUREMENT]->(m2:Measurement)
                WHERE m1.metric_type = m2.metric_type 
                      AND m1.year = m2.year
                      AND m1.year >= date().year - 5
                WITH s1, s2, b, r1, r2, m1, m2, m1.metric_type as metric, m1.year as year,
                     m1.value as origin_value, m2.value as neighbor_value,
                     abs(m1.value - m2.value) / (m1.value + 0.01) * 100 as pct_difference
                RETURN s1 as origin_state_node,
                       b as border_relationship,
                       s2 as neighbor_state_node,
                       r1 as measurement_rel1,
                       r2 as measurement_rel2,
                       m1 as measurement1_node,
                       m2 as measurement2_node,
                       s1.name as origin_state,
                       s2.name as neighboring_state,
                       metric,
                       year,
                       round(origin_value) as origin_value,
                       round(neighbor_value) as neighbor_value,
                       round(pct_difference, 2) as percent_difference
                ORDER BY year DESC, pct_difference DESC
                LIMIT {limit}
            """
    
    def _generate_trend_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for trend analysis over time."""
        # Extract year range from keywords
        import datetime
        current_year = datetime.datetime.now().year
        year_filter = ""
        
        # Check for temporal keywords
        for i, keyword in enumerate(keywords):
            keyword_lower = keyword.lower()
            
            if keyword_lower in ['past', 'last', 'previous', 'recent']:
                for j in range(i+1, min(i+3, len(keywords))):
                    if keywords[j].isdigit():
                        years_back = int(keywords[j])
                        year_filter = f" AND m.year >= {current_year - years_back} AND m.year <= {current_year}"
                        break
            elif keyword.isdigit() and len(keyword) == 4 and 1900 <= int(keyword) <= 2100:
                year_filter = f" AND m.year = {keyword}"
            elif 'decade' in keyword_lower:
                year_filter = f" AND m.year >= {current_year - 10} AND m.year <= {current_year}"
            elif keyword_lower == 'years' and i > 0 and keywords[i-1].isdigit():
                years_back = int(keywords[i-1])
                year_filter = f" AND m.year >= {current_year - years_back} AND m.year <= {current_year}"
        
        # Build dynamic WHERE clause for any state mentioned
        where_conditions = []
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if keyword_lower not in ['trend', 'trends', 'pattern', 'patterns', 'past', 'last', 'years', 
                                    'year', 'recent', 'previous', 'decade'] and not keyword_lower.isdigit():
                where_conditions.append(f"toLower(s.name) CONTAINS '{keyword_lower}'")
        
        where_clause = " OR ".join(where_conditions) if where_conditions else "1=1"
        where_clause = f"({where_clause}){year_filter}"
        
        return f"""
            MATCH (s:State)-[rel:HAS_MEASUREMENT]->(m:Measurement)
            WHERE {where_clause}
            WITH s, rel, m,
                 s.name as state, 
                 m.year as year,
                 avg(m.value) as avg_value,
                 collect(DISTINCT m.metric_type) as metrics
            RETURN s as state_node,
                   rel as relationship,
                   m as measurement_node,
                   state,
                   year,
                   round(avg_value) as average_value,
                   metrics[0..3] as sample_metrics
            ORDER BY year DESC
            LIMIT {limit}
        """
    
    def _generate_comparison_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for comparing states and their metrics."""
        # Dynamically build list of states to compare from keywords
        states_to_compare = []
        for keyword in keywords:
            # Any capitalized word could be a state name
            if keyword[0].isupper() and len(keyword) > 2:
                states_to_compare.append(keyword)
        
        if states_to_compare:
            # Compare specific states
            states_filter = "s.name IN ['" + "', '".join(states_to_compare) + "']"
            return f"""
                MATCH (s:State)-[:HAS_MEASUREMENT]->(m:Measurement)
                WHERE {states_filter}
                      AND m.year >= date().year - 3
                WITH s.name as state,
                     m.metric_type as metric,
                     avg(m.value) as avg_value,
                     count(m) as measurement_count
                RETURN state,
                       metric,
                       round(avg_value, 2) as average_value,
                       measurement_count
                ORDER BY metric, avg_value DESC
                LIMIT {limit}
            """
        else:
            # Compare top states by metrics
            return f"""
                MATCH (s:State)-[:HAS_MEASUREMENT]->(m:Measurement)
                WHERE m.year >= date().year - 3
                WITH s.name as state,
                     count(DISTINCT m.metric_type) as metric_count,
                     avg(m.value) as avg_value
                RETURN state,
                       metric_count,
                       round(avg_value, 2) as average_value
                ORDER BY avg_value DESC
                LIMIT {limit}
            """
    
    def _generate_relationship_query(self, keywords: List[str], limit: int) -> str:
        """Generate query exploring relationships in the graph."""
        # Check for specific relationship types in keywords
        if 'supplier' in [k.lower() for k in keywords]:
            return f"""
                MATCH (f:Farm)-[:CONTRACTS_WITH]->(s:Supplier)
                OPTIONAL MATCH (s)-[:DELIVERS_TO]->(f2:Farm)
                WITH f, s, count(DISTINCT f2) as other_farms
                RETURN f.name as farm_name,
                       s.name as supplier_name,
                       s.type as supplier_type,
                       s.reliability_score as reliability,
                       other_farms as also_supplies_to
                ORDER BY s.reliability_score DESC
                LIMIT {limit}
            """
        else:
            return f"""
                MATCH path = (f1:Farm)-[*1..3]-(f2:Farm)
                WHERE f1.id < f2.id  // Avoid duplicate paths
                WITH f1, f2, path,
                     length(path) as path_length,
                     [node in nodes(path) | labels(node)[0]] as node_types,
                     [rel in relationships(path) | type(rel)] as relationship_types
                RETURN f1.name as farm1,
                       f2.name as farm2,
                       path_length,
                       node_types,
                       relationship_types
                ORDER BY path_length ASC
                LIMIT {limit}
            """
    
    def _generate_location_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for location-based searches using spatial data."""
        location_keywords = [k for k in keywords 
                           if k.lower() in ['iowa', 'california', 'texas', 'nebraska', 'kansas']]
        
        if location_keywords:
            location = location_keywords[0]
            return f"""
                MATCH (f:Farm)
                WHERE toLower(f.state) = toLower('{location}')
                   OR toLower(f.county) CONTAINS toLower('{location}')
                OPTIONAL MATCH (f)-[:OWNS]->(e:Equipment)
                OPTIONAL MATCH (f)-[:CONNECTED_TO]-(neighbor:Farm)
                WITH f, count(DISTINCT e) as equipment_count,
                     count(DISTINCT neighbor) as neighbor_count
                RETURN f.name as farm_name,
                       f.location,
                       f.county,
                       f.state,
                       f.primary_crop,
                       f.size_acres,
                       equipment_count,
                       neighbor_count
                ORDER BY f.size_acres DESC
                LIMIT {limit}
            """
        
        # Find clusters of farms
        else:
            return f"""
                MATCH (f:Farm)
                OPTIONAL MATCH (f)-[:LOCATED_IN]->(region:Region)
                WITH region, collect(f) as farms_in_region
                WHERE size(farms_in_region) > 1
                RETURN region.name as region_name,
                       size(farms_in_region) as farm_count,
                       round(avg([f IN farms_in_region | f.size_acres]), 2) as avg_size,
                       [f IN farms_in_region | f.primary_crop][0..5] as top_crops
                ORDER BY farm_count DESC
                LIMIT {limit}
            """
    
    def _generate_aggregation_query(self, keywords: List[str], limit: int) -> str:
        """Generate query for aggregations using graph algorithms."""
        return f"""
            MATCH (f:Farm)
            OPTIONAL MATCH (f)-[:OWNS]->(e:Equipment)
            OPTIONAL MATCH (f)-[:SUPPLIES]->(s:Supplier)
            OPTIONAL MATCH (f)-[:GROWS]->(c:Crop)
            WITH f.state as state,
                 count(DISTINCT f) as farm_count,
                 sum(f.size_acres) as total_acres,
                 count(DISTINCT e) as equipment_count,
                 count(DISTINCT s) as supplier_count,
                 collect(DISTINCT c.type) as crop_types
            RETURN state,
                   farm_count,
                   round(total_acres, 0) as total_acres,
                   round(total_acres / farm_count, 2) as avg_farm_size,
                   equipment_count,
                   supplier_count,
                   size(crop_types) as crop_diversity
            ORDER BY farm_count DESC
            LIMIT {limit}
        """
    
    def _generate_general_query(self, keywords: List[str], limit: int) -> str:
        """Generate a general search query for agricultural state data."""
        # Check if this is a list/collection query
        has_list_keywords = any(k.lower() in ['list', 'all', 'each', 'collect'] for k in keywords)
        
        # Available years in our Neo4j database
        available_years = [1997, 2002, 2007, 2012, 2017, 2022]
        
        # Check for temporal keywords and extract year range
        import datetime
        current_year = datetime.datetime.now().year
        year_filter = ""
        selected_years = []
        
        # Check for "past X years" pattern
        for i, keyword in enumerate(keywords):
            keyword_lower = keyword.lower()
            
            # Check for "past/last X years" pattern
            if keyword_lower in ['past', 'last', 'previous', 'recent']:
                # Look for a number in the next keywords
                for j in range(i+1, min(i+3, len(keywords))):
                    if keywords[j].isdigit():
                        years_back = int(keywords[j])
                        # Map to available years intelligently
                        cutoff_year = current_year - years_back
                        selected_years = [y for y in available_years if y >= cutoff_year]
                        # If no years match, use the most recent available years
                        if not selected_years:
                            if years_back <= 5:
                                selected_years = [2022, 2017]  # Last 2 data points
                            elif years_back <= 10:
                                selected_years = [2022, 2017, 2012]  # Last 3 data points
                            else:
                                selected_years = available_years[-4:]  # Last 4 data points
                        year_filter = f" AND m.year IN {selected_years}"
                        break
            
            # Check for specific year mentions
            elif keyword.isdigit() and len(keyword) == 4 and 1900 <= int(keyword) <= 2100:
                year = int(keyword)
                # Map to nearest available year
                if year in available_years:
                    year_filter = f" AND m.year = {year}"
                else:
                    # Find nearest available year
                    nearest = min(available_years, key=lambda y: abs(y - year))
                    year_filter = f" AND m.year = {nearest}"
            
            # Check for decade patterns
            elif 'decade' in keyword_lower:
                # Use last 3 data points for decade analysis
                selected_years = available_years[-3:]
                year_filter = f" AND m.year IN {selected_years}"
            
            # Check for specific year range keywords
            elif keyword_lower == 'years' and i > 0:
                # Check if previous word is a number
                if keywords[i-1].isdigit():
                    years_back = int(keywords[i-1])
                    cutoff_year = current_year - years_back
                    selected_years = [y for y in available_years if y >= cutoff_year]
                    if not selected_years:
                        # Use appropriate number of recent years
                        num_years = min(3, len(available_years))
                        selected_years = available_years[-num_years:]
                    year_filter = f" AND m.year IN {selected_years}"
        
        # Define regions for better state matching
        regions = {
            'midwest': ['Iowa', 'Illinois', 'Indiana', 'Michigan', 'Minnesota', 
                       'Missouri', 'Ohio', 'Wisconsin', 'Kansas', 'Nebraska', 
                       'North Dakota', 'South Dakota'],
            'northeast': ['Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 
                         'New Jersey', 'New York', 'Pennsylvania', 'Rhode Island', 'Vermont'],
            'south': ['Alabama', 'Arkansas', 'Delaware', 'Florida', 'Georgia', 'Kentucky',
                     'Louisiana', 'Maryland', 'Mississippi', 'North Carolina', 'Oklahoma',
                     'South Carolina', 'Tennessee', 'Texas', 'Virginia', 'West Virginia'],
            'west': ['Alaska', 'Arizona', 'California', 'Colorado', 'Hawaii', 'Idaho',
                    'Montana', 'Nevada', 'New Mexico', 'Oregon', 'Utah', 'Washington', 'Wyoming']
        }
        
        # Build WHERE conditions dynamically based on keywords
        where_conditions = []
        state_list = []
        
        for keyword in keywords[:5]:  # Limit to 5 keywords
            # Sanitize keyword to prevent injection
            keyword_safe = self._sanitize_input(keyword.lower())
            
            # Check for regional keywords
            if keyword_safe in regions:
                state_list.extend(regions[keyword_safe])
                continue
            
            # Skip generic keywords and temporal keywords
            if keyword_safe in ['impact', 'trend', 'comparison', 'location', 'quantity', 'list', 'all',
                               'past', 'last', 'years', 'year', 'recent', 'previous', 'decade',
                               'farms', 'farm', 'performances', 'performance']:
                continue
            # Skip numbers (likely years)
            if keyword_safe.isdigit():
                continue
            # Dynamically search in state names and metric types
            where_conditions.append(
                f"(toLower(s.name) CONTAINS '{keyword_safe}' OR "
                f"toLower(m.metric_type) CONTAINS '{keyword_safe}')"
            )
        
        # If we have a state list from regions, add it to WHERE
        if state_list:
            state_names = "', '".join(state_list)
            where_conditions.append(f"s.name IN ['{state_names}']")
        
        # Build WHERE clause
        if where_conditions:
            where_clause = " OR ".join(where_conditions)
        else:
            where_clause = "1=1"
        
        # Add year filter to where clause
        where_clause = f"({where_clause}){year_filter}"
        
        if has_list_keywords:
            # Return aggregated/collected results
            return f"""
                MATCH (s:State)-[:HAS_MEASUREMENT]->(m:Measurement)
                WHERE {where_clause}
                WITH s.name as state,
                     COLLECT(DISTINCT m.metric_type) as metrics,
                     COLLECT(DISTINCT m.year) as years,
                     COUNT(m) as measurement_count
                RETURN state, metrics, years, measurement_count
                ORDER BY measurement_count DESC
                LIMIT {limit}
            """
        else:
            # Enhanced query with temporal and meaningful relationships
            # If we have multiple years selected, include temporal analysis
            if selected_years and len(selected_years) > 1:
                years_str = ', '.join(map(str, selected_years))
                return f"""
                    // Fetch measurements for multiple years
                    MATCH (s:State)-[rel:HAS_MEASUREMENT]->(m:Measurement)
                    WHERE {where_clause}
                    
                    // Get historical data for same state and metric
                    WITH s, m, rel
                    OPTIONAL MATCH (s)-[:HAS_MEASUREMENT]->(hist:Measurement)
                    WHERE hist.metric_type = m.metric_type AND hist.year < m.year
                          AND hist.year IN [{years_str}]
                    
                    // Get comparison data from other states
                    WITH s, m, rel, collect(hist) as history
                    OPTIONAL MATCH (s2:State)-[:HAS_MEASUREMENT]->(m2:Measurement)
                    WHERE s2.name <> s.name AND m2.metric_type = m.metric_type AND m2.year = m.year
                    
                    WITH s, m, rel, history,
                         // Calculate year-over-year growth
                         CASE 
                             WHEN size(history) > 0 AND history[0].value > 0
                             THEN round((m.value - history[0].value) / history[0].value * 100, 2)
                             ELSE null
                         END as growth_rate,
                         // Economic comparisons
                         CASE 
                             WHEN m.metric_type CONTAINS 'Income' AND m.value > m2.value * 1.2 
                             THEN {{type: 'HIGHER_INCOME_THAN', target: s2.name, diff: m.value - m2.value}}
                             WHEN m.metric_type CONTAINS 'Expense' AND m.value < m2.value * 0.8
                             THEN {{type: 'MORE_EFFICIENT_THAN', target: s2.name, diff: m2.value - m.value}}
                             ELSE null
                         END as economic_rel,
                         collect(DISTINCT s2) as compared_states,
                         // Historical trend
                         CASE
                             WHEN size(history) > 0 AND m.value > history[0].value * 1.1
                             THEN 'GROWING'
                             WHEN size(history) > 0 AND m.value < history[0].value * 0.9
                             THEN 'DECLINING'
                             ELSE 'STABLE'
                         END as trend
                    
                    // Get actual geographic relationships from database
                    WITH s, m, rel, growth_rate, economic_rel, compared_states, trend, history
                    OPTIONAL MATCH (s)-[:BORDERS]->(border:State)
                    WITH s, m, rel, growth_rate, economic_rel, compared_states, trend, history,
                         collect(DISTINCT border.name) as borders,
                         CASE 
                             WHEN s.name IN ['Iowa', 'Illinois', 'Indiana', 'Ohio', 'Nebraska', 'Minnesota', 'Wisconsin'] THEN 'CORN_BELT'
                             WHEN s.name IN ['Kansas', 'Oklahoma', 'Texas', 'Nebraska', 'Colorado'] THEN 'WHEAT_BELT'
                             WHEN s.name IN ['Texas', 'Georgia', 'Mississippi', 'Arkansas', 'Louisiana', 'Alabama'] THEN 'COTTON_BELT'
                             ELSE 'OTHER'
                         END as ag_belt
                    
                    RETURN s as state_node,
                           rel as measurement_rel,
                           m as measurement_node,
                           s.name as state,
                           m.metric_type as metric,
                           m.year as year,
                           m.value as value,
                           growth_rate as year_over_year_growth,
                           trend as performance_trend,
                           size(history) as historical_data_points,
                           [h in history | {{year: h.year, value: h.value}}][0..3] as previous_values,
                           borders as border_states,
                           ag_belt as agricultural_belt,
                           economic_rel as economic_relationship,
                           size(compared_states) as compared_to_count
                    ORDER BY m.year DESC, m.value DESC
                    LIMIT {limit}
                """
            else:
                # Single year query with standard relationships
                return f"""
                    MATCH (s:State)-[rel:HAS_MEASUREMENT]->(m:Measurement)
                    WHERE {where_clause}
                    WITH s, m, rel
                    
                    // Create dynamic relationships based on data
                    OPTIONAL MATCH (s2:State)-[:HAS_MEASUREMENT]->(m2:Measurement)
                    WHERE s2.name <> s.name AND m2.metric_type = m.metric_type AND m2.year = m.year
                    
                    WITH s, m, rel, 
                         CASE 
                             WHEN m.metric_type CONTAINS 'Income' AND m.value > m2.value * 1.2 
                             THEN {{type: 'HIGHER_INCOME_THAN', target: s2.name, diff: m.value - m2.value}}
                             WHEN m.metric_type CONTAINS 'Expense' AND m.value < m2.value * 0.8
                             THEN {{type: 'MORE_EFFICIENT_THAN', target: s2.name, diff: m2.value - m.value}}
                             ELSE null
                         END as economic_rel,
                         collect(DISTINCT s2) as compared_states
                
                // Get actual geographic relationships from database
                WITH s, m, rel, economic_rel, compared_states
                OPTIONAL MATCH (s)-[:BORDERS]->(border:State)
                WITH s, m, rel, economic_rel, compared_states,
                     collect(DISTINCT border.name) as borders,
                     CASE 
                         WHEN s.name IN ['Iowa', 'Illinois', 'Indiana', 'Ohio', 'Nebraska', 'Minnesota', 'Wisconsin'] THEN 'CORN_BELT'
                         WHEN s.name IN ['Kansas', 'Oklahoma', 'Texas', 'Nebraska', 'Colorado'] THEN 'WHEAT_BELT'
                         WHEN s.name IN ['Texas', 'Georgia', 'Mississippi', 'Arkansas', 'Louisiana', 'Alabama'] THEN 'COTTON_BELT'
                         ELSE 'OTHER'
                     END as ag_belt
                
                RETURN s as state_node,
                       rel as measurement_rel,
                       m as measurement_node,
                       s.name as state,
                       m.metric_type as metric,
                       m.year as year,
                       m.value as value,
                       borders as border_states,
                       ag_belt as agricultural_belt,
                       economic_rel as economic_relationship,
                       size(compared_states) as compared_to_count
                ORDER BY m.year DESC, m.value DESC
                LIMIT {limit}
            """
    
    def explain_query(self, cypher: str) -> str:
        """
        Generate a human-readable explanation of the Cypher query.
        
        Args:
            cypher: The Cypher query to explain
            
        Returns:
            Human-readable explanation
        """
        explanation = "This graph query "
        
        if "MATCH" in cypher:
            if "*" in cypher:
                explanation += "traverses multiple relationship paths "
            else:
                explanation += "matches graph patterns "
        
        if "OPTIONAL MATCH" in cypher:
            explanation += "with optional relationships "
        
        if "-[:OWNS]->" in cypher:
            explanation += "including equipment ownership "
        if "-[:SUPPLIES]->" in cypher:
            explanation += "including supplier relationships "
        if "-[:GROWS]->" in cypher:
            explanation += "including crop cultivation "
        if "-[:AFFECTED_BY]->" in cypher:
            explanation += "including weather impacts "
        if "-[:CONNECTED_TO]-" in cypher:
            explanation += "exploring farm connections "
        
        if "count(" in cypher.lower() or "sum(" in cypher.lower() or "avg(" in cypher.lower():
            explanation += "with aggregations "
        
        if "collect(" in cypher.lower():
            explanation += "collecting related entities "
        
        if "WHERE" in cypher:
            explanation += "filtered by conditions "
        
        if "ORDER BY" in cypher:
            if "DESC" in cypher:
                explanation += "sorted in descending order "
            else:
                explanation += "sorted in ascending order "
        
        return explanation.strip()