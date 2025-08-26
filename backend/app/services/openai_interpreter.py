"""
OpenAI GPT-4 interpreter service for analyzing and comparing query results.
Provides intelligent insights from SQL and Cypher query results.
"""

from typing import Dict, Any, List, Optional, AsyncIterator
import json
import httpx
from openai import AsyncOpenAI
from app.core.config import settings
from app.core.logging import app_logger
from app.utils.table_formatter import format_results_with_tables


class OpenAIInterpreter:
    """Interprets and analyzes database query results using GPT-4."""
    
    def __init__(self):
        """Initialize the OpenAI interpreter."""
        self.enabled = bool(settings.openai_api_key)
        
        if self.enabled:
            # Configure timeout: 120 seconds total, 10 seconds for connection
            timeout_config = httpx.Timeout(120.0, connect=10.0)
            self.client = AsyncOpenAI(
                api_key=settings.openai_api_key,
                timeout=timeout_config,
                max_retries=2  # Add retries for resilience
            )
            self.model = settings.openai_model or "gpt-4-turbo-preview"
            self.max_tokens = 4000  # Max for gpt-4-turbo-preview is 4096
            app_logger.info(f"OpenAI interpreter initialized with model {self.model}, max_tokens {self.max_tokens}, timeout 120s")
        else:
            self.client = None
            self.model = None
            self.max_tokens = None
            app_logger.warning("OpenAI API key not configured - AI interpretation disabled")
    
    async def interpret_results_stream(
        self,
        query: str,
        sql_results: Optional[List[Dict]] = None,
        cypher_results: Optional[List[Dict]] = None,
        sql_performance: Optional[Dict] = None,
        cypher_performance: Optional[Dict] = None
    ) -> AsyncIterator[str]:
        """
        Stream interpretation results in real-time.
        
        Yields partial results as they're generated.
        """
        if not self.enabled:
            yield json.dumps({
                'sql_interpretation': "AI interpretation is not available (OpenAI API key not configured)",
                'graph_interpretation': "AI interpretation is not available (OpenAI API key not configured)",
                'comparison': '',
                'error': "OpenAI API key not configured"
            })
            return
        
        app_logger.info("Starting streaming interpretation with GPT-4")
        
        # Build the context for GPT-4
        context = self._build_context(
            query, sql_results, cypher_results,
            sql_performance, cypher_performance
        )
        
        try:
            # Create streaming response
            stream = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_system_prompt()
                    },
                    {
                        "role": "user",
                        "content": context
                    }
                ],
                max_tokens=self.max_tokens,
                temperature=0.3,
                stream=True,  # Enable streaming
                timeout=120.0
            )
            
            accumulated_text = ""
            async for chunk in stream:
                if chunk.choices[0].delta.content is not None:
                    chunk_text = chunk.choices[0].delta.content
                    accumulated_text += chunk_text
                    # Yield each chunk as it arrives for real-time display
                    yield chunk_text
            
            # After streaming is complete, parse the accumulated text using section markers
            app_logger.debug(f"Streaming complete. Total response length: {len(accumulated_text)}")
            
            import re
            
            # Extract SQL interpretation
            sql_match = re.search(r'\[START SQL INTERPRETATION\](.*?)\[END SQL INTERPRETATION\]', 
                                 accumulated_text, re.DOTALL)
            sql_interp = sql_match.group(1).strip() if sql_match else ""
            
            # Extract Graph interpretation
            graph_match = re.search(r'\[START GRAPH INTERPRETATION\](.*?)\[END GRAPH INTERPRETATION\]', 
                                   accumulated_text, re.DOTALL)
            graph_interp = graph_match.group(1).strip() if graph_match else ""
            
            # Extract Comparison
            comparison_match = re.search(r'\[START COMPARISON\](.*?)\[END COMPARISON\]', 
                                        accumulated_text, re.DOTALL)
            comparison = comparison_match.group(1).strip() if comparison_match else ""
            
            # Fallback if section markers aren't found
            if not sql_interp and not graph_interp:
                app_logger.warning("Section markers not found in streaming response, using fallback parsing")
                # Try to split the text intelligently
                if "SQL" in accumulated_text and "Graph" in accumulated_text:
                    # Find the Graph section
                    graph_start = max(
                        accumulated_text.find("Graph Database"),
                        accumulated_text.find("Neo4j"),
                        accumulated_text.find("Knowledge Graph")
                    )
                    if graph_start > 0:
                        sql_interp = accumulated_text[:graph_start].strip()
                        graph_interp = accumulated_text[graph_start:].strip()
                    else:
                        # Split evenly if no clear boundary
                        mid_point = len(accumulated_text) // 2
                        sql_interp = accumulated_text[:mid_point].strip()
                        graph_interp = accumulated_text[mid_point:].strip()
                else:
                    # Use full text for both as last resort
                    sql_interp = accumulated_text
                    graph_interp = accumulated_text
            
            # Ensure graph interpretation is distinct
            if graph_interp == sql_interp and len(graph_interp) > 100:
                app_logger.warning("Graph and SQL interpretations are identical in stream, attempting to differentiate")
                graph_interp = f"Graph Database Analysis: {graph_interp}"
            
            app_logger.info(f"Parsed streaming interpretations - SQL: {len(sql_interp)} chars, Graph: {len(graph_interp)} chars")
            
            # Yield the final structured response as JSON
            final_response = {
                'sql_interpretation': sql_interp,
                'graph_interpretation': graph_interp,
                'comparison': comparison
            }
            yield json.dumps(final_response)
            app_logger.debug("Successfully yielded structured response")
            
        except httpx.TimeoutException as e:
            app_logger.error(f"Streaming timed out: {e}")
            yield json.dumps({
                'error': 'Timeout: Request took longer than 120 seconds'
            })
        except Exception as e:
            app_logger.error(f"Streaming failed: {e}")
            yield json.dumps({
                'error': f'Streaming error: {str(e)}'
            })
    
    async def interpret_results(
        self,
        query: str,
        sql_results: Optional[List[Dict]] = None,
        cypher_results: Optional[List[Dict]] = None,
        sql_performance: Optional[Dict] = None,
        cypher_performance: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Interpret and analyze query results from both databases.
        
        Args:
            query: Original natural language query
            sql_results: Results from Supabase SQL query
            cypher_results: Results from Neo4j Cypher query
            sql_performance: Performance metrics for SQL query
            cypher_performance: Performance metrics for Cypher query
            
        Returns:
            Dictionary with interpretation and insights
        """
        if not self.enabled:
            return {
                'sql_interpretation': "AI interpretation is not available (OpenAI API key not configured)",
                'graph_interpretation': "AI interpretation is not available (OpenAI API key not configured)",
                'comparison': '',
                'error': "OpenAI API key not configured"
            }
        
        app_logger.info("Interpreting query results with GPT-4")
        
        # Build the context for GPT-4
        context = self._build_context(
            query, sql_results, cypher_results,
            sql_performance, cypher_performance
        )
        
        # Generate interpretation
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_system_prompt()
                    },
                    {
                        "role": "user",
                        "content": context
                    }
                ],
                max_tokens=self.max_tokens,
                temperature=0.3,  # Lower temperature for more focused, factual analysis
                timeout=120.0  # Explicit timeout at API level too
            )
            
            interpretation_text = response.choices[0].message.content
            
            # Log for debugging
            app_logger.debug(f"Raw AI response length: {len(interpretation_text)} chars")
            app_logger.debug(f"Raw AI response: {interpretation_text[:1000]}..." if len(interpretation_text) > 1000 else interpretation_text)
            
            # Parse the response using section markers
            import re
            
            # Extract SQL interpretation
            sql_match = re.search(r'\[START SQL INTERPRETATION\](.*?)\[END SQL INTERPRETATION\]', 
                                 interpretation_text, re.DOTALL)
            sql_interp = sql_match.group(1).strip() if sql_match else ""
            
            # Extract Graph interpretation
            graph_match = re.search(r'\[START GRAPH INTERPRETATION\](.*?)\[END GRAPH INTERPRETATION\]', 
                                   interpretation_text, re.DOTALL)
            graph_interp = graph_match.group(1).strip() if graph_match else ""
            
            # Extract Comparison
            comparison_match = re.search(r'\[START COMPARISON\](.*?)\[END COMPARISON\]', 
                                        interpretation_text, re.DOTALL)
            comparison = comparison_match.group(1).strip() if comparison_match else ""
            
            # Fallback if section markers aren't found
            if not sql_interp and not graph_interp:
                app_logger.warning("Section markers not found in AI response, using fallback parsing")
                # Try to split the text intelligently
                if "SQL" in interpretation_text and "Graph" in interpretation_text:
                    # Find the Graph section
                    graph_start = max(
                        interpretation_text.find("Graph Database"),
                        interpretation_text.find("Neo4j"),
                        interpretation_text.find("Knowledge Graph")
                    )
                    if graph_start > 0:
                        sql_interp = interpretation_text[:graph_start].strip()
                        graph_interp = interpretation_text[graph_start:].strip()
                    else:
                        # Split evenly if no clear boundary
                        mid_point = len(interpretation_text) // 2
                        sql_interp = interpretation_text[:mid_point].strip()
                        graph_interp = interpretation_text[mid_point:].strip()
                else:
                    # Use full text for both as last resort
                    sql_interp = interpretation_text
                    graph_interp = interpretation_text
            
            # Ensure graph interpretation is distinct
            if graph_interp == sql_interp and len(graph_interp) > 100:
                app_logger.warning("Graph and SQL interpretations are identical, attempting to differentiate")
                graph_interp = f"Graph Database Analysis: {graph_interp}"
            
            app_logger.info(f"Parsed interpretations - SQL: {len(sql_interp)} chars, Graph: {len(graph_interp)} chars")
            
            return {
                'sql_interpretation': sql_interp,
                'graph_interpretation': graph_interp,
                'comparison': comparison,
                'model_used': self.model,
                'tokens_used': response.usage.total_tokens if response.usage else None
            }
            
        except httpx.TimeoutException as e:
            app_logger.error(f"OpenAI request timed out after 120 seconds: {e}")
            return {
                'sql_interpretation': "Request timed out. The AI service is taking longer than expected. Please try a simpler query or try again later.",
                'graph_interpretation': "Request timed out. The AI service is taking longer than expected. Please try a simpler query or try again later.",
                'comparison': '',
                'error': f'Timeout: OpenAI took longer than 120 seconds to respond. This usually happens with complex queries or high API load.'
            }
        except httpx.ConnectError as e:
            app_logger.error(f"Failed to connect to OpenAI API: {e}")
            return {
                'sql_interpretation': "Cannot connect to AI service. Please check your internet connection.",
                'graph_interpretation': "Cannot connect to AI service. Please check your internet connection.",
                'comparison': '',
                'error': f'Connection Error: Unable to reach OpenAI servers. Please check network connectivity.'
            }
        except Exception as e:
            app_logger.error(f"OpenAI interpretation failed: {e}")
            error_message = str(e)
            # Provide specific error messages for common issues
            if 'rate_limit' in error_message.lower():
                user_message = "API rate limit reached. Please wait a moment and try again."
            elif 'invalid_api_key' in error_message.lower() or 'unauthorized' in error_message.lower():
                user_message = "Invalid API key. Please check your OpenAI API key configuration."
            elif 'context_length_exceeded' in error_message.lower():
                user_message = "Query results too large for AI analysis. Try a more specific query."
            else:
                user_message = f"AI service error: {error_message[:1000]}" if len(error_message) > 1000 else f"AI service error: {error_message}"
            
            return {
                'sql_interpretation': user_message,
                'graph_interpretation': user_message,
                'comparison': '',
                'error': error_message
            }
    
    def _get_system_prompt(self) -> str:
        """Get the system prompt for GPT-4."""
        return """You are an expert agricultural data analyst analyzing agricultural data from the United States.

        You will receive ASCII tables showing query results about farms, agricultural income, expenses, and performance metrics.
        
        FORMAT YOUR RESPONSE WITH THESE EXACT SECTION MARKERS:
        
        [START SQL INTERPRETATION]
        Analyze the agricultural data (500+ words).
        Include the ASCII table in your response.
        Focus on the actual numbers, trends, and patterns in the data.
        Answer the user's query directly using specific values from the results.
        Discuss agricultural insights, regional patterns, and economic implications.
        DO NOT mention "SQL database" or explain database capabilities.
        [END SQL INTERPRETATION]
        
        [START GRAPH INTERPRETATION]
        Analyze the agricultural network data (500+ words).
        Include the ASCII table in your response.
        Focus on relationships between states, regions, and agricultural metrics.
        Highlight network effects, regional clustering, and interconnected patterns.
        If data is limited, discuss the agricultural relationships that exist in the region.
        DO NOT mention "graph database" or explain database capabilities.
        [END GRAPH INTERPRETATION]
        
        [START COMPARISON]
        Compare the insights from both analyses (300+ words).
        Focus on which analysis provides better agricultural insights for this query.
        Provide actionable recommendations for agricultural stakeholders.
        DO NOT mention database types or technologies.
        [END COMPARISON]
        
        BE DETAILED AND THOROUGH - Focus on agricultural insights, not database technology."""
    
    def _build_context(
        self,
        query: str,
        sql_results: Optional[List[Dict]],
        cypher_results: Optional[List[Dict]],
        sql_performance: Optional[Dict],
        cypher_performance: Optional[Dict]
    ) -> str:
        """Build context string for GPT-4."""
        context_parts = [f"User Query: {query}\n"]
        
        # Format results as ASCII tables
        tables = format_results_with_tables(sql_results, cypher_results)
        
        # Add first dataset results with ASCII table
        context_parts.append("\n=== DATASET 1: State-Level Agricultural Metrics ===")
        context_parts.append("\nASCII TABLE:")
        context_parts.append(tables['sql_table'])
        
        # Only include limited raw data to avoid overwhelming the context
        if sql_results and len(sql_results) > 0:
            # Only send first 10 rows as raw JSON for analysis
            limited_sql = sql_results[:10] if len(sql_results) > 10 else sql_results
            context_parts.append(f"\nSample Data (first 10 rows): {json.dumps(limited_sql, default=str)}")
        else:
            context_parts.append("\nNo results available")
        
        if sql_performance:
            context_parts.append(f"\nQuery execution time: {sql_performance.get('execution_time', 'N/A')}ms")
        
        # Add second dataset results with ASCII table
        context_parts.append("\n=== DATASET 2: Agricultural Network Relationships ===")
        context_parts.append("\nASCII TABLE:")
        context_parts.append(tables['graph_table'])
        
        # Only include limited raw data to avoid overwhelming the context
        if cypher_results and len(cypher_results) > 0:
            # Only send first 10 rows as raw JSON for analysis
            limited_cypher = cypher_results[:10] if len(cypher_results) > 10 else cypher_results
            context_parts.append(f"\nSample Data (first 10 rows): {json.dumps(limited_cypher, default=str)}")
        else:
            context_parts.append("\nNo results available")
        
        if cypher_performance:
            context_parts.append(f"\nQuery execution time: {cypher_performance.get('execution_time', 'N/A')}ms")
        
        # Add comprehensive analysis request
        # Add context engineering for better analysis
        context_parts.append("\n=== ANALYSIS CONTEXT ===")
        context_parts.append(f"You are analyzing agricultural data based on the query: '{query}'")
        context_parts.append("FIRST DATASET: Agricultural metrics by state and year including income, expenses, and farm statistics across the United States.")
        context_parts.append("SECOND DATASET: Network relationships between states, regions, and agricultural measurements showing connections like borders, agricultural belts (Corn Belt, Wheat Belt, Cotton Belt), and economic relationships.")
        context_parts.append("\nANALYSIS REQUIREMENTS:")
        context_parts.append("1. Use ONLY the actual data provided in the ASCII tables above")
        context_parts.append("2. Write DETAILED analysis (minimum 400 words per section)")
        context_parts.append("3. Reference specific values from the tables")
        context_parts.append("4. Answer the user's query directly: " + query)
        context_parts.append("5. Focus on agricultural insights, NOT database technology")
        
        return "\n".join(context_parts)
    
    def _calculate_statistics(self, results: List[Dict]) -> Dict[str, Any]:
        """Calculate statistics for numeric fields in the results."""
        if not results:
            return {}
        
        stats = {}
        # Find numeric fields
        first_record = results[0]
        for key, value in first_record.items():
            if isinstance(value, (int, float)):
                values = [r.get(key) for r in results if isinstance(r.get(key), (int, float))]
                if values:
                    stats[key] = {
                        'min': min(values),
                        'max': max(values),
                        'avg': sum(values) / len(values),
                        'count': len(values),
                        'sum': sum(values)
                    }
        return stats
    
    def _get_unique_values(self, results: List[Dict]) -> Dict[str, List]:
        """Get unique values for categorical fields."""
        if not results:
            return {}
        
        unique_vals = {}
        first_record = results[0]
        for key, value in first_record.items():
            if isinstance(value, str):
                unique = list(set(r.get(key) for r in results if r.get(key)))
                if unique:
                    unique_vals[key] = unique[:10]  # Limit to 10 unique values
        return unique_vals
    
    def _extract_insights(self, interpretation: str) -> Dict[str, Any]:
        """Extract structured insights from the interpretation."""
        insights = {
            'has_sql_advantage': False,
            'has_graph_advantage': False,
            'recommended_database': 'both',
            'key_patterns': [],
            'actionable_insights': []
        }
        
        # Simple keyword-based extraction
        interpretation_lower = interpretation.lower()
        
        # Determine database advantages
        if 'sql' in interpretation_lower and any(word in interpretation_lower 
            for word in ['better', 'faster', 'efficient', 'suitable']):
            insights['has_sql_advantage'] = True
            
        if 'graph' in interpretation_lower or 'neo4j' in interpretation_lower:
            if any(word in interpretation_lower 
                for word in ['better', 'faster', 'relationship', 'connection']):
                insights['has_graph_advantage'] = True
        
        # Determine recommended database
        if insights['has_sql_advantage'] and not insights['has_graph_advantage']:
            insights['recommended_database'] = 'sql'
        elif insights['has_graph_advantage'] and not insights['has_sql_advantage']:
            insights['recommended_database'] = 'graph'
        
        # Extract patterns (lines containing pattern-related keywords)
        pattern_keywords = ['pattern', 'trend', 'correlation', 'relationship', 'finding']
        lines = interpretation.split('\n')
        for line in lines:
            if any(keyword in line.lower() for keyword in pattern_keywords):
                insights['key_patterns'].append(line.strip())
        
        # Extract actionable insights (lines with action words)
        action_keywords = ['should', 'recommend', 'consider', 'optimize', 'improve']
        for line in lines:
            if any(keyword in line.lower() for keyword in action_keywords):
                insights['actionable_insights'].append(line.strip())
        
        return insights
    
    async def generate_summary(
        self,
        results: List[Dict],
        query_type: str
    ) -> str:
        """
        Generate a brief summary of query results.
        
        Args:
            results: Query results to summarize
            query_type: Type of query performed
            
        Returns:
            Brief summary string
        """
        if not results:
            return "No results found for this query."
        
        summary_parts = []
        
        # Add result count
        summary_parts.append(f"Found {len(results)} results")
        
        # Add query type context
        if query_type == 'impact_analysis':
            summary_parts.append("analyzing impact relationships")
        elif query_type == 'trend_analysis':
            summary_parts.append("showing trends over time")
        elif query_type == 'comparison':
            summary_parts.append("comparing different categories")
        elif query_type == 'ranking':
            summary_parts.append("ranked by performance metrics")
        
        # Add key statistics if available
        if results and len(results) > 0:
            first_result = results[0]
            numeric_fields = [k for k, v in first_result.items() 
                            if isinstance(v, (int, float))]
            if numeric_fields:
                summary_parts.append(f"with {len(numeric_fields)} metrics analyzed")
        
        return ". ".join(summary_parts) + "."
    
    async def explain_difference(
        self,
        sql_query: str,
        cypher_query: str
    ) -> str:
        """
        Explain the difference between SQL and Cypher approaches.
        
        Args:
            sql_query: The SQL query used
            cypher_query: The Cypher query used
            
        Returns:
            Explanation of differences
        """
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a database expert. Explain the key differences "
                                 "between SQL and Cypher query approaches in simple terms."
                    },
                    {
                        "role": "user",
                        "content": f"""Explain the difference between these two queries:
                        
                        SQL Query:
                        {sql_query[:2000] if len(sql_query) > 2000 else sql_query}
                        
                        Cypher Query:
                        {cypher_query[:2000] if len(cypher_query) > 2000 else cypher_query}
                        
                        Focus on:
                        1. What each query is doing differently
                        2. Advantages of each approach
                        3. When to use each type
                        
                        Keep it brief (2-3 sentences).
                        """
                    }
                ],
                max_tokens=150,
                temperature=0.5
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            app_logger.error(f"Failed to explain query differences: {e}")
            return ("SQL excels at structured data and aggregations, while Cypher "
                   "specializes in relationship traversal and pattern matching.")