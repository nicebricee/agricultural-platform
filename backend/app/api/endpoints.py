"""
API endpoints for the Agricultural Data Platform.
"""

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse
from typing import List, Dict, Any, AsyncGenerator
import asyncio
import time
import json

from app.models.schemas import (
    SearchRequest,
    SearchResponse,
    SampleQuery,
    SampleQueriesResponse,
    QueryResults,
    ErrorResponse
)
from app.core.logging import app_logger
from app.services.keyword_extractor import KeywordExtractor
from app.services.sql_query_generator import SQLQueryGenerator
from app.services.cypher_query_generator import CypherQueryGenerator
from app.services.openai_interpreter import OpenAIInterpreter
from app.services.graph_formatter import GraphFormatter

router = APIRouter()

# Initialize services
keyword_extractor = KeywordExtractor()
sql_generator = SQLQueryGenerator()
cypher_generator = CypherQueryGenerator()
ai_interpreter = OpenAIInterpreter()
graph_formatter = GraphFormatter()


@router.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest, req: Request) -> SearchResponse:
    """
    Process a natural language search query.
    
    This endpoint:
    1. Extracts keywords from the natural language query
    2. Generates SQL and Cypher queries
    3. Executes both queries in parallel
    4. Uses AI to interpret the results
    5. Returns comparative insights
    """
    start_time = time.time()
    
    try:
        app_logger.info(f"Processing search query: {request.query}")
        
        # Get database manager from app state
        if not hasattr(req.app.state, 'db_manager'):
            raise HTTPException(status_code=503, detail="Database connections not initialized")
        
        db_manager = req.app.state.db_manager
        
        # Step 1: Extract keywords
        keywords = await keyword_extractor.extract(request.query)
        app_logger.debug(f"Extracted keywords: {keywords}")
        
        # Step 2: Generate queries
        sql_result = await sql_generator.generate(request.query, keywords)
        cypher_result = await cypher_generator.generate(request.query, keywords)
        
        # Extract the actual query strings from the result dictionaries
        sql_query = sql_result['sql'] if isinstance(sql_result, dict) else sql_result
        cypher_query = cypher_result['cypher'] if isinstance(cypher_result, dict) else cypher_result
        
        app_logger.debug(f"SQL Query: {sql_query}")
        app_logger.debug(f"Cypher Query: {cypher_query}")
        
        # Step 3: Execute queries in parallel
        query_results = await db_manager.execute_parallel_queries(
            sql_query=sql_query,
            cypher_query=cypher_query
        )
        
        # Format graph results if they contain graph structure
        graph_results = query_results["graph_results"]
        if graph_formatter.detect_graph_format(graph_results):
            # Format for display while keeping original for AI
            formatted_graph_data = graph_formatter.format_for_display(graph_results)
            # Add format indicator
            graph_results["display_format"] = "neo4j_graph"
            graph_results["formatted_data"] = formatted_graph_data
            app_logger.info(f"Graph structure detected! Nodes: {len(graph_results.get('graph_structure', {}).get('nodes', {}))}, "
                           f"Relationships: {len(graph_results.get('graph_structure', {}).get('relationships', []))}")
        else:
            # Fallback: Even without graph structure, format Neo4j results differently
            # Check if results contain node-like fields (state_node, measurement_node, etc.)
            if graph_results.get("data") and len(graph_results["data"]) > 0:
                first_record = graph_results["data"][0]
                if any(key.endswith("_node") for key in first_record.keys()):
                    # We have node fields, use fallback formatter
                    formatted_graph_data = graph_formatter.format_for_display(graph_results)
                    graph_results["display_format"] = "neo4j_graph"
                    graph_results["formatted_data"] = formatted_graph_data
                    app_logger.info("Using fallback graph formatter for node fields")
                else:
                    graph_results["display_format"] = "table"
                    app_logger.debug("No graph structure or node fields detected, using table format")
            else:
                graph_results["display_format"] = "table"
                app_logger.debug("No graph results to format")
        
        # SQL results always in table format
        query_results["sql_results"]["display_format"] = "table"
        
        app_logger.debug(f"Display formats - SQL: {query_results['sql_results']['display_format']}, "
                        f"Graph: {graph_results['display_format']}")
        
        # Step 4: Interpret results with AI (use original data for interpretation)
        interpretation = await ai_interpreter.interpret_results(
            query=request.query,
            sql_results=query_results["sql_results"]["data"],
            cypher_results=query_results["graph_results"]["data"],
            sql_performance={"execution_time": query_results["sql_results"]["execution_time"]},
            cypher_performance={"execution_time": query_results["graph_results"]["execution_time"]}
        )
        
        # Step 5: Prepare response
        total_time = time.time() - start_time
        
        # Add truncation indicators
        sql_total = query_results["sql_results"]["row_count"]
        graph_total = query_results["graph_results"]["row_count"]
        sql_truncated = sql_total > request.max_results
        graph_truncated = graph_total > request.max_results
        
        sql_interp = interpretation.get("sql_interpretation", "")
        if sql_truncated:
            sql_interp = f"[Showing {request.max_results} of {sql_total} results] {sql_interp}"
        
        graph_interp = interpretation.get("graph_interpretation", "")
        if graph_truncated:
            graph_interp = f"[Showing {request.max_results} of {graph_total} results] {graph_interp}"
        
        # Use formatted data for graph if available, otherwise use regular data
        graph_display_data = graph_results.get("formatted_data", graph_results["data"])[:request.max_results]
        
        response = SearchResponse(
            query=request.query,
            keywords=keywords,
            sql_results=QueryResults(
                data=query_results["sql_results"]["data"][:request.max_results],
                execution_time=query_results["sql_results"]["execution_time"],
                row_count=query_results["sql_results"]["row_count"],
                interpretation=sql_interp,
                display_format=query_results["sql_results"].get("display_format", "table")
            ),
            graph_results=QueryResults(
                data=graph_display_data,
                execution_time=query_results["graph_results"]["execution_time"],
                row_count=query_results["graph_results"]["row_count"],
                interpretation=graph_interp,
                display_format=graph_results.get("display_format", "table")
            ),
            total_execution_time=total_time
        )
        
        app_logger.info(f"Search completed in {total_time:.2f}s")
        return response
        
    except Exception as e:
        app_logger.error(f"Search error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/stream")
async def search_stream(request: SearchRequest, req: Request) -> StreamingResponse:
    """
    Process a natural language search query with streaming response.
    
    This endpoint streams the AI interpretation in real-time as it's generated.
    """
    async def generate_stream() -> AsyncGenerator[str, None]:
        start_time = time.time()
        
        try:
            # Send initial status
            yield f"data: {json.dumps({'status': 'starting', 'message': 'Processing query...'})}\n\n"
            
            # Get database manager from app state
            if not hasattr(req.app.state, 'db_manager'):
                yield f"data: {json.dumps({'error': 'Database connections not initialized'})}\n\n"
                return
            
            db_manager = req.app.state.db_manager
            
            # Step 1: Extract keywords
            yield f"data: {json.dumps({'status': 'extracting', 'message': 'Extracting keywords...'})}\n\n"
            keywords = await keyword_extractor.extract(request.query)
            yield f"data: {json.dumps({'keywords': keywords})}\n\n"
            
            # Step 2: Generate queries
            yield f"data: {json.dumps({'status': 'generating', 'message': 'Generating database queries...'})}\n\n"
            sql_result = await sql_generator.generate(request.query, keywords)
            cypher_result = await cypher_generator.generate(request.query, keywords)
            
            sql_query = sql_result['sql'] if isinstance(sql_result, dict) else sql_result
            cypher_query = cypher_result['cypher'] if isinstance(cypher_result, dict) else cypher_result
            
            # Step 3: Execute queries in parallel
            yield f"data: {json.dumps({'status': 'executing', 'message': 'Executing database queries...'})}\n\n"
            app_logger.info(f"Executing SQL query: {sql_query}")
            app_logger.info(f"Executing Cypher query: {cypher_query}")
            
            query_results = await db_manager.execute_parallel_queries(
                sql_query=sql_query,
                cypher_query=cypher_query
            )
            
            # Debug logging for query results
            sql_results = query_results['sql_results']
            graph_results = query_results['graph_results']
            
            app_logger.debug(f"SQL results - Rows: {sql_results['row_count']}, Execution time: {sql_results['execution_time']:.3f}s")
            app_logger.debug(f"Graph results - Rows: {graph_results['row_count']}, Execution time: {graph_results['execution_time']:.3f}s")
            
            if 'error' in sql_results:
                app_logger.error(f"SQL query error: {sql_results['error']}")
            if 'error' in graph_results:
                app_logger.error(f"Graph query error: {graph_results['error']}")
            
            # Send query results summary
            yield f"data: {json.dumps({'sql_rows': sql_results['row_count'], 'graph_rows': graph_results['row_count']})}\n\n"
            
            # Step 4: Stream AI interpretation
            yield f"data: {json.dumps({'status': 'interpreting', 'message': 'Generating AI analysis...'})}\n\n"
            
            # Start streaming interpretation
            interpretation_chunks = []
            final_json_chunk = None
            
            async for chunk in ai_interpreter.interpret_results_stream(
                query=request.query,
                sql_results=query_results["sql_results"]["data"],
                cypher_results=query_results["graph_results"]["data"],
                sql_performance={"execution_time": query_results["sql_results"]["execution_time"]},
                cypher_performance={"execution_time": query_results["graph_results"]["execution_time"]}
            ):
                # Check if this chunk is the final JSON response
                # Look for specific JSON structure to avoid false positives
                stripped_chunk = chunk.strip()
                if (stripped_chunk.startswith('{') and 
                    '"sql_interpretation"' in stripped_chunk and 
                    '"graph_interpretation"' in stripped_chunk):
                    try:
                        # This looks like the final JSON chunk with expected structure
                        json.loads(stripped_chunk)
                        final_json_chunk = stripped_chunk
                        app_logger.debug("Received final JSON chunk from AI interpreter")
                        # Don't send this chunk to client as it's the structured response
                        continue
                    except json.JSONDecodeError:
                        # Not valid JSON, treat as regular chunk
                        pass
                
                # Send regular text chunks for real-time streaming display
                yield f"data: {json.dumps({'chunk': chunk})}\n\n"
                interpretation_chunks.append(chunk)
                await asyncio.sleep(0.01)  # Small delay to prevent overwhelming the client
            
            # Parse the final JSON interpretation
            interpretation = None
            if final_json_chunk:
                try:
                    interpretation = json.loads(final_json_chunk)
                    app_logger.debug("Successfully parsed final JSON interpretation")
                except json.JSONDecodeError as e:
                    app_logger.error(f"Failed to parse final JSON chunk: {e}")
            
            # Fallback if no valid JSON interpretation was received
            if not interpretation:
                app_logger.warning("No valid JSON interpretation received, creating fallback")
                full_text = ''.join(interpretation_chunks)
                interpretation = {
                    'sql_interpretation': f"SQL Database Analysis: {full_text[:800]}..." if len(full_text) > 800 else f"SQL Database Analysis: {full_text}",
                    'graph_interpretation': f"Graph Database Analysis: {full_text[:800]}..." if len(full_text) > 800 else f"Graph Database Analysis: {full_text}",
                    'comparison': "Unable to generate detailed comparison due to parsing issues. Both databases returned results but analysis formatting failed.",
                    'error': "AI interpretation parsing failed"
                }
            
            # Send final results
            total_time = time.time() - start_time
            final_response = {
                'status': 'complete',
                'query': request.query,
                'keywords': keywords,
                'sql_results': {
                    'data': query_results["sql_results"]["data"][:request.max_results],
                    'execution_time': query_results["sql_results"]["execution_time"],
                    'row_count': query_results["sql_results"]["row_count"],
                    'interpretation': interpretation.get("sql_interpretation", "")
                },
                'graph_results': {
                    'data': query_results["graph_results"]["data"][:request.max_results],
                    'execution_time': query_results["graph_results"]["execution_time"],
                    'row_count': query_results["graph_results"]["row_count"],
                    'interpretation': interpretation.get("graph_interpretation", "")
                },
                'total_execution_time': total_time
            }
            
            yield f"data: {json.dumps(final_response)}\n\n"
            
        except Exception as e:
            app_logger.error(f"Streaming search error: {str(e)}", exc_info=True)
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable Nginx buffering
        }
    )


@router.get("/sample-queries", response_model=SampleQueriesResponse)
async def get_sample_queries() -> SampleQueriesResponse:
    """
    Get sample queries for demonstration purposes.
    """
    queries = [
        SampleQuery(
            title="Supply Chain Impact",
            query="Which farms will be affected if fertilizer supplier X has contamination issues?",
            category="Supply Chain",
            description="Shows how graph databases reveal cascading supply chain impacts"
        ),
        SampleQuery(
            title="Equipment Maintenance",
            query="What patterns predict tractor maintenance failures?",
            category="Equipment",
            description="Demonstrates pattern recognition across equipment networks"
        ),
        SampleQuery(
            title="Organic Certification",
            query="Where should we focus organic certification efforts?",
            category="Market Analysis",
            description="Identifies influence nodes in agricultural communities"
        ),
        SampleQuery(
            title="Crop Production Trends",
            query="Show me corn production trends in Iowa",
            category="Production",
            description="Analyzes crop yields and production patterns"
        ),
        SampleQuery(
            title="Supplier Reliability",
            query="Which equipment suppliers are most reliable?",
            category="Supply Chain",
            description="Evaluates supplier performance across farm networks"
        ),
        SampleQuery(
            title="Drought Impact",
            query="Show me all farms affected by drought in California",
            category="Environmental",
            description="Maps environmental impacts across agricultural regions"
        ),
        SampleQuery(
            title="Market Access",
            query="Find organic farms near grain elevators",
            category="Market Analysis",
            description="Identifies market opportunities based on proximity"
        ),
        SampleQuery(
            title="Cost Analysis",
            query="What's the impact of fertilizer price increases?",
            category="Economics",
            description="Analyzes economic impacts across farm operations"
        )
    ]
    
    categories = list(set(q.category for q in queries))
    
    return SampleQueriesResponse(
        queries=queries,
        categories=sorted(categories)
    )


@router.get("/system-info")
async def get_system_info(request: Request) -> Dict[str, Any]:
    """
    Get system information and status.
    """
    db_manager = request.app.state.db_manager if hasattr(request.app.state, 'db_manager') else None
    
    system_info = {
        "version": "1.0.0",
        "status": "operational",
        "databases": {
            "supabase": {
                "connected": await db_manager.check_supabase_health() if db_manager else False,
                "type": "SQL (PostgreSQL)"
            },
            "neo4j": {
                "connected": await db_manager.check_neo4j_health() if db_manager else False,
                "type": "Graph"
            }
        },
        "features": [
            "Natural language search",
            "Parallel query execution",
            "AI-powered interpretations",
            "Encrypted credentials",
            "Real-time comparisons"
        ],
        "limits": {
            "max_query_length": 500,
            "max_results": 50,
            "query_timeout": 5
        }
    }
    
    return system_info