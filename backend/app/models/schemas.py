"""
Pydantic models for request/response validation.
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime


class SearchRequest(BaseModel):
    """Natural language search request."""
    
    query: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Natural language query about agricultural data"
    )
    max_results: Optional[int] = Field(
        default=200,
        ge=1,
        le=500,
        description="Maximum number of results to return"
    )
    
    @validator("query")
    def clean_query(cls, v):
        """Clean and validate the query string."""
        # Remove excessive whitespace
        v = " ".join(v.split())
        # Basic SQL injection prevention
        dangerous_patterns = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE"]
        for pattern in dangerous_patterns:
            if pattern in v.upper():
                raise ValueError(f"Query contains potentially dangerous pattern: {pattern}")
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "query": "Which farms will be affected if fertilizer supplier X has issues?",
                "max_results": 50
            }
        }


class QueryResults(BaseModel):
    """Results from a database query."""
    
    data: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Query result data"
    )
    execution_time: float = Field(
        ...,
        description="Query execution time in seconds"
    )
    row_count: int = Field(
        ...,
        description="Number of rows returned"
    )
    interpretation: Optional[str] = Field(
        default=None,
        description="AI-generated interpretation of results"
    )
    error: Optional[str] = Field(
        default=None,
        description="Error message if query failed"
    )
    display_format: Optional[str] = Field(
        default="table",
        description="Display format: 'table' for SQL, 'neo4j_graph' for graph with nodes/relationships"
    )


class SearchResponse(BaseModel):
    """Response containing both SQL and Graph query results."""
    
    query: str = Field(
        ...,
        description="Original search query"
    )
    keywords: List[str] = Field(
        default_factory=list,
        description="Keywords extracted from the query"
    )
    sql_results: QueryResults = Field(
        ...,
        description="Results from SQL database"
    )
    graph_results: QueryResults = Field(
        ...,
        description="Results from Graph database"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Response timestamp"
    )
    total_execution_time: float = Field(
        ...,
        description="Total query execution time"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "query": "Show me corn production trends in Iowa",
                "keywords": ["corn", "production", "trends", "Iowa"],
                "sql_results": {
                    "data": [{"farm_id": 1, "corn_yield": 180}],
                    "execution_time": 0.45,
                    "row_count": 25,
                    "interpretation": "Iowa corn production shows steady growth..."
                },
                "graph_results": {
                    "data": [{"farm": {"id": 1}, "relationships": 5}],
                    "execution_time": 0.38,
                    "row_count": 30,
                    "interpretation": "Network analysis reveals corn production clusters..."
                },
                "timestamp": "2025-08-22T10:30:00Z",
                "total_execution_time": 0.83
            }
        }


class SampleQuery(BaseModel):
    """Sample query for demonstration."""
    
    title: str = Field(
        ...,
        description="Short title for the query"
    )
    query: str = Field(
        ...,
        description="Sample query text"
    )
    category: str = Field(
        ...,
        description="Query category"
    )
    description: Optional[str] = Field(
        default=None,
        description="Brief description of what the query demonstrates"
    )


class SampleQueriesResponse(BaseModel):
    """Response containing sample queries."""
    
    queries: List[SampleQuery] = Field(
        ...,
        description="List of sample queries"
    )
    categories: List[str] = Field(
        ...,
        description="Available query categories"
    )


class ErrorResponse(BaseModel):
    """Error response model."""
    
    error: str = Field(
        ...,
        description="Error message"
    )
    detail: Optional[str] = Field(
        default=None,
        description="Detailed error information"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Error timestamp"
    )
    request_id: Optional[str] = Field(
        default=None,
        description="Request ID for tracking"
    )


class HealthCheckResponse(BaseModel):
    """Health check response model."""
    
    status: str = Field(
        ...,
        description="Overall health status"
    )
    timestamp: float = Field(
        ...,
        description="Timestamp"
    )
    environment: str = Field(
        ...,
        description="Environment name"
    )
    services: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="Service health statuses"
    )


class DatabaseInfo(BaseModel):
    """Information about a database."""
    
    name: str = Field(
        ...,
        description="Database name"
    )
    type: str = Field(
        ...,
        description="Database type (SQL or Graph)"
    )
    status: str = Field(
        ...,
        description="Connection status"
    )
    records_count: Optional[int] = Field(
        default=None,
        description="Approximate number of records"
    )
    last_updated: Optional[datetime] = Field(
        default=None,
        description="Last data update timestamp"
    )


class SystemInfoResponse(BaseModel):
    """System information response."""
    
    version: str = Field(
        ...,
        description="API version"
    )
    databases: List[DatabaseInfo] = Field(
        ...,
        description="Database information"
    )
    features: List[str] = Field(
        ...,
        description="Enabled features"
    )
    limits: Dict[str, Any] = Field(
        ...,
        description="System limits and quotas"
    )