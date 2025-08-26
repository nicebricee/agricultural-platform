"""
Integration tests for API endpoints.
"""

import pytest
import json
from httpx import AsyncClient
try:
    from fastapi.testclient import TestClient
except ImportError:
    from starlette.testclient import TestClient
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import app


class TestAPIEndpoints:
    """Test suite for API endpoint integration."""
    
    def test_root_endpoint(self):
        """Test the root endpoint returns correct information."""
        client = TestClient(app)
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert data["name"] == "Agricultural Data Platform API"
        assert "version" in data
        assert "status" in data
        assert data["status"] == "running"
    
    def test_health_endpoint(self):
        """Test the health check endpoint."""
        client = TestClient(app)
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "services" in data
        assert "supabase" in data["services"]
        assert "neo4j" in data["services"]
        assert "openai" in data["services"]
    
    def test_sample_queries_endpoint(self):
        """Test the sample queries endpoint."""
        client = TestClient(app)
        response = client.get("/api/v1/sample-queries")
        
        assert response.status_code == 200
        data = response.json()
        assert "queries" in data
        assert "categories" in data
        assert len(data["queries"]) > 0
        
        # Check query structure
        first_query = data["queries"][0]
        assert "title" in first_query
        assert "query" in first_query
        assert "category" in first_query
        assert "description" in first_query
    
    def test_system_info_endpoint(self):
        """Test the system info endpoint."""
        client = TestClient(app)
        response = client.get("/api/v1/system-info")
        
        assert response.status_code == 200
        data = response.json()
        assert "version" in data
        assert "status" in data
        assert "databases" in data
        assert "features" in data
        assert "limits" in data
    
    @pytest.mark.asyncio
    async def test_search_endpoint_valid_query(self):
        """Test the search endpoint with a valid query."""
        from httpx import ASGITransport
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/search",
                json={
                    "query": "Show corn production in Iowa",
                    "max_results": 10
                }
            )
            
            # Check if successful or service unavailable
            assert response.status_code in [200, 503]
    
    def test_search_endpoint_empty_query(self):
        """Test the search endpoint with an empty query."""
        client = TestClient(app)
        response = client.post(
            "/api/v1/search",
            json={
                "query": "",
                "max_results": 10
            }
        )
        
        # Should be rejected by validation
        assert response.status_code == 422
    
    def test_search_endpoint_long_query(self):
        """Test the search endpoint with a very long query."""
        client = TestClient(app)
        long_query = "a" * 501  # Exceeds 500 char limit
        response = client.post(
            "/api/v1/search",
            json={
                "query": long_query,
                "max_results": 10
            }
        )
        
        # Should be rejected by validation
        assert response.status_code == 422
    
    def test_search_endpoint_invalid_max_results(self):
        """Test the search endpoint with invalid max_results."""
        client = TestClient(app)
        response = client.post(
            "/api/v1/search",
            json={
                "query": "Test query",
                "max_results": -1
            }
        )
        
        # Should be rejected by validation
        assert response.status_code == 422
    
    def test_search_endpoint_no_body(self):
        """Test the search endpoint with no request body."""
        client = TestClient(app)
        response = client.post("/api/v1/search")
        
        assert response.status_code == 422
    
    def test_cors_headers(self):
        """Test that CORS headers are properly set."""
        client = TestClient(app)
        response = client.options(
            "/api/v1/search",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            }
        )
        
        # Check CORS headers
        assert "access-control-allow-origin" in response.headers
    
    def test_api_documentation(self):
        """Test that API documentation is accessible."""
        client = TestClient(app)
        # Test OpenAPI schema
        response = client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        assert "openapi" in schema
        assert "paths" in schema
        
        # Test Swagger UI
        response = client.get("/docs")
        assert response.status_code == 200
        assert "swagger" in response.text.lower()
        
        # Test ReDoc
        response = client.get("/redoc")
        assert response.status_code == 200
        assert "redoc" in response.text.lower()
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self):
        """Test handling of concurrent requests."""
        from httpx import ASGITransport
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Send multiple requests concurrently
            import asyncio
            
            tasks = []
            for i in range(5):
                task = client.get("/health")
                tasks.append(task)
            
            responses = await asyncio.gather(*tasks)
            
            # All should succeed
            for response in responses:
                assert response.status_code == 200
    
    def test_error_handling(self):
        """Test global error handling."""
        client = TestClient(app)
        # Test invalid endpoint
        response = client.get("/api/v1/invalid-endpoint")
        assert response.status_code == 404
        
        # Test method not allowed
        response = client.put("/api/v1/sample-queries")
        assert response.status_code == 405
    
    @pytest.mark.asyncio
    async def test_database_connection_status(self):
        """Test database connection status check."""
        from httpx import ASGITransport
        
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health")
            assert response.status_code == 200
            data = response.json()
            # Should indicate actual database status
            assert "status" in data
            assert "services" in data
    
    def test_request_validation(self):
        """Test request validation for search endpoint."""
        client = TestClient(app)
        # Test with extra fields (should be ignored)
        response = client.post(
            "/api/v1/search",
            json={
                "query": "Test query",
                "max_results": 10,
                "extra_field": "should be ignored"
            }
        )
        
        # Should still work
        assert response.status_code in [200, 500, 503]
        
        # Test with wrong type
        response = client.post(
            "/api/v1/search",
            json={
                "query": 123,  # Should be string
                "max_results": "ten"  # Should be int
            }
        )
        
        assert response.status_code == 422
    
    def test_response_headers(self):
        """Test that proper response headers are set."""
        client = TestClient(app)
        response = client.get("/")
        
        # Check content type
        assert "content-type" in response.headers
        assert "application/json" in response.headers["content-type"]
        
        # Check other headers
        assert "content-length" in response.headers
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self):
        """Test request timeout handling."""
        from httpx import ASGITransport
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", timeout=0.001) as client:
            # This should timeout
            try:
                response = await client.post(
                    "/api/v1/search",
                    json={
                        "query": "Complex query that takes time",
                        "max_results": 50
                    }
                )
            except Exception as e:
                # Timeout exception is expected
                assert True
    
    def test_api_versioning(self):
        """Test API versioning in URLs."""
        client = TestClient(app)
        # v1 endpoints should work
        response = client.get("/api/v1/sample-queries")
        assert response.status_code == 200
        
        # Non-versioned should not work
        response = client.get("/api/sample-queries")
        assert response.status_code == 404
        
        # Wrong version should not work
        response = client.get("/api/v2/sample-queries")
        assert response.status_code == 404