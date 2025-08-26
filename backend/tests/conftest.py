"""
Pytest configuration and shared fixtures for all tests.
"""

import pytest
import asyncio
from typing import AsyncGenerator, Generator
from fastapi.testclient import TestClient
from httpx import AsyncClient
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import app
from app.core.config import settings
from app.core.database import DatabaseManager


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_client() -> TestClient:
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    """Create an async test client for the FastAPI app."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
async def db_manager() -> AsyncGenerator[DatabaseManager, None]:
    """Create a database manager for testing."""
    manager = DatabaseManager()
    await manager.initialize()
    yield manager
    await manager.close()


@pytest.fixture
def sample_query() -> str:
    """Sample natural language query for testing."""
    return "Show me corn farms in Iowa with drought impact"


@pytest.fixture
def sample_keywords() -> list:
    """Sample keywords for testing."""
    return ["corn", "farms", "iowa", "drought", "impact"]


@pytest.fixture
def sample_sql_query() -> str:
    """Sample SQL query for testing."""
    return """
        SELECT f.name, f.location, f.primary_crop
        FROM farms f
        WHERE f.state = 'Iowa' 
        AND f.primary_crop = 'corn'
        LIMIT 50
    """


@pytest.fixture
def sample_cypher_query() -> str:
    """Sample Cypher query for testing."""
    return """
        MATCH (f:Farm {state: 'Iowa', primary_crop: 'corn'})
        OPTIONAL MATCH (f)-[:AFFECTED_BY]->(w:WeatherEvent {type: 'drought'})
        RETURN f.name, f.location, w.severity
        LIMIT 50
    """


@pytest.fixture
def mock_settings(monkeypatch):
    """Mock settings for testing."""
    # Override settings for testing
    monkeypatch.setattr(settings, "environment", "testing")
    monkeypatch.setattr(settings, "debug", True)
    monkeypatch.setattr(settings, "max_results", 10)
    monkeypatch.setattr(settings, "query_timeout", 5)
    return settings


@pytest.fixture
def mock_env_variables(monkeypatch):
    """Mock environment variables for testing."""
    test_env = {
        "SUPABASE_URL": "https://test.supabase.co",
        "SUPABASE_ANON_KEY": "test_anon_key",
        "NEO4J_URI": "neo4j://localhost:7687",
        "NEO4J_USERNAME": "neo4j",
        "NEO4J_PASSWORD": "test_password",
        "OPENAI_API_KEY": "test_openai_key",
    }
    
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)
    
    return test_env