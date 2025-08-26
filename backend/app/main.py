"""
FastAPI application for the Agricultural Data Platform.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import time
from typing import Dict, Any

from app.core.config import settings, validate_configuration
from app.core.logging import app_logger
from app.api import endpoints
from app.core.database import DatabaseManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    app_logger.info("Starting Agricultural Data Platform API")
    app_logger.info(f"Environment: {settings.environment}")
    app_logger.info(f"Debug mode: {settings.debug}")
    
    # Validate configuration
    config_valid = validate_configuration()
    if not config_valid:
        app_logger.warning("Configuration validation failed - some features may not work")
    
    # Initialize database connections
    db_manager = DatabaseManager()
    await db_manager.initialize()
    app.state.db_manager = db_manager
    
    # Check if we should seed data
    if settings.auto_seed_data:
        app_logger.info("Auto-seeding data is enabled")
        # TODO: Implement data seeding
    
    app_logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    app_logger.info("Shutting down Agricultural Data Platform API")
    await db_manager.close()
    app_logger.info("Application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Agricultural Data Platform API",
    description="Compare SQL and Graph database insights for agricultural data",
    version="1.0.0",
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    lifespan=lifespan
)

# Configure CORS
cors_origins = settings.cors_origins.split(",") if isinstance(settings.cors_origins, str) else settings.cors_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Add request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add request processing time to response headers."""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Log slow requests
    if process_time > 2.0:
        app_logger.warning(
            f"Slow request: {request.method} {request.url.path} took {process_time:.2f}s"
        )
    
    return response


# Add global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions globally."""
    app_logger.error(f"Unhandled exception: {exc}", exc_info=True)
    
    if settings.debug:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "detail": str(exc),
                "type": type(exc).__name__
            }
        )
    else:
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error"}
        )


# Root endpoint
@app.get("/", tags=["Root"])
async def root() -> Dict[str, Any]:
    """Root endpoint providing API information."""
    return {
        "name": "Agricultural Data Platform API",
        "version": "1.0.0",
        "status": "operational",
        "environment": settings.environment,
        "documentation": "/docs" if settings.debug else None
    }


# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check(request: Request) -> Dict[str, Any]:
    """Health check endpoint for monitoring."""
    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "environment": settings.environment,
        "services": {}
    }
    
    # Check database connections
    if hasattr(request.app.state, 'db_manager'):
        db_manager = request.app.state.db_manager
        
        # Check Supabase
        supabase_healthy = await db_manager.check_supabase_health()
        health_status["services"]["supabase"] = {
            "status": "healthy" if supabase_healthy else "unhealthy"
        }
        
        # Check Neo4j
        neo4j_healthy = await db_manager.check_neo4j_health()
        health_status["services"]["neo4j"] = {
            "status": "healthy" if neo4j_healthy else "unhealthy"
        }
        
        # Check OpenAI (just verify key exists)
        health_status["services"]["openai"] = {
            "status": "healthy" if settings.openai_api_key else "unhealthy"
        }
        
        # Overall health
        all_healthy = supabase_healthy and neo4j_healthy and settings.openai_api_key
        health_status["status"] = "healthy" if all_healthy else "degraded"
    else:
        health_status["status"] = "initializing"
    
    return health_status


# Include API routes
app.include_router(endpoints, prefix="/api/v1")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.backend_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )