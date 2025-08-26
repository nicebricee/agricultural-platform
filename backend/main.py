"""
Main FastAPI application for Agricultural Data Platform.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from app.core.config import settings, validate_configuration
from app.core.logging import app_logger
from app.api.endpoints import router as api_router
from app.db.supabase_client import SupabaseManager
from app.db.neo4j_client import Neo4jManager


# Create FastAPI app
app = FastAPI(
    title="Agricultural Data Platform API",
    description="Compare SQL vs Graph database insights for agricultural data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    app_logger.info("Starting Agricultural Data Platform API")
    
    # Validate configuration
    if not validate_configuration():
        app_logger.warning("Configuration validation failed - some features may be unavailable")
    
    # Initialize database connections
    try:
        # Test Supabase connection
        supabase_mgr = SupabaseManager()
        app_logger.info("Supabase connection initialized")
    except Exception as e:
        app_logger.error(f"Failed to initialize Supabase: {e}")
    
    try:
        # Test Neo4j connection
        neo4j_mgr = Neo4jManager()
        await neo4j_mgr.verify_connection()
        app_logger.info("Neo4j connection verified")
    except Exception as e:
        app_logger.error(f"Failed to initialize Neo4j: {e}")
    
    app_logger.info(f"API running in {settings.environment} mode")
    app_logger.info(f"MLENC encryption: {'Active' if settings.encryption_method == 'MLENC' else 'Inactive'}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    app_logger.info("Shutting down Agricultural Data Platform API")
    
    # Close Neo4j connections
    try:
        neo4j_mgr = Neo4jManager()
        await neo4j_mgr.close()
    except:
        pass


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "Agricultural Data Platform API",
        "version": "1.0.0",
        "status": "running",
        "environment": settings.environment,
        "encryption": settings.encryption_method,
        "docs": "/docs"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {
        "status": "healthy",
        "environment": settings.environment,
        "services": {}
    }
    
    # Check Supabase
    try:
        supabase_mgr = SupabaseManager()
        health_status["services"]["supabase"] = "connected"
    except:
        health_status["services"]["supabase"] = "disconnected"
        health_status["status"] = "degraded"
    
    # Check Neo4j
    try:
        neo4j_mgr = Neo4jManager()
        await neo4j_mgr.verify_connection()
        health_status["services"]["neo4j"] = "connected"
    except:
        health_status["services"]["neo4j"] = "disconnected"
        health_status["status"] = "degraded"
    
    # Check OpenAI
    health_status["services"]["openai"] = "configured" if settings.openai_api_key else "not configured"
    
    return health_status


# Include API routes
app.include_router(api_router, prefix="/api/v1")


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    app_logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc) if settings.debug else "An error occurred"
        }
    )


if __name__ == "__main__":
    # Run with uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.backend_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )