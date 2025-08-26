#!/bin/bash
# Start backend with clean environment - no cached env vars

echo "Starting backend with clean environment..."

# Explicitly unset problematic cached variables
unset OPENAI_API_KEY
unset OPENAI_MODEL
unset OPENAI_MAX_TOKENS
unset SUPABASE_URL
unset SUPABASE_ANON_KEY
unset SUPABASE_SERVICE_KEY
unset NEO4J_URI
unset NEO4J_USERNAME
unset NEO4J_PASSWORD

echo "Cleared all cached environment variables"

# Start backend with minimal environment
# This ensures it reads from .env file, not shell environment
exec env -i \
    PATH="$PATH" \
    HOME="$HOME" \
    USER="$USER" \
    TERM="${TERM:-xterm}" \
    PYTHONPATH="/Users/brice/Ontology-Pipeline/backend" \
    python -m uvicorn app.main:app --port 8000 --reload