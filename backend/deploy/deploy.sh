#!/bin/bash

# Agricultural Data Platform - Backend Deployment Script for DigitalOcean
# Run this script on your DigitalOcean droplet after initial setup

set -e

echo "=== Agricultural Data Platform Backend Deployment ==="

# Configuration
APP_DIR="/var/www/agricultural-api"
REPO_URL="https://gitlab.com/geojensen/ops.how.git"
BRANCH="production-release"
SERVICE_NAME="agricultural-api"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# 1. Update system packages
echo "📦 Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# 2. Install required system packages
echo "🔧 Installing required packages..."
sudo apt-get install -y python3-pip python3-venv python3-dev build-essential nginx supervisor postgresql-client

# 3. Create application directory
echo "📁 Setting up application directory..."
sudo mkdir -p $APP_DIR
sudo chown $USER:$USER $APP_DIR

# 4. Clone the repository (backend only)
echo "📥 Cloning repository..."
cd $APP_DIR
if [ -d ".git" ]; then
    git pull origin $BRANCH
else
    git clone --branch $BRANCH $REPO_URL .
fi

# 5. Create Python virtual environment
echo "🐍 Setting up Python virtual environment..."
cd $APP_DIR/backend
python3 -m venv venv
source venv/bin/activate

# 6. Install Python dependencies
echo "📚 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# 7. Copy environment file
echo "⚙️ Setting up environment variables..."
if [ ! -f "$APP_DIR/.env" ]; then
    echo -e "${RED}Warning: .env file not found!${NC}"
    echo "Please create $APP_DIR/.env with the following variables:"
    echo "  SUPABASE_URL=your_supabase_url"
    echo "  SUPABASE_KEY=your_supabase_key"
    echo "  NEO4J_URI=your_neo4j_uri"
    echo "  NEO4J_USER=your_neo4j_user"
    echo "  NEO4J_PASSWORD=your_neo4j_password"
    echo "  OPENAI_API_KEY=your_openai_api_key"
    echo "  BACKEND_PORT=8000"
    echo "  CORS_ORIGINS=https://ops.how,https://geojensen.gitlab.io"
    exit 1
fi

# 8. Setup systemd service
echo "🚀 Setting up systemd service..."
sudo cp $APP_DIR/backend/deploy/agricultural-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME

# 9. Setup Nginx
echo "🌐 Configuring Nginx..."
sudo cp $APP_DIR/backend/deploy/nginx.conf /etc/nginx/sites-available/$SERVICE_NAME
sudo ln -sf /etc/nginx/sites-available/$SERVICE_NAME /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx

# 10. Start the application
echo "▶️ Starting the application..."
sudo systemctl restart $SERVICE_NAME

# 11. Check status
echo "✅ Checking service status..."
sudo systemctl status $SERVICE_NAME --no-pager

echo -e "${GREEN}✨ Deployment complete!${NC}"
echo "API should be accessible at: http://$(curl -s ifconfig.me):8000"
echo ""
echo "Useful commands:"
echo "  - Check logs: sudo journalctl -u $SERVICE_NAME -f"
echo "  - Restart service: sudo systemctl restart $SERVICE_NAME"
echo "  - Check status: sudo systemctl status $SERVICE_NAME"