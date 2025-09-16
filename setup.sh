#!/bin/bash

# S&P 500 Agentic AI Project Setup Script
# This script helps set up the development environment

echo "🚀 Setting up S&P 500 Agentic AI Project..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+ and try again."
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js 18+ and try again."
    exit 1
fi

echo "✅ Python and Node.js are installed"

# Setup server (backend)
echo "📦 Setting up backend server..."
cd server

# Create virtual environment
if [ ! -d ".venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment and install dependencies
echo "Installing Python dependencies..."
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file from template if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp config.template .env
    echo "⚠️  Please edit server/.env with your actual API keys and database credentials"
fi

echo "✅ Backend setup complete"

# Setup client (frontend)
echo "📦 Setting up frontend client..."
cd ../client

# Install Node.js dependencies
echo "Installing Node.js dependencies..."
npm install

echo "✅ Frontend setup complete"

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit server/.env with your OpenAI API key and TiDB credentials"
echo "2. Start the backend server:"
echo "   cd server && source .venv/bin/activate && uvicorn main:app --reload --port 8000"
echo "3. Start the frontend client:"
echo "   cd client && npm run dev"
echo "4. Open http://localhost:5173 in your browser"
echo ""
echo "Happy coding! 🚀"
