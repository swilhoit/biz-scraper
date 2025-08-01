#!/bin/bash
# Business Scraper Runner Script

echo "Business Listing Scraper"
echo "======================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt -q

# Check for .env file
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    echo "Please copy .env.example to .env and add your ScraperAPI key"
    exit 1
fi

# Run the scraper with arguments
python main.py "$@"