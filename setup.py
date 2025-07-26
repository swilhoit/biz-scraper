#!/usr/bin/env python3
"""
Setup script for Business Listings Scraper
Helps configure the environment and .env file
"""

import os
import sys

def create_env_file():
    """Create .env file with user input"""
    print("Setting up Business Listings Scraper...")
    print("=" * 50)
    
    # Check if .env already exists
    if os.path.exists('.env'):
        overwrite = input(".env file already exists. Overwrite? (y/n): ").lower()
        if overwrite != 'y':
            print("Setup cancelled.")
            return False
    
    # Get API key from user
    print("\nYou need a Scraper API key to use this scraper.")
    print("Get one free at: https://www.scraperapi.com/")
    print("(Free tier includes 1000 requests per month)")
    
    api_key = input("\nEnter your Scraper API key: ").strip()
    
    if not api_key:
        print("Error: API key is required!")
        return False
    
    # Create .env file
    try:
        with open('.env', 'w') as f:
            f.write(f"# Scraper API Configuration\n")
            f.write(f"SCRAPER_API_KEY={api_key}\n")
        
        print("\n✅ .env file created successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error creating .env file: {e}")
        return False

def check_dependencies():
    """Check if required packages are installed"""
    print("\nChecking dependencies...")
    
    required_packages = [
        'requests', 'beautifulsoup4', 'pandas', 
        'python-dotenv', 'lxml'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} (missing)")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n⚠️  Missing packages: {', '.join(missing_packages)}")
        print("Install them with: pip install -r requirements.txt")
        return False
    else:
        print("\n✅ All dependencies are installed!")
        return True

def main():
    """Main setup function"""
    print("Business Listings Scraper Setup")
    print("=" * 40)
    
    # Create .env file
    if not create_env_file():
        sys.exit(1)
    
    # Check dependencies
    deps_ok = check_dependencies()
    
    print("\n" + "=" * 50)
    if deps_ok:
        print("🎉 Setup complete! You can now run:")
        print("   python business_scraper.py")
    else:
        print("⚠️  Please install missing dependencies first:")
        print("   pip install -r requirements.txt")
        print("   Then run: python business_scraper.py")
    print("=" * 50)

if __name__ == "__main__":
    main() 