from flask import Flask, jsonify
import threading
import subprocess
import os
import logging
from datetime import datetime

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

scraper_status = {
    "running": False,
    "last_run": None,
    "last_status": None,
    "last_error": None
}

def run_scraper():
    """Run the scraper in a separate thread"""
    global scraper_status
    
    try:
        scraper_status["running"] = True
        scraper_status["last_run"] = datetime.now().isoformat()
        
        logging.info("Starting scraper execution...")
        result = subprocess.run(
            ["python", "main.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            scraper_status["last_status"] = "success"
            scraper_status["last_error"] = None
            logging.info("Scraper completed successfully")
        else:
            scraper_status["last_status"] = "failed"
            scraper_status["last_error"] = result.stderr
            logging.error(f"Scraper failed: {result.stderr}")
            
    except Exception as e:
        scraper_status["last_status"] = "error"
        scraper_status["last_error"] = str(e)
        logging.error(f"Error running scraper: {e}")
    finally:
        scraper_status["running"] = False

@app.route('/')
def home():
    """Root endpoint"""
    return jsonify({
        "service": "Business Scraper",
        "status": "ready",
        "endpoints": {
            "/": "Service info",
            "/health": "Health check",
            "/ready": "Readiness check",
            "/run": "Trigger scraper execution",
            "/status": "Get scraper status"
        }
    })

@app.route('/health')
def health():
    """Health check endpoint for Cloud Run"""
    return jsonify({"status": "healthy"}), 200

@app.route('/ready')
def ready():
    """Readiness check endpoint"""
    return jsonify({"status": "ready"}), 200

@app.route('/run')
def run():
    """Trigger scraper execution"""
    if scraper_status["running"]:
        return jsonify({
            "status": "already_running",
            "message": "Scraper is already running"
        }), 409
    
    # Run scraper in background thread
    thread = threading.Thread(target=run_scraper)
    thread.start()
    
    return jsonify({
        "status": "started",
        "message": "Scraper execution started"
    }), 202

@app.route('/status')
def status():
    """Get scraper status"""
    return jsonify(scraper_status)

if __name__ == '__main__':
    # Get port from environment variable (Cloud Run sets this)
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)