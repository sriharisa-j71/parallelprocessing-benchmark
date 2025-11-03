from flask import Flask, request, Response
import time
import random
import orjson
import logging
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Request counter for monitoring
request_count = {'capitalize': 0, 'line_length': 0}


def json_response(data, status=200):
    """Helper function to create JSON response using orjson"""
    return Response(
        orjson.dumps(data),
        status=status,
        mimetype='application/json'
    )


def count_requests(endpoint_name):
    """Decorator to count requests per endpoint"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            request_count[endpoint_name] += 1
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def simulate_api_latency():
    """
    Simulate real-world API latency (10ms to 2 seconds)
    Returns delay in seconds
    """
    delay = random.uniform(0.01, 2.0)
    time.sleep(delay)
    return delay


def process_capitalize(line: str) -> tuple:
    """
    Process capitalize logic
    
    Args:
        line: Input line to process
        
    Returns:
        Tuple of (capitalized_word, processing_time_ms)
    """
    # Simulate API latency
    delay = simulate_api_latency()
    
    # Get first word and capitalize it
    if not line:
        return '', round(delay * 1000, 2)
    
    first_word = (
        line.split('|')[0].strip() 
        if '|' in line 
        else line.split()[0] if line.split() else ''
    )
    capitalized = first_word.upper()
    
    return capitalized, round(delay * 1000, 2)


def process_line_length(line: str) -> tuple:
    """
    Process line length logic
    
    Args:
        line: Input line to process
        
    Returns:
        Tuple of (length, processing_time_ms)
    """
    # Simulate API latency
    delay = simulate_api_latency()
    
    if not line:
        return 0, round(delay * 1000, 2)
    
    length = len(line)
    return length, round(delay * 1000, 2)


@app.route('/capitalize', methods=['POST'])
@count_requests('capitalize')
def capitalize_first_word():
    """API endpoint that returns the first word capitalized"""
    try:
        # Use orjson for faster parsing
        data = orjson.loads(request.data)
        line = data.get('line', '')
        
        if not line:
            return json_response({'error': 'No line provided'}, 400)
        
        # Process the request
        capitalized, processing_time = process_capitalize(line)
        
        return json_response({
            'capitalized': capitalized,
            'processing_time': processing_time
        })
        
    except orjson.JSONDecodeError:
        logger.warning("Invalid JSON received")
        return json_response({'error': 'Invalid JSON'}, 400)
    except Exception as e:
        logger.error(f"Error in capitalize endpoint: {e}", exc_info=True)
        return json_response({'error': 'Internal server error'}, 500)


@app.route('/line_length', methods=['POST'])
@count_requests('line_length')
def get_line_length():
    """API endpoint that returns the length of the line"""
    try:
        # Use orjson for faster parsing
        data = orjson.loads(request.data)
        line = data.get('line', '')
        
        if not line:
            return json_response({'error': 'No line provided'}, 400)
        
        # Process the request
        length, processing_time = process_line_length(line)
        
        return json_response({
            'length': length,
            'processing_time': processing_time
        })
        
    except orjson.JSONDecodeError:
        logger.warning("Invalid JSON received")
        return json_response({'error': 'Invalid JSON'}, 400)
    except Exception as e:
        logger.error(f"Error in line_length endpoint: {e}", exc_info=True)
        return json_response({'error': 'Internal server error'}, 500)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return json_response({
        'status': 'healthy',
        'json_parser': 'orjson',
        'project_manager': 'uv',
        'server': 'Flask',
        'threading': 'enabled',
        'timestamp': time.time()
    })


@app.route('/metrics', methods=['GET'])
def metrics():
    """Metrics endpoint to check server statistics"""
    return json_response({
        'requests': {
            'capitalize': request_count['capitalize'],
            'line_length': request_count['line_length'],
            'total': request_count['capitalize'] + request_count['line_length']
        },
        'server': {
            'type': 'Flask',
            'threaded': True,
            'uptime_seconds': time.time() - app.config.get('START_TIME', time.time())
        }
    })


@app.route('/reset_metrics', methods=['POST'])
def reset_metrics():
    """Reset request counters"""
    request_count['capitalize'] = 0
    request_count['line_length'] = 0
    return json_response({'status': 'metrics reset'})


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return json_response({'error': 'Endpoint not found'}, 404)


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}", exc_info=True)
    return json_response({'error': 'Internal server error'}, 500)


@app.before_request
def before_request():
    """Log incoming requests (optional, can be disabled for better performance)"""
    # Uncomment to enable request logging:
    # logger.debug(f"{request.method} {request.path} from {request.remote_addr}")
    pass


@app.after_request
def after_request(response):
    """Add headers to response"""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    return response


if __name__ == '__main__':
    # Store start time for uptime calculation
    app.config['START_TIME'] = time.time()
    
    print("=" * 70)
    print("🚀 Flask API Server - Multi-threaded Mode")
    print("=" * 70)
    print(f"📍 Server URL: http://localhost:5000")
    print(f"⏱️  API Latency: 10ms - 2000ms (simulating real-world delays)")
    print(f"⚡ JSON Parser: orjson (high-performance)")
    print(f"📦 Project Manager: uv (ultra-fast)")
    print(f"🧵 Threading: Enabled (handles concurrent requests)")
    print("=" * 70)
    print("\n📋 Available Endpoints:")
    print("  POST /capitalize     - Capitalize first word")
    print("  POST /line_length    - Get line length")
    print("  GET  /health         - Health check")
    print("  GET  /metrics        - Server metrics & request counts")
    print("  POST /reset_metrics  - Reset metrics counters")
    print("=" * 70)
    print("\n✨ Starting server...\n")
    
    # Run Flask with threading enabled
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,              # Disable debug mode for better performance
        threaded=True,            # Enable threading - CRITICAL for concurrent requests
        use_reloader=False,       # Disable reloader for production
        # processes=1,            # Use threads, not processes
    )