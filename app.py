import logging
from flask import Flask, request

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print('Hello')

@app.route('/', methods=['GET', 'POST'])
def log_request():
    # Log incoming request
    print(f"Incoming request: {request}")
    return 'OK'
@app.route('/api', methods=['POST'])
def api():
    data = request.get_json()
    logger.info(f'Received POST request with data: {data}')
    
    # Your Flask code here
    
    return 'Success'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
