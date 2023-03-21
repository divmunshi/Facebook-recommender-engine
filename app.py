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
@app.route('/evt', methods=['POST'])
def evt():
    data = request.get_json()
    logger.info(f'Received evt POST request with data: {data}')
    
    # Your Flask code here
    
    return 'Success'
@app.route('/item', methods=['POST'])
def item():
    data = request.get_json()
    logger.info(f'Received item POST request with data: {data}')
    
    # Your Flask code here
    
    return 'Success'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
