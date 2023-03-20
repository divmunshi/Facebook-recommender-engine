from flask import Flask, request

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def log_request():
    # Log incoming request
    print(f"Incoming request: {request.method} {request.path}")
    return 'OK'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
