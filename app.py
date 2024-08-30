import logging
from flask import Flask, request, jsonify
from db_utils import save_data_to_db

# Set up logging
logging.basicConfig(filename='app.log', level=logging.ERROR, 
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')

app = Flask(__name__)

@app.route('/api/webhook', methods=['POST'])
def webhook():
    try:
        incoming_data = request.json
        if isinstance(incoming_data, list):
            save_data_to_db(incoming_data)  # If data is a list of records
        else:
            save_data_to_db([incoming_data])  # Wrap single record in a list
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        # Log the error
        app.logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='162.254.34.181', debug=True, port=3000)
