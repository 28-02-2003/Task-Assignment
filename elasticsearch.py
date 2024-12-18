from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch

# Initialize the Flask app and Elasticsearch connection
app = Flask(__name__)
es = Elasticsearch([{'host': 'localhost', 'port': 8989, 'scheme': 'http'}])

# Route to create a collection (index) using the POST method
@app.route('/create_collection', methods=['POST'])
def create_collection():
    index_name = request.json.get('name')  # Get the collection name from the request body
    
    if not index_name:
        return jsonify({"error": "Index name is required"}), 400

    # Check if the index already exists
    if es.indices.exists(index=index_name):
        return jsonify({"message": f"Collection '{index_name}' already exists."}), 400
    
    # Create the index (collection) with the specified name
    es.indices.create(index=index_name)
    return jsonify({
        "acknowledged": True,
        "shards_acknowledged": True,
        "index": index_name
    }), 201

# Route to index data into a collection (index)
@app.route('/index_data', methods=['POST'])
def index_data():
    collection_name = request.json.get('collection_name')
    data = request.json.get('data')
    
    if not collection_name or not data:
        return jsonify({"error": "Collection name and data are required"}), 400
    
    for record in data:
        es.index(index=collection_name, document=record)
    
    return jsonify({"message": "Data indexed successfully"}), 200

# Route to search data by column in a collection
@app.route('/search_by_column', methods=['GET'])
def search_by_column():
    collection_name = request.args.get('collection_name')
    column_name = request.args.get('column_name')
    column_value = request.args.get('column_value')

    if not collection_name or not column_name or not column_value:
        return jsonify({"error": "Collection name, column name, and column value are required"}), 400

    query = {
        "query": {
            "match": {
                column_name: column_value
            }
        }
    }

    try:
        result = es.search(index=collection_name, body=query)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    hits = result['hits']['hits']
    serializable_result = {
        "total_hits": result['hits']['total']['value'],
        "hits": [{"_id": hit["_id"], "_source": hit["_source"]} for hit in hits]
    }

    return jsonify(serializable_result), 200

# Route to get employee count in a collection
@app.route('/get_emp_count', methods=['GET'])
def get_emp_count():
    collection_name = request.args.get('collection_name')
    
    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400
    
    try:
        response = es.count(index=collection_name)
        return jsonify({"employee_count": response['count']}), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# Route to delete an employee by ID in a collection
@app.route('/del_emp_by_id', methods=['DELETE'])
def del_emp_by_id():
    collection_name = request.args.get('collection_name')
    employee_id = request.args.get('employee_id')
    
    if not collection_name or not employee_id:
        return jsonify({"error": "Collection name and employee ID are required"}), 400
    
    try:
        result = es.delete_by_query(
            index=collection_name,
            body={
                "query": {
                    "term": {"Employee ID": employee_id}
                }
            }
        )
        return jsonify({"message": f"Employee {employee_id} deleted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# Route to get department facets (count of employees by department)
@app.route('/get_dep_facet', methods=['GET'])
def get_dep_facet():
    collection_name = request.args.get('collection_name')
    
    if not collection_name:
        return jsonify({"error": "Collection name is required"}), 400
    
    query = {
        "aggs": {
            "departments": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    
    try:
        result = es.search(index=collection_name, body=query)
        return jsonify(result['aggregations']['departments']['buckets']), 200
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# Start the Flask app on localhost:8989
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8989)
