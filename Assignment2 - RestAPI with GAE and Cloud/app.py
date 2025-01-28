from flask import Flask, request, jsonify
from google.cloud import datastore

app = Flask(__name__)
client = datastore.Client()

# Constants
BUSINESS_ENTITY = "Business"
REVIEW_ENTITY = "Review"
HTTP_STATUS = {
    "CREATED": 201,
    "OK": 200,
    "NO_CONTENT": 204,
    "BAD_REQUEST": 400,
    "NOT_FOUND": 404,
    "CONFLICT": 409
}

def validate_fields(data, required_fields):
    """Check for missing fields in request data."""
    missing_fields = [field for field in required_fields if field not in data]
    return missing_fields

def entity_not_found(entity):
    """Return a standard not found error."""
    return jsonify({"Error": f"No {entity} with this business_id exists"}), HTTP_STATUS["NOT_FOUND"]

def review_not_found(entity):
    """Return a standard not found error."""
    return jsonify({"Error": f"No {entity} with this review_id exists"}), HTTP_STATUS["NOT_FOUND"]

def missing_fields_response(missing_fields):
    """Return a standardized response for missing fields."""
    return jsonify({"Error": "The request body is missing at least one of the required attributes"}), HTTP_STATUS["BAD_REQUEST"]

@app.route("/", methods=['GET'])
def welcome_route():
    return "Assignment 2 - Tan Ton"

@app.route("/businesses", methods=['POST'])
def create_business():
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'state', 'city', 'zip_code']
    
    missing_fields = validate_fields(data, required_fields)
    if missing_fields:
        return missing_fields_response(missing_fields)

    new_business = datastore.Entity(key=client.key(BUSINESS_ENTITY))
    new_business.update(data)

    client.put(new_business)
    new_business['id'] = new_business.key.id

    return jsonify(new_business), HTTP_STATUS["CREATED"]

@app.route("/businesses/<int:business_id>", methods=['GET'])
def get_business_by_id(business_id):
    key = client.key(BUSINESS_ENTITY, business_id)
    business = client.get(key)

    if not business:
        return entity_not_found("business")

    business['id'] = business.key.id
    return jsonify(business), HTTP_STATUS["OK"]

@app.route("/businesses", methods=['GET'])
def get_all_businesses():
    query = client.query(kind=BUSINESS_ENTITY)
    results = list(query.fetch())
    
    for business in results:
        business['id'] = business.key.id

    return jsonify(results), HTTP_STATUS["OK"]

@app.route("/businesses/<int:business_id>", methods=['DELETE'])
def delete_business_by_id(business_id):
    key = client.key(BUSINESS_ENTITY, business_id)
    business = client.get(key)
    
    if not business:
        return entity_not_found("business")

    # Delete associated reviews
    query = client.query(kind=REVIEW_ENTITY)
    query.add_filter('business_id', '=', business_id)
    reviews = list(query.fetch())

    for review in reviews:
        client.delete(review.key)

    client.delete(key)
    return '', HTTP_STATUS["NO_CONTENT"]

@app.route("/businesses/<int:business_id>", methods=['PUT'])
def edit_business(business_id):
    key = client.key(BUSINESS_ENTITY, business_id)
    business = client.get(key)

    if not business:
        return entity_not_found("business")

    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'zip_code']
    missing_fields = validate_fields(data, required_fields)

    if missing_fields:
        return entity_not_found(missing_fields)

    business.update(data)
    client.put(business)
    
    business['id'] = business.key.id
    return jsonify(business), HTTP_STATUS["OK"]

@app.route("/owners/<int:owner_id>/businesses", methods=['GET'])
def list_businesses_for_owner(owner_id):
    query = client.query(kind=BUSINESS_ENTITY)
    query.add_filter('owner_id', '=', owner_id)
    businesses = list(query.fetch())
    
    for business in businesses:
        business['id'] = business.key.id

    return jsonify(businesses), HTTP_STATUS["OK"]

@app.route("/reviews", methods=['POST'])
def create_review():
    data = request.get_json()
    required_fields = ['user_id', 'business_id', 'stars']
    
    missing_fields = validate_fields(data, required_fields)
    if missing_fields:
        return missing_fields_response(missing_fields)

    # Check if the business exists
    business_key = client.key(BUSINESS_ENTITY, data['business_id'])
    business = client.get(business_key)
    if not business:
        return entity_not_found("business")

    # Check for existing review
    query = client.query(kind=REVIEW_ENTITY)
    query.add_filter('user_id', '=', data['user_id'])
    query.add_filter('business_id', '=', data['business_id'])
    existing_review = list(query.fetch(limit=1))
    if existing_review:
        return jsonify({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), HTTP_STATUS["CONFLICT"]

    # Create a new review
    review = datastore.Entity(key=client.key(REVIEW_ENTITY))
    review.update(data)
    
    client.put(review)
    review['id'] = review.key.id
    return jsonify(review), HTTP_STATUS["CREATED"]

@app.route("/reviews/<int:review_id>", methods=['GET'])
def get_review_by_id(review_id):
    key = client.key(REVIEW_ENTITY, review_id)
    review = client.get(key)

    if not review:
        return review_not_found("review")

    review['id'] = review.key.id
    return jsonify(review), HTTP_STATUS["OK"]

@app.route("/reviews/<int:review_id>", methods=['PUT'])
def update_review(review_id):
    data = request.get_json()
    required_fields = ['stars']

    missing_fields = validate_fields(data, required_fields)
    if missing_fields:
        return missing_fields_response(missing_fields)

    key = client.key(REVIEW_ENTITY, review_id)
    review = client.get(key)

    if not review:
        return review_not_found("review")

    review.update({
        'stars': data['stars'],
        'review_text': data.get('review_text', review.get('review_text'))
    })

    client.put(review)
    review['id'] = review.key.id
    return jsonify(review), HTTP_STATUS["OK"]

@app.route("/reviews/<int:review_id>", methods=['DELETE'])
def delete_review(review_id):
    key = client.key(REVIEW_ENTITY, review_id)
    review = client.get(key)

    if not review:
        return review_not_found("review")

    client.delete(key)
    return '', HTTP_STATUS["NO_CONTENT"]

@app.route("/users/<int:user_id>/reviews", methods=['GET'])
def list_reviews_for_user(user_id):
    query = client.query(kind=REVIEW_ENTITY)
    query.add_filter('user_id', '=', user_id)
    reviews = list(query.fetch())

    for review in reviews:
        review['id'] = review.key.id

    return jsonify(reviews), HTTP_STATUS["OK"]

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
