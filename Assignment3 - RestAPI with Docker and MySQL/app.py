from __future__ import annotations

import logging
import os

from flask import Flask, request, jsonify

import sqlalchemy

from connect_connector import connect_with_connector

app = Flask(__name__)

logger = logging.getLogger()


# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

# create 'lodgings' table in database if it does not already exist
def create_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        
        create_businesses_table_query = sqlalchemy.text (
            '''
            CREATE TABLE IF NOT EXISTS businesses (
                id INT NOT NULL AUTO_INCREMENT,
                owner_id INT NOT NULL,
                name VARCHAR(50) NOT NULL,
                street_address VARCHAR(100) NOT NULL,
                city VARCHAR(50) NOT NULL,
                state CHAR(2) NOT NULL,
                zip_code CHAR(5) NOT NULL,
                PRIMARY KEY (id)
            );
            '''
        )

        create_review_table_query = sqlalchemy.text (
            '''
            CREATE TABLE IF NOT EXISTS review (
                id INT NOT NULL AUTO_INCREMENT,
                user_id INT NOT NULL,
                business_id INT NOT NULL,
                stars INT NOT NULL,
                review_text VARCHAR(1000),
                PRIMARY KEY (id),
                FOREIGN KEY (business_id) REFERENCES businesses(id)
            );
            '''
        )

        conn.execute(create_businesses_table_query)
        conn.execute(create_review_table_query)
        conn.commit()


@app.route('/')
def index():
    return 'Please navigate to /lodgings to use this API'      
            

# Create a business
@app.route('/businesses', methods=['POST'])
def create_businesses():
    content = request.get_json()

    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    missing_fields = [field for field in required_fields if field not in content]
    if missing_fields:
        return jsonify({ "Error": "The request body is missing at least one of the required attributes" }), 400

    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            # Preparing a statement before hand can help protect against injections.
            stmt = sqlalchemy.text(
                'INSERT INTO businesses(owner_id, name, street_address, city, state, zip_code) '
                ' VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
            )
            # connection.execute() automatically starts a transaction
            conn.execute(stmt, parameters={'owner_id': content['owner_id'],
                                        'name': content['name'], 
                                        'street_address': content['street_address'], 
                                        'city': content['city'],
                                        'state': content['state'],
                                        'zip_code': int(content['zip_code'])})
            # The function last_insert_id() returns the most recent value
            # generated for an `AUTO_INCREMENT` column when the INSERT 
            # statement is executed
            stmt2 = sqlalchemy.text('SELECT last_insert_id()')
            # scalar() returns the first column of the first row or None if there are no rows
            business_id = conn.execute(stmt2).scalar()
            # Remember to commit the transaction
            conn.commit()
        
        links = f"{request.base_url}/{business_id}"

        return ({'id': business_id,
                'owner_id': content['owner_id'],
                'name': content['name'], 
                'street_address': content['street_address'], 
                'city': content['city'],
                'state': content['state'],
                'zip_code': int(content['zip_code']),
                'self': links}, 201)
    
    except Exception as e:
        logger.exception(e)
        return ({'Error': 'Unable to create business'}, 500)

# Get buisness by id
@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business(business_id):
    try:
        with db.connect() as conn:
            # Prepare a statement to prevent SQL injection.
            stmt = sqlalchemy.text(
                'SELECT id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE id = :business_id'
            )
            business = conn.execute(stmt, {'business_id': business_id}).one_or_none()
            
            if business:
                return jsonify({
                    'id': business.id,
                    'owner_id': business.owner_id,
                    'name': business.name,
                    'street_address': business.street_address,
                    'city': business.city,
                    'state': business.state,
                    'zip_code': int(business.zip_code),
                    'self': f"{request.base_url}"
                }), 200
            else:
                return jsonify({'Error': 'No business with this business_id exists'}), 404

    except Exception as e:
        # Log the exception details
        return jsonify({'Error': str(e)}), 500

# List all businesses with pagination
@app.route('/businesses', methods=['GET'])
def list_businesses():
    offset = request.args.get('offset', default=0, type=int)
    limit = request.args.get('limit', default=3, type=int)

    try:
        with db.connect() as conn:
            stmt = sqlalchemy.text(
                'SELECT id, owner_id, name, street_address, city, state, zip_code FROM businesses ORDER BY id LIMIT :limit OFFSET :offset'
            )
            businesses = conn.execute(stmt, {'limit': limit, 'offset': offset}).fetchall()
            business_list = []

            for business in businesses:
                business_list.append({
                    'id': business.id,
                    'owner_id': business.owner_id,
                    'name': business.name,
                    'street_address': business.street_address,
                    'city': business.city,
                    'state': business.state,
                    'zip_code': int(business.zip_code),
                    'self': f"{request.base_url}/{business.id}"
                })
            
            # Construct the next_url only if not reaching the last page
            if len(businesses) == limit:
                next_url = f"{request.base_url}?offset={offset + limit}&limit={limit}"  
            else:
                next_url = None

            return jsonify({
                'entries': business_list,
                'next': next_url
            }), 200

    except Exception as e:
        return jsonify({'Error': str(e)}), 500


# Update a business
@app.route('/businesses/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    content = request.get_json()

    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    missing_fields = [field for field in required_fields if field not in content]
    if missing_fields:
        return jsonify({ "Error": "The request body is missing at least one of the required attributes" }), 400

    try:
        with db.connect() as conn:
            # Check if the business exists to handle the 404 Not Found case
            check_stmt = sqlalchemy.text(
                'SELECT id FROM businesses WHERE id = :business_id'
            )
            existing_business = conn.execute(check_stmt, {'business_id': business_id}).one_or_none()
            
            if not existing_business:
                return jsonify({'Error': 'No business with this business_id exists'}), 404

            # If the business exists, update the business with the new details
            update_stmt = sqlalchemy.text(
                '''
                UPDATE businesses
                SET owner_id = :owner_id, name = :name, street_address = :street_address, city = :city, state = :state, zip_code = :zip_code
                WHERE id = :business_id
                '''
            )
            conn.execute(update_stmt, {
                'business_id': business_id,
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': int(content['zip_code'])
            })
            conn.commit()

            # Return the updated business details
            updated_business = conn.execute(check_stmt, {'business_id': business_id}).one_or_none()
            return jsonify({
                'id': updated_business.id,
                'owner_id': content['owner_id'],
                'name': content['name'],
                'street_address': content['street_address'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': int(content['zip_code']),
                'self': f"{request.base_url}"
            }), 200

    except Exception as e:
        # Log the exception and return a server error
        return jsonify({'Error': str(e)}), 500

# Delete a business
@app.route('/businesses/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    try:
        with db.connect() as conn:
            # Check if the business exists
            check_stmt = sqlalchemy.text(
                'SELECT id FROM businesses WHERE id = :business_id'
            )
            existing_business = conn.execute(check_stmt, {'business_id': business_id}).one_or_none()
            
            if not existing_business:
                return jsonify({'Error': 'No business with this business_id exists'}), 404

            # If the business exists, delete any reviews associated with the business first
            delete_reviews_stmt = sqlalchemy.text(
                'DELETE FROM review WHERE business_id = :business_id'
            )
            conn.execute(delete_reviews_stmt, {'business_id': business_id})

            # Now delete the business
            delete_business_stmt = sqlalchemy.text(
                'DELETE FROM businesses WHERE id = :business_id'
            )
            conn.execute(delete_business_stmt, {'business_id': business_id})
            conn.commit()

            # Return no content as everything was deleted successfully
            return ('', 204)

    except Exception as e:
        # Log the exception and return a server error
        return jsonify({'Error': str(e)}), 500

# List a business for owner
@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_businesses_for_owner(owner_id):
    try:
        with db.connect() as conn:
            # Prepare a statement to fetch businesses by owner_id
            stmt = sqlalchemy.text(
                'SELECT id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE owner_id = :owner_id ORDER BY id'
            )
            businesses = conn.execute(stmt, {'owner_id': owner_id}).fetchall()

            # Check if any businesses are found
            if not businesses:
                return jsonify({'Error': 'No businesses found for this owner'}), 404

            # Prepare the business list, including self URLs
            business_list = []
            for business in businesses:
                business_list.append({
                    'id': business.id,
                    'owner_id': business.owner_id,
                    'name': business.name,
                    'street_address': business.street_address,
                    'city': business.city,
                    'state': business.state,
                    'zip_code': int(business.zip_code),
                    'self': f"{request.host_url.rstrip('/')}/businesses/{business.id}"
                })

            # Return the list of businesses
            return jsonify(business_list), 200

    except Exception as e:
        # Log the exception and return a server error
        return jsonify({'Error': str(e)}), 500

# Create a review
@app.route('/reviews', methods=['POST'])
def create_review():
    content = request.get_json()

    required_fields = ['user_id', 'business_id', 'stars']
    missing_fields = [field for field in required_fields if field not in content]
    if missing_fields:
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400

    try:
        with db.connect() as conn:
            # Check if the business exists
            check_business = sqlalchemy.text('SELECT id FROM businesses WHERE id = :business_id')
            business_exists = conn.execute(check_business, {'business_id': content['business_id']}).one_or_none()

            if not business_exists:
                return jsonify({'Error': 'No business with this business_id exists'}), 404

            # Check for existing review from the same user for this business
            check_review = sqlalchemy.text(
                'SELECT id FROM review WHERE user_id = :user_id AND business_id = :business_id'
            )
            review_exists = conn.execute(check_review, {'user_id': content['user_id'], 'business_id': content['business_id']}).one_or_none()

            if review_exists:
                return jsonify({'Error': 'You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review'}), 409

            # Insert new review
            insert_review = sqlalchemy.text(
                'INSERT INTO review (user_id, business_id, stars, review_text) VALUES (:user_id, :business_id, :stars, :review_text)'
            )

            conn.execute(insert_review, {
                'user_id': content['user_id'],
                'business_id': content['business_id'],
                'stars': content['stars'],
                'review_text': content.get('review_text', '')  # Defaulting to empty string if not provided
            })

            last_insert_id = sqlalchemy.text('SELECT last_insert_id()')
            review_id = conn.execute(last_insert_id).scalar()
            conn.commit()

            return jsonify({
                'id': review_id,
                'user_id': content['user_id'],
                'stars': content['stars'],
                'review_text': content.get('review_text', ''),
                'self': f"{request.base_url}/{review_id}",
                'business': f"{request.host_url.rstrip('/')}/businesses/{content['business_id']}"
            }), 201

    except Exception as e:
        return jsonify({'Error': str(e)}), 500

@app.route('/reviews/<int:review_id>', methods=['GET'])
def get_review(review_id):
    try:
        with db.connect() as conn:
            # Prepare a statement to fetch the review by ID
            stmt = sqlalchemy.text(
                '''
                SELECT id, user_id, business_id, stars, review_text
                FROM review
                WHERE id = :review_id
                '''
            )
            review = conn.execute(stmt, {'review_id': review_id}).one_or_none()

            if review is None:
                return jsonify({'Error': 'No review with this review_id exists'}), 404

            # Construct the response with the complete URLs for the review and the business
            review_data = {
                'id': review.id,
                'user_id': review.user_id,
                'stars': review.stars,
                'review_text': review.review_text,
                'self': f"{request.base_url}",
                'business': f"{request.host_url.rstrip('/')}/businesses/{review.business_id}"
            }

            return jsonify(review_data), 200

    except Exception as e:
        # Log the exception details
        logger.exception(f"Failed to fetch review: {e}")
        return jsonify({'Error': str(e)}), 500

@app.route('/reviews/<int:review_id>', methods=['PUT'])
def update_review(review_id):
    content = request.get_json()

    # Check if 'stars' is in the request, as it's required
    if 'stars' not in content:
        return jsonify({'Error': 'The request body is missing at least one of the required attributes'}), 400

    try:
        with db.connect() as conn:
            # Check if the review exists
            check_review = sqlalchemy.text(
                'SELECT * FROM review WHERE id = :review_id'
            )
            review = conn.execute(check_review, {'review_id': review_id}).one_or_none()

            if not review:
                return jsonify({'Error': 'No review with this review_id exists'}), 404
            

            update_params = {
                'review_id': review_id,
                'stars': content['stars']    
            }
            update_values = ['stars = :stars']

            if 'review_text' in content:
                update_values.append('review_text = :review_text')
                update_params['review_text'] = content['review_text']

            update_stmt = sqlalchemy.text(
                f'''
                    UPDATE review SET {", ".join(update_values)} WHERE id = :review_id
                '''
            )

            conn.execute(update_stmt, update_params)
            conn.commit()

            # Fetch the updated review to return in the response
            updated_review = conn.execute(check_review, {'review_id': review_id}).one_or_none()

            return jsonify({
                'id': updated_review.id,
                'user_id': updated_review.user_id,
                'stars': updated_review.stars,
                'review_text': updated_review.review_text,
                'self': f"{request.base_url}",
                'business': f"{request.host_url.rstrip('/')}/businesses/{updated_review.business_id}"
            }), 200

    except Exception as e:
        logger.exception(f"Failed to update review: {e}")
        return jsonify({'Error': str(e)}), 500

@app.route('/reviews/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    try:
        with db.connect() as conn:
            # Check if the review exists
            check_review = sqlalchemy.text(
                'SELECT id FROM review WHERE id = :review_id'
            )
            review = conn.execute(check_review, {'review_id': review_id}).one_or_none()

            if not review:
                return jsonify({'Error': 'No review with this review_id exists'}), 404

            delete_review = sqlalchemy.text(
                'DELETE FROM review WHERE id = :review_id'
            )
            conn.execute(delete_review, {'review_id': review_id})
            conn.commit()

            return ('', 204)

    except Exception as e:
        logger.exception(f"Failed to delete review: {e}")
        return jsonify({'Error': str(e)}), 500

@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def get_reviews_for_user(user_id):
    try:
        with db.connect() as conn:
            # Prepare a statement to fetch all reviews by the user
            stmt = sqlalchemy.text(
                '''
                SELECT id, user_id, business_id, stars, review_text
                FROM review
                WHERE user_id = :user_id
                '''
            )
            reviews = conn.execute(stmt, {'user_id': user_id}).fetchall()

            if not reviews:
                return jsonify([]), 404

            # Construct the response list of reviews
            reviews_data = [
                {
                    'id': review.id,
                    'user_id': review.user_id,
                    'business': f"{request.host_url.rstrip('/')}/businesses/{review.business_id}",
                    'stars': review.stars,
                    'review_text': review.review_text,
                    'self': f"{request.host_url.rstrip('/')}/reviews/{review.id}"
                }
                for review in reviews
            ]

            return jsonify(reviews_data), 200

    except Exception as e:
        # Log the exception details
        logger.exception(f"Failed to fetch reviews for user {user_id}: {e}")
        return jsonify({'Error': str(e)}), 500

if __name__ == '__main__':
    init_db()
    create_table(db)
    app.run(host='0.0.0.0', port=8080, debug=True)