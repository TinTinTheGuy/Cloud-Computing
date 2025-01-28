# Adapted from example code in "Exploration - Implementing Auth Using JWTs"
# Canvas - CS493 Cloud App Development, Module 7: Security and JWTs.

from flask import Flask, request, jsonify, render_template
from google.cloud import datastore

import requests
import json

from six.moves.urllib.request import urlopen
from jose import jwt
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = 'SECRET_KEY'

client = datastore.Client()
query = client.query(kind='Your_Entity_Kind')
results = list(query.fetch())
for entity in results:  
    client.delete(entity.key)

CLIENT_ID = 'KGAku915F74tc4Lp2L5HyDs0K7zILFnK'
CLIENT_SECRET = 'vCi5NS_5PtUAR118arw6Vqe2xQc7FBpgbk7i7-e5Oy21aSn0c8ElsQZJxvDNgVBG'
DOMAIN = 'dev-27m0fz7aferqa705.us.auth0.com'

ALGORITHMS = ["RS256"]

oauth = OAuth(app)

auth0 = oauth.register(
    'auth0',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    api_base_url="https://" + DOMAIN,
    access_token_url="https://" + DOMAIN + "/oauth/token",
    authorize_url="https://" + DOMAIN + "/authorize",
    client_kwargs={
        'scope': 'openid profile email',
    },
)

# This code is adapted from https://auth0.com/docs/quickstart/backend/python/01-authorization?_ga=2.46956069.349333901.1589042886-466012638.1589042885#create-the-jwt-validation-decorator

class AuthError(Exception):
    def __init__(self, error, status_code):
        self.error = error
        self.status_code = status_code


@app.errorhandler(AuthError)
def handle_auth_error(ex):
    response = jsonify(ex.error)
    response.status_code = ex.status_code
    return response

# Verify the JWT in the request's Authorization header
def verify_jwt(request):
    if 'Authorization' in request.headers:
        auth_header = request.headers['Authorization'].split()
        token = auth_header[1]
    else:
        return None
    
    jsonurl = urlopen("https://"+ DOMAIN+"/.well-known/jwks.json")
    jwks = json.loads(jsonurl.read())
    try:
        unverified_header = jwt.get_unverified_header(token)
    except jwt.JWTError:
        return None
    if unverified_header["alg"] == "HS256":
        return None
    rsa_key = {}
    for key in jwks["keys"]:
        if key["kid"] == unverified_header["kid"]:
            rsa_key = {
                "kty": key["kty"],
                "kid": key["kid"],
                "use": key["use"],
                "n": key["n"],
                "e": key["e"]
            }
    if rsa_key:
        try:
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=ALGORITHMS,
                audience=CLIENT_ID,
                issuer="https://"+ DOMAIN+"/"
            )
        except jwt.ExpiredSignatureError:
            return None
        except jwt.JWTClaimsError:
            return None
        except Exception:
            return None
        return payload
    else:
        return None

@app.route('/')
def index():
    return "Please navigate to /lodgings to use this API"\
    
@app.route('/businesses', methods=['POST'])
def businesses_post():
    content = request.get_json()

    if check_attributes(content) is False:
        return {"Error": "The request body is missing at least one of the required attributes"}, 400

    payload = verify_jwt(request)
    if not payload:
        return {"Error": "Invalid JWT / Unauthorized access."}, 401

    new_business = datastore.entity.Entity(client.key("businesses"))
    new_business.update({
        "owner_id": payload.get("sub"),
        **content 
    })

    client.put(new_business)
    new_business['id'] = new_business.key.id 

    self_link = f"{request.url_root}businesses/{new_business['id']}"

    response_data = {
        "id": new_business['id'],
        "owner_id": payload.get("sub"),
        **content,  
        "self": self_link 
    }

    return jsonify(response_data), 201


@app.route('/businesses/<int:id>', methods=['GET'])
def get_business(id):
    payload = verify_jwt(request)
    if payload is None:
        return {"Error": "Invalid JWT / Unauthorized access."}, 401

    business_key = client.key("businesses", id)
    business = client.get(key=business_key)
    if not business or payload['sub'] != business['owner_id']:
        return {"Error": "No business with this business_id exists"}, 403

    business['id'] = business.key.id
    self_link = f"{request.url_root}businesses/{business['id']}"
    return jsonify({**business, "self": self_link}), 200


@app.route('/businesses', methods=['GET'])
def businesses_list():
    payload = verify_jwt(request)
    base_url = request.url_root
    sub = payload.get('sub') if payload else None
    
    query = client.query(kind="businesses")
    if sub:
        query.add_filter('owner_id', '=', sub)

    results = query.fetch()
    businesses = [
        {
            "id": r.key.id,
            "owner_id": r.get('owner_id'),
            "name": r.get('name'),
            "street_address": r.get('street_address'),
            "city": r.get('city'),
            "state": r.get('state'),
            "zip_code": r.get('zip_code'),
            "inspection_score": r.get('inspection_score') if sub else None,
            "self": f"{base_url}businesses/{r.key.id}"
        }
        for r in results
    ]

    return jsonify(businesses), 200


@app.route('/businesses/<int:id>', methods=['DELETE'])
def delete_business(id):
    payload = verify_jwt(request)
    if payload is None:
        return {"Error": "Invalid JWT / Unauthorized access."}, 401

    business_key = client.key("businesses", id)
    business = client.get(key=business_key)
    
    if not business or payload['sub'] != business['owner_id']:
        return {"Error": "No business with this business_id exists"}, 403

    client.delete(business_key)
    return '', 204


def check_attributes(content):
    return all(field in content and content[field] is not None for field in ['name','street_address', 'city', 'state', 'zip_code', 'inspection_score'])


# Decode the JWT supplied in the Authorization header
@app.route('/decode', methods=['GET'])
def decode_jwt():
    payload = verify_jwt(request)
    return payload                  

# Generate a JWT from the Auth0 domain and return it
# Request: JSON body with 2 properties with "username" and "password"
#       of a user registered with this Auth0 domain
# Response: JSON with the JWT as the value of the property id_token

@app.route('/login', methods=['POST'])
def login_user():
    content = request.get_json()
    username = content["username"]
    password = content["password"]
    body = {'grant_type':'password','username':username,
            'password':password,
            'client_id':CLIENT_ID,
            'client_secret':CLIENT_SECRET
           }
    headers = { 'content-type': 'application/json' }
    url = 'https://' + DOMAIN + '/oauth/token'
    r = requests.post(url, json=body, headers=headers)
    return r.text, 200, {'Content-Type':'application/json'}

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)