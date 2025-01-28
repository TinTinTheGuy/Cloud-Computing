from flask import Flask, redirect, request, url_for, render_template, session
import requests
import os
import secrets
from google.cloud import datastore

app = Flask(__name__)
app.secret_key = os.urandom(24)  # For session management

CLIENT_ID = ''
CLIENT_SECRET = ''
REDIRECT_URI =  'https://assignment4-renew.uw.r.appspot.com/oauth'
AUTHORIZATION_URL = 'https://accounts.google.com/o/oauth2/v2/auth'
TOKEN_URL = 'https://oauth2.googleapis.com/token'
USER_INFO_URL = 'https://people.googleapis.com/v1/people/me?personFields=names'

datastore_client = datastore.Client()

def store_state(state):
    """Store the randomly generated state in Datastore."""
    entity = datastore.Entity(datastore_client.key("state"))
    entity.update({"state": state})
    datastore_client.put(entity)

def verify_state(state):
    """Verify if the state exists in Datastore and remove it to prevent reuse."""
    query = datastore_client.query(kind="state")
    query.add_filter("state", "=", state)
    results = list(query.fetch())
    if results:
        datastore_client.delete(results[0].key)  # Delete used state
        return True
    return False

@app.route('/')
def welcome():
    print("Welcome page accessed")
    state = secrets.token_urlsafe(16)
    session['state'] = state 
    store_state(state)
    print("Welcome page accessed")
    authorization_url = f"{AUTHORIZATION_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}" \
                        f"&response_type=code&scope=https://www.googleapis.com/auth/userinfo.profile" \
                        f"&state={state}"
    print("Welcome page accessed")
    return render_template('welcome.html', authorization_url=authorization_url)

@app.route('/_ah/warmup')
def warmup():
    return '', 200

@app.route('/oauth', methods=['GET'])
def oauth_callback():
    code = request.args.get('code')
    state = request.args.get('state')
    if not state or not verify_state(state):
        return "State verification failed", 400
    token_response = requests.post(TOKEN_URL, data={
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'code': code,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    })
    token_data = token_response.json()
    access_token = token_data.get('access_token')
    print(access_token)
    if not access_token:
        return "Failed to retrieve access token", 400

    # Use the access token to get the user info
    user_info_response = requests.get(USER_INFO_URL, headers={
        'Authorization': f'Bearer {access_token}'
    })
    user_info = user_info_response.json()
    names = user_info.get("names", [{}])[0]
    given_name = names.get("givenName", "Unknown")
    family_name = names.get("familyName", "Unknown")
    print("Welcome page accessed")


    # Render the user info page with user's name and state
    return render_template('userinfo.html', given_name=given_name, family_name=family_name, state=state)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)
