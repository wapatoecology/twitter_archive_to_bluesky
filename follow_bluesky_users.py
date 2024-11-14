import requests
import pandas as pd
import time

# -------------------------
# Instructions for CSV File Preparation and Running the Script
# -------------------------
# To use this script, start by preparing a CSV file with a list of Twitter handles:
# 
# 1. Get your Twitter archive from Twitter, which includes lists of followers and accounts you follow.
#    - Extract this archive and locate the followers or following list.
#    - Use this file as a base for your CSV.
#
# 2. Modify the file so it has a single column titled "Handle."
#    - The "Handle" column should contain just the Twitter usernames without the "@" symbol.
#    - Example format:
# 
#        Handle
#        username1
#        username2
#        username3
# 
# 3. Save this CSV as 'twitter_handles.csv' and place it in the same directory as this script.
# 
# 4. Run the script by opening a terminal or command prompt, navigating to the script’s folder, and entering:
# 
#        python follow_bluesky_users.py
#
# The script will use this list to attempt to find and follow each Twitter follower on Bluesky.
# Results are saved in 'follow_results.csv' in the same directory.

# -------------------------
# User Configuration Section
# -------------------------

# Replace with your Bluesky username and password
BLUESKY_USERNAME = 'your_bluesky_username'
BLUESKY_PASSWORD = 'your_bluesky_password'

# Path to your CSV file containing Twitter handles
CSV_FILE_PATH = 'twitter_handles.csv'  # Ensure the CSV is in the same folder as the script

# Delay between each request in seconds to avoid rate limits
REQUEST_DELAY = 1  

# Domain suffix to check for Bluesky handles (most users use .bsky.social)
DOMAIN_SUFFIX = '.bsky.social'

# Configurations for handling rate limits
REQUEST_LIMIT = 3000  # Number of requests before a pause
PAUSE_DURATION = 600  # Pause duration in seconds (10 minutes)

# -------------------------
# End of User Configuration
# -------------------------

def authenticate(username, password):
    """
    Authenticate with the Bluesky API and return an access token and user DID.
    
    Parameters:
        username (str): Bluesky username.
        password (str): Bluesky password.

    Returns:
        tuple: Access token and DID if authentication is successful.
    """
    auth_url = 'https://bsky.social/xrpc/com.atproto.server.createSession'
    auth_payload = {
        'identifier': username,
        'password': password
    }
    auth_response = requests.post(auth_url, json=auth_payload)
    
    # Check if authentication was successful
    if auth_response.status_code == 200:
        auth_data = auth_response.json()
        access_token = auth_data['accessJwt']
        user_did = auth_data['did']
        print('Authentication successful.')
        return access_token, user_did
    else:
        # Exit if authentication fails, providing details
        print('Authentication failed.')
        print(f"Status Code: {auth_response.status_code}")
        print(f"Response: {auth_response.text}")
        exit()

def resolve_handle(handle, headers):
    """
    Attempt to resolve a Bluesky handle to retrieve the DID.
    
    Parameters:
        handle (str): Bluesky handle to resolve.
        headers (dict): Headers including the access token for authentication.

    Returns:
        str or None: DID of the handle if found; None if not found.
    """
    resolve_url = f'https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle={handle}'
    
    while True:
        response = requests.get(resolve_url, headers=headers)
        
        # Check for rate limit and pause if needed
        if response.status_code == 429:
            print("Rate limit reached while resolving handle. Pausing for 10 minutes...")
            time.sleep(PAUSE_DURATION)
        
        # Re-authenticate if session expired
        elif response.status_code == 401:
            print("Session expired. Re-authenticating...")
            headers['Authorization'], _ = authenticate(BLUESKY_USERNAME, BLUESKY_PASSWORD)
        
        # Handle successful response and return DID if available
        elif response.status_code == 200:
            data = response.json()
            return data.get('did', None)
        
        # Exit loop and return None for all other response statuses
        else:
            return None

def follow_user(session_did, target_did, headers):
    """
    Send a follow request to the target user using their DID.
    
    Parameters:
        session_did (str): The DID of the authenticated user.
        target_did (str): The DID of the user to follow.
        headers (dict): Headers including the access token for authentication.

    Returns:
        Response object: The API response.
    """
    follow_url = 'https://bsky.social/xrpc/com.atproto.repo.createRecord'
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())  # Format timestamp in UTC
    follow_payload = {
        "collection": "app.bsky.graph.follow",
        "repo": session_did,
        "record": {
            "subject": target_did,
            "createdAt": timestamp,
            "$type": "app.bsky.graph.follow"
        }
    }
    
    while True:
        response = requests.post(follow_url, headers=headers, json=follow_payload)
        
        # Check for rate limit and pause if needed
        if response.status_code == 429:
            print("Rate limit reached while following user. Pausing for 10 minutes...")
            time.sleep(PAUSE_DURATION)
        
        # Re-authenticate if session expired
        elif response.status_code == 401:
            print("Session expired. Re-authenticating...")
            headers['Authorization'], _ = authenticate(BLUESKY_USERNAME, BLUESKY_PASSWORD)
        
        # Return the response for other statuses
        else:
            return response

def main():
    # Authenticate initially to retrieve access token and user DID
    access_token, session_did = authenticate(BLUESKY_USERNAME, BLUESKY_PASSWORD)
    
    # Set headers for future requests with the access token
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Load Twitter handles from the specified CSV file
    handles_df = pd.read_csv(CSV_FILE_PATH)
    print("Columns in handles_df:", handles_df.columns.tolist())

    # Check if 'Handle' column exists; exit if not found
    if 'Handle' not in handles_df.columns:
        print("Column 'Handle' not found in CSV.")
        print("Please ensure your CSV has a column named 'Handle'.")
        exit()

    # Convert the 'Handle' column to a list
    handles = handles_df['Handle'].tolist()

    # Prepare a list to store follow attempt results
    results = []

    for i, handle in enumerate(handles, start=1):
        # Construct Bluesky handle with .bsky.social suffix
        bluesky_handle = f"{handle}{DOMAIN_SUFFIX}"

        # Resolve handle to get the user's DID
        target_did = resolve_handle(bluesky_handle, headers)

        # Follow the user if the DID was found
        if target_did:
            follow_response = follow_user(session_did, target_did, headers)
            if follow_response.status_code == 200:
                print(f"Successfully followed {bluesky_handle}.")
                results.append({
                    'Twitter Handle': handle,
                    'Bluesky Handle': bluesky_handle,
                    'Followed': True,
                    'Message': 'Success'
                })
            else:
                print(f"Failed to follow {bluesky_handle}: {follow_response.text}")
                results.append({
                    'Twitter Handle': handle,
                    'Bluesky Handle': bluesky_handle,
                    'Followed': False,
                    'Message': follow_response.text
                })
        else:
            print(f"No matching Bluesky account for Twitter handle {handle}.")
            results.append({
                'Twitter Handle': handle,
                'Bluesky Handle': '',
                'Followed': False,
                'Message': 'No matching Bluesky account'
            })

        # Pause between requests to reduce rate-limit risk
        time.sleep(REQUEST_DELAY)

        # Pause every 3,000 requests for rate limit reset
        if i % REQUEST_LIMIT == 0:
            print("Reached 3,000 requests, pausing for 10 minutes to reset rate limit...")
            time.sleep(PAUSE_DURATION)

    # Save follow results to 'follow_results.csv'
    results_df = pd.DataFrame(results)
    results_df.to_csv('follow_results.csv', index=False)
    print("Process completed. Results saved to 'follow_results.csv'.")

if __name__ == '__main__':
    main()
