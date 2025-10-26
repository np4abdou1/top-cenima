import requests

def fetch_episode_server_data():
    """
    Replicates the JavaScript fetch request for episode server data 
    using only essential headers.
    """
    
    # The URL for the POST request to retrieve server/embed data
    url = "https://web7.topcinema.cam/wp-content/themes/movies2023/Ajaxat/Single/Server.php"
    
    # Essential headers filtered for reusability (excluding cookies, UA hints, etc.)
    headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest", # Essential for AJAX server check
        # The Referer is often required by the server to validate the request source
        "Referer": "https://web7.topcinema.cam/%d8%a7%d9%86%d9%85%d9%8a-%d9%88%d9%86-%d8%a8%d9%8a%d8%b3-one-piece-%d8%a7%d9%84%d8%ad%d9%84%d9%82%d8%a9-1146-%d9%85%d8%aa%d8%b1%d8%ac%d9%85%d8%a9/watch/"
    }
    
    # The body data, which contains the episode ID (id) and an index (i)
    # The requests library will automatically URL-encode this dictionary.
    data = {
        "id": "177863",
        "i": "0"
    }
    
    try:
        # Make the POST request
        response = requests.post(url, headers=headers, data=data)
        
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
        
        # Print the response content (e.g., HTML for the video player/server list)
        print("--- Server Response Content ---")
        print(response.text)
        
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err}")

if __name__ == "__main__":
    # Ensure you have the 'requests' library installed: pip install requests
    fetch_episode_server_data()
