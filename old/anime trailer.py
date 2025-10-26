import requests

def fetch_trailer_data():
    """
    Replicates the JavaScript fetch request to get trailer data.
    """
    
    # The URL for the POST request
    url = "https://web7.topcinema.cam/wp-content/themes/movies2023/Ajaxat/Home/LoadTrailer.php"
    
    # The headers dictionary, translated from the JS fetch options
    headers = {
        "accept": "*/*",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "priority": "u=1, i",
        "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
        "sec-ch-ua-arch": "\"x86\"",
        "sec-ch-ua-bitness": "\"64\"",
        "sec-ch-ua-full-version": "\"141.0.7390.108\"",
        "sec-ch-ua-full-version-list": "\"Google Chrome\";v=\"141.0.7390.108\", \"Not?A_Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"141.0.7390.108\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": "\"\"",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-ch-ua-platform-version": "\"10.0.0\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        # Note: Cookies are session-specific and may expire. 
        # This long cookie string is included as requested, but if the script
        # fails, a stale cookie is the most likely reason.
        "cookie": "_gid=GA1.2.139640073.1761358227; cf_clearance=ERvsN.qvapxCh5mwI_UZ9fUR88VsXbHGhDuFuKbv3Zk-1761439030-1.2.1.1-HYUDbAMPrB.Jix1L.rTS8p5kUhc.HaKBp8KNAQq58.PyNYTzr8mt6ekP_DVvvBhk3KbxBul0MloKW_QKxLHzPAMFnqBncWfCc73gIDqD67cIk0r3pVr.86WgPUTqYQr3eSQRf8e2PJ_rltkRKv9DtxrPeCuPsMpg3lCdC2oDKDRvV4l46F9sni8lF_4OOwDXYb608JOYw1lTV6svhOJhcCIzOUOFPngGbWQSRoMkY0s; prefetchAd_6969551=true; _ga_6ZDPCTTMZN=GS2.1.s1761465735$o42$g1$t1761470056$j42$l0$h0; _ga=GA1.2.1204073573.1760472053",
        "Referer": "https://web7.topcinema.cam/%d8%a7%d9%86%d9%85%d9%8a-%d9%88%d9%86-%d8%a8%d9%8a%d8%b3-one-piece-%d8%a7%d9%84%d8%ad%d9%84%d9%82%d8%a9-1146-%d9%85%d8%aa%d8%b1%d8%ac%d9%85%d8%a9/"
    }
    
    # The body of the request.
    # The requests library will automatically URL-encode this dictionary
    # to match the 'application/x-www-form-urlencoded' content-type.
    data = {
        "href": "https://web7.topcinema.cam/series/%d8%a7%d9%86%d9%85%d9%8a-kaoru-hana-wa-rin-to-saku-%d9%85%d8%aa%d8%b1%d8%ac%d9%85/"
    }
    
    try:
        # Make the POST request
        response = requests.post(url, headers=headers, data=data)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        # Print the response content (e.g., the trailer HTML)
        print("--- Response Content ---")
        print(response.text)
        
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")

if __name__ == "__main__":
    # To run this script, you need to install the 'requests' library:
    # pip install requests
    fetch_trailer_data()
