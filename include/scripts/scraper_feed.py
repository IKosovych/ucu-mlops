import os
import time
from datetime import datetime
from io import StringIO
import requests

import pandas as pd
from google.cloud import storage


URL = "https://assets.msn.com/service/news/feed/pages/weblayout"
HEADERS = {
    'sec-ch-ua-platform': '"macOS"',
    'Referer': 'https://www.msn.com/',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
    'cookie': '_C_Auth=; USRLOC=; MUID=251ABE4B128669581D24AB7313A768DF; _EDGE_S=F=1&SID=0379E5B2942D655010ACF08A9580643D; _EDGE_V=1; MUIDB=251ABE4B128669581D24AB7313A768DF; msnup=; eupubconsent-v2=CQIIHzAQIIHzAAcABBENAaEsAP_gAEPgACiQg1NV_H__bW9r8Xr3aft0eY1H99j77uQxBheJE-4FyBvW_JwXh2EwNA26tqIKmRIEuzZBAQFkHJHURVigSIgVqyHsYkGchTNIJ6BkgFMRI2dYCFxvmYtjMQIY5_p_d3fx2D-t_dv83dzjz8lHnzV5P0ckcAAAA4gNDfl9bRKb-5IOd-78v4v09l_rk2_eTVm_pcvr7B-ufs87_XU-9_YAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEQaoaAACIAFAAXAA4AD4AKAAqABcADkAHgAgABIAC6AGAAZQA0ADUAHAAPAAfgBEACOAEwAKEAUgBTACrAFwAXQAxABmADQAG8APQAfgBCACGgEQARIAjgBLACaAE4AKMAUoAwABhwDKAMsAZoA0QBsgDkAHPAO4A7wB7AD4gH2AfsA_wEAgIOAhABEYCLAIwARqAjgCOgEiAJKASkAnYBPwCgwFQAVEAq4BYgC5gF1gLyAvQBfQDFAGfANeAbQA3ABxADjgHSAOoAdsA9oB9gD_gImARfAjwCPYEiASLAlQCVgExQJkAmUBM4CbQE7AKHgUeBSICk4FNAU2Ap8BUMCpAKlAVUArkBYUCxALFAWUAtEBagC2IFuAW6AuABcgC6AF2gLvgXkBeYC-gF_gMEAYMAw0BiADFgGPAMhgZGBkkDJgMnAZUAywBmYDOQGeANEgaMBo4DTQGpgNVgauBrIDXgG0QNuA28BuQDdAG-AOAAcEA7YB3IDxQHjwPJA8oB8UD5APlAfSA-uB9oH3QP2A_cCAIEBAIGAQPAgjBBMEFAIMAQbAhCBCgCFcELQQuAhiBDOCHIIdQQ8BD0CH4EUwIwARpAjWBG8COIEdAI7AR7Aj6BH8CQgEiAJFASNgkgCSkEmASZAlHBKgEqQJYQSzBLSCW4JcQS6BLsCX0EwATBAmGBMUCY4EyYJmAmcBNICagE2IJtgm5BN4E3wJwhBQCDUAIGApgABAAMAA4ACgAIoATgBYADCAHgAewBCAEQAI4ATAArgBcgDmAPAAhgBEgCLAEuAK0AZ0A2QDgAHGAOcAeQA_ACAAEYAJMAToAosBXgFfALsAX4AzgBtgDeAHHAOaAdQA9QB8gD9gISAR1AkQCRQElwJaAl4BNgCdgFCQKRApIBTYCxQFogLZAXIAugBdwC9AGHgMZAY9AyMDJAGTgM5AZ4A0yBrQGuwNyA3QB3AD3gH8AQFAgcBCuCIIIhARqAjeBHECPgEhwJMgSoEAUAAAgAGAAcABSAEQASQAnACwAGEAPAA9gCEAIgARwAmABXgDmAO4AhgBEgCLAEuAK0AZ0A2QDgAHGAOcAeQA-QB-AEAAIwASaAnQCdgFFgK8Ar4BdgC_AGcANQAbYA3gBxwDmgHqAPkAfsBCQCO4EiASKAkuBLQEvAJsATsAoQBSQCm4FiAWKAtEBbIC5AF0ALuAXoAw8BjIDHgGSAMnAZyA0yBrQGugN0AdwA94B_AEBQIHAQaAhXBEEEQgI1ARvAjiBJkUArABFACoALAAhABMAC4AHgARwA4ACOAFFgK8Ar4BdgC_AGcAN4Ac0A_YCPQEigJeATYAsUBaMC2ALZAXcAvQBh4DOQGeQNaA14BuQD3gH8AQFAgcBDiCIIIhARqAjeBHECPgEhwJMjAEwAIoAVABYAEIAJgAjgBwAEcAKLAV4BXwC7AF-AM4AbwA5oB-wEegJFAS8AmwBaMC2ALZAXcAvQBh4DOQGeQNaA10B7wD4gH8AQFAgcBDiCIIIhARqAjeBHECTI.f_wACHwAAAAA; ntps={"m":"en-gb"}; OptanonAlertBoxClosed=2024-11-15T06:36:28.116Z; _clck=9b574j%7C2%7Cfr0%7C0%7C1781; _clsk=1bu55st%7C1732000082155%7C4%7C0%7Cwww.clarity.ms%2Feus-c-sc%2Fcollect; adslrid=_; _C_ETH=1; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Nov+19+2024+11%3A48%3A15+GMT%2B0200+(Eastern+European+Standard+Time)&version=202310.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CV2STACK42%3A1&AwaitingReconsent=false&geolocation=%3B'
}
PARAMS = {
    "User": "32ECA84BA2B6640D2FCBBD1DA3A46590",
    "activityId": "02B10774-35CC-40E3-820E-93C2E8047B1B",
    "adoffsets": "c1:-1,c2:-1",
    "apikey": "0QfOX3Vn51YCzitbLaRkTTBadtWpgTN8NZLW0C1SEM",
    "audienceMode": "adult",
    "cm": "en-us",
    "colstatus": "c1:0,c2:0",
    "column": "c2",
    "colwidth": "300",
    "contentType": "article,video,slideshow,webcontent",
    "dprValue": "1.8",
    "duotone": "true",
    "it": "web",
    "l3v": "2",
    "layout": "c2",
    "memory": "8",
    "newsSkip": "0",
    "newsTop": "48",
    "ocid": "hponeservicefeed",
    "pgc": "1035",
    "private": "1",
    "scn": "ANON",
    "timeOut": "1000",
    "vpSize": "882x1003",
    "wposchema": "byregion"
}
RECORD_LIMIT = 5000
DESIRED_CATEGORIES = ['lifestyle']

def fetch_stories(url: str, skip: int, page: int) -> pd.DataFrame:
    """
    Fetch stories from MSN service based on given parameters.

    Parameters:
    - url (str): URL to the MSN service endpoint.
    - skip (int): How many stories to skip.

    Returns:
    - DataFrame containing stories fetched from the MSN feed.
    """
    params_with_skip = PARAMS.copy()
    params_with_skip["newsSkip"] = str(skip)
    params_with_skip["newsTop"] = str(page)

    response = requests.get(url, params=params_with_skip, headers=HEADERS)

    print("Fetching stories...")
    if response.status_code == 200:
        response_json = response.json()

        stories = []
        for section in response_json.get('sections', []):
            for card in section.get('cards', []):
                # Append only the necessary fields to the list
                for subcard in card.get("subCards", []):
                    try:
                        date_object = datetime.strptime(subcard.get('publishedDateTime'), '%Y-%m-%dT%H:%M:%SZ')
                    except Exception as e:
                        continue
                        
                    print(f'{date_object.month} month, {params_with_skip["newsTop"]}, {params_with_skip.get("newsSkip")}')
                    stories.append({
                        'id': subcard.get('id'),
                        'title': subcard.get('title'),
                        'category': subcard.get('category'),
                        'url': subcard.get('url'),
                        'publishedDateTime': subcard.get('publishedDateTime')
                    })

        if stories:
            return pd.DataFrame(stories)
        else:
            return pd.DataFrame()
    else:
        time.sleep(3)
        print(f"Failed to fetch stories. Status code: {response.status_code}")
        return pd.DataFrame()

def setup_google_credentials():
    credentials_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_json:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS is not set in environment variables")

    service_account_path = "/tmp/google_credentials.json"
    with open(service_account_path, "w") as f:
        f.write(credentials_json)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

def main():
    """
    Main function to fetch data and save it into a CSV file.
    """
    skip = 0
    page = 0
    all_stories_df = pd.DataFrame()
    fetch_stories(URL, skip, page)

    while skip < RECORD_LIMIT:
        stories_df = fetch_stories(URL, skip, page)
        if not stories_df.empty:
            all_stories_df = pd.concat([all_stories_df, stories_df], ignore_index=True)
            skip += 100
            page += 1
        else:
            break

    filename = os.environ.get("SCRAPER_OUTPUT_FILENAME")
    setup_google_credentials()

    upload_df_to_gcs(
        df=all_stories_df,
        bucket_name=os.environ.get("GCS_BUCKET_NAME"),
        blob_path=filename,
        service_key_path=os.environ.get("SERVICE_KEY_PATH")
    )
    print(f"Unique data successfully saved to {filename}.")

def upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str, service_key_path="gcp_service_key.json"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    print(f"DataFrame uploaded to gs://{bucket_name}/{blob_path}")


if __name__ == "__main__":
    main()
