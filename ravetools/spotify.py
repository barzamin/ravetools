from typing import Any
import requests
import json

SPOTIFY_PARTNER_BASE = "https://api-partner.spotify.com"
SPOTIFY_WEB_URL = "https://open.spotify.com"
SPOTIFY_APP_VERSION = 896000000
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0"

# operationName=queryArtistOverview
# &variables={"uri":"spotify:artist:0ErzCpIMyLcjPiwT4elrtZ","locale":""}
# &extensions={"persistedQuery":{"version":1,"sha256Hash":"4bc52527bb77a5f8bbb9afe491e9aa725698d29ab73bff58d49169ee29800167"}}


class WebplayerGQLClient:
    def __init__(self):
        self.session = requests.Session()

        self.refresh_token()

    def _query(self, op_name: str, query_hash: str, variables: dict[str, Any]):
        resp = self.session.get(
            f"{SPOTIFY_PARTNER_BASE}/pathfinder/v1/query",
            params={
                "operationName": op_name,
                "variables": json.dumps(variables),
                "extensions": json.dumps(
                    {
                        "persistedQuery": {
                            "version": 1,
                            "sha256Hash": query_hash,
                        }
                    }
                ),
            },
            headers=self.headers,
        )

        return resp.json()

    def query_artist_overview(self, id: str):
        resp = self._query(
            "queryArtistOverview",
            "4bc52527bb77a5f8bbb9afe491e9aa725698d29ab73bff58d49169ee29800167",
            {"uri": f"spotify:artist:{id}", "locale": ""},
        )

        return resp

    def _build_headers(self):
        return {
            "User-Agent": UA,
            "Authorization": f"Bearer {self.access_token}",
            "Origin": SPOTIFY_WEB_URL,
            "Referer": f"{SPOTIFY_WEB_URL}/",
            "app-platform": "WebPlayer",
            "spotify-app-version": str(SPOTIFY_APP_VERSION),
        }

    def refresh_token(self):
        # anonymous token
        resp = self.session.get("https://open.spotify.com/get_access_token")
        resp_json = resp.json()
        self.client_id = resp_json["clientId"]
        self.access_token = resp_json["accessToken"]
        self.headers = self._build_headers()


if __name__ == "__main__":
    import sys

    cl = WebplayerGQLClient()
    artist = cl.query_artist_overview(
        sys.argv[1] if len(sys.argv) > 1 else "4zGBj9dI63YIWmZkPl3o7V"
    )  # dj rashad

    name = artist["data"]["artistUnion"]["profile"]["name"]
    monthlies = artist["data"]["artistUnion"]["stats"]["monthlyListeners"]
    print(f"{name} had {monthlies} streams in the last month")
