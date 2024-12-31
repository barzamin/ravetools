import unicodedata
import re

import requests
from requests.adapters import HTTPAdapter, Retry


SEARCH_BASE = 'https://genius.com/api/search'


def _str_normalize(x):
    return unicodedata.normalize('NFKC', x.translate({0x2019: None, 0x200b: None}).strip().lower())


class Genius:
    def __init__(self):
        retries = Retry(total=5, backoff_factor=0.1)
        adapter = HTTPAdapter(max_retries=retries)
        self._session = requests.Session()
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        title_skip_patterns = [
            'track\\s?list',
            'album art(work)?',
            'liner notes',
            'booklet',
            'credits',
            'interview',
            'skit',
            'instrumental',
            'setlist',
        ]
        self._title_skip_re = re.compile('|'.join(f'({p})' for p in title_skip_patterns))

    def _get_json(self, url: str, **kwargs):
        return self._session.get(url, **kwargs).json()

    def _search_type(self, q: str, type: str = 'song', *,  per_page: int = None, page: int = None):
        resp = self._get_json(f'{SEARCH_BASE}/{type}',
                         params={
                             'q': q,
                             'per_page': per_page,
                             'page': page,
                         })

        return resp

    @staticmethod
    def _title_matches(us: str, other: str) -> bool:
        return _str_normalize(us) == _str_normalize(other)

    def _has_lyrics(self, song) -> bool:
        if song['lyrics_state'] != 'complete' or song.get('instrumental'):
            return False

        if self._title_skip_re.search(song['title']): return False


    def search_song(self, title: str, artist: str):
        resp = self._search_type(f'{title} {artist}', 'song')
        hits = resp['response']['sections'][0]['hits']

        # try to find an exact match
        for song in hits:
            song = song['result']
            if self._title_matches(song['title'], title):
                return song

        # couldn't find a title match. pick the first song which actually has lyrics
        for song in hits:
            song = song['result']
            if self._has_lyrics(song):
                return song

        return None
