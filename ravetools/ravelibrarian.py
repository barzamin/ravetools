import rapidfuzz
from pathlib import Path
from pprint import pprint
from dataclasses import dataclass
from typing import Iterator, Any, Self

from dotenv import load_dotenv
import click
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from tinytag import TinyTag

import IPython


load_dotenv()

TrackInfo = dict

def fetch_playlist_items(sp: spotipy.Spotify, uri: str, fields = None) -> list[TrackInfo]:
    offset = 0
    tracks = []
    while True:
        res = sp.playlist_items(uri, offset=offset, fields=fields+',total' if fields else None)
        if len(res['items']) == 0: break

        tracks.extend(res['items'])
        offset = offset + len(res['items'])

    return tracks


@dataclass
class Track:
    title: str
    artist: str

    def title_match_score(self, other: Self) -> bool:
        return rapidfuzz.fuzz.partial_ratio(self.title, other.title)

    def artist_match_score(self, other: Self) -> bool:
        if self.artist:
            return rapidfuzz.fuzz.partial_ratio(self.artist, other.artist)
        else:
            return False

@dataclass
class SpotifyTrack(Track):
    spotify_id: str

    @classmethod
    def from_track(cls, item: dict[str, Any]) -> Self:
        return SpotifyTrack(
            spotify_id=item['id'],
            title=item['name'],
            artist=item['artists'][0]['name'],
        )

@dataclass
class CrateTrack(Track):
    path: Path
    tags: TinyTag

    @classmethod
    def from_file_tags(cls, path: Path, tags: TinyTag):
        return cls(
            title=tags.title,
            artist=tags.artist,
            path=path,
            tags=tags
        )

def _read_crate(path) -> Iterator[CrateTrack]:
    crate = Path(path)

    for p in crate.iterdir():
        if not p.is_file() or not p.suffix.lower() in TinyTag.SUPPORTED_FILE_EXTENSIONS: continue

        yield CrateTrack.from_file_tags(p, TinyTag.get(p))

def read_crate(*args, **kwargs) -> list[CrateTrack]:
    return list(_read_crate(*args, **kwargs))


TITLE_MATCH_THRESHOLD = 70.0
ARTIST_MATCH_THRESHOLD = 70.0
N_POTENTIAL_MATCHES = 5

def reconcile(
    crate: list[CrateTrack],
    online: list[SpotifyTrack]
) -> tuple[
    list[tuple[CrateTrack, SpotifyTrack]],
    list[SpotifyTrack]
]:
    online_names = [track.title for track in online]
    online_only = set(range(len(online)))

    pairs = []

    for offline_item in crate:
        potential_matches = rapidfuzz.process.extract(
            offline_item.title,
            online_names,
            scorer=rapidfuzz.fuzz.WRatio,
            limit=N_POTENTIAL_MATCHES
        )
        for _, score, idx in potential_matches:
            if score < TITLE_MATCH_THRESHOLD: continue

            matched_item = online[idx]
            if offline_item.artist_match_score(matched_item) < ARTIST_MATCH_THRESHOLD:
                continue

            pairs.append((offline_item, matched_item))
            online_only.discard(idx)
            break

    return pairs, [online[i] for i in online_only]

@click.command()
@click.option('--spotify-client-id', envvar='SPOTIFY_CLIENT_ID')
@click.option('--spotify-client-secret', envvar='SPOTIFY_CLIENT_SECRET')
@click.argument('playlist_uri')
@click.argument('crate', type=click.Path(exists=True))
def cli(spotify_client_id, spotify_client_secret, playlist_uri, crate):
    auth_manager = SpotifyClientCredentials(spotify_client_id, spotify_client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    playlist_items = fetch_playlist_items(sp, playlist_uri)
    spotify_tracks = [SpotifyTrack.from_track(i['track']) for i in playlist_items]

    crate_tracks = read_crate(crate)

    pairs, online_only = reconcile(crate_tracks, spotify_tracks)

    for offline, online in pairs:
        print(f'MATCH {offline.artist} - {offline.title} = {online.artist} - {online.title} ({online.spotify_id})')

    for track in online_only:
        print(f'MISSING {track.artist} - {track.title} ({track.spotify_id})')

