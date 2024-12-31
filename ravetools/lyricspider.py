import logging
import sqlite3
import sys
import time
from dataclasses import dataclass
from itertools import chain
from multiprocessing import Pool, Process, Queue
from os import PathLike
from typing import Any, Iterator

import click
import requests
import spotipy
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm

from .genius import Genius

load_dotenv()

logger = logging.getLogger(__name__)


class DB:
    MIGRATIONS = [
        """\
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT NOT NULL,
            title TEXT NOT NULL,
            artists TEXT NOT NULL
        );

        CREATE TABLE lyrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id INTEGER REFERENCES tracks(id),
            genius_url TEXT NOT NULL,
            lyrics TEXT NOT NULL
        );

        CREATE UNIQUE INDEX idx_tracks_spotify_id ON tracks(spotify_id);
        """,
        """
        CREATE UNIQUE INDEX idx_lyrics_track_id ON lyrics(track_id);
        """,
    ]

    def __init__(self, database: str | bytes | PathLike[str] | PathLike[bytes]):
        self.conn = sqlite3.connect(database)

    def cursor(self):
        return self.conn.cursor()

    def get_schema_version(self):
        cur = self.cursor()
        cur.execute("PRAGMA user_version")
        return cur.fetchone()[0]

    def migrate(self):
        cur_ver = self.get_schema_version()

        # 0 is fresh, 1 is first migration, etc.
        with self.conn:
            if cur_ver < len(DB.MIGRATIONS):
                ver = cur_ver
                while ver < len(DB.MIGRATIONS):
                    click.echo(f"running migration {i+1}")
                    with self.conn:
                        self.conn.executescript(migration)
                        self.conn.execute(f"PRAGMA user_version = {ver+1:d};")

                    ver += 1


@click.group()
@click.option("--db", default="lyricspider.sqlite")
@click.option("--log-level", default="info")
@click.pass_context
def cli(ctx, db, log_level):
    logging.basicConfig(level=log_level.upper())

    ctx.ensure_object(dict)
    ctx.obj["db"] = DB(db)
    ctx.obj["db"].migrate()


@cli.command()
@click.option("--spotify-client-id", envvar="SPOTIFY_CLIENT_ID")
@click.option("--spotify-client-secret", envvar="SPOTIFY_CLIENT_SECRET")
@click.option("--spotify-redirect-uri", envvar="SPOTIFY_REDIRECT_URI")
@click.option("--page-size", default=25)
@click.pass_context
def sync(
    ctx, spotify_client_id, spotify_client_secret, spotify_redirect_uri, page_size
):
    """sync spotify saved song metadata to the local database."""

    db = ctx.obj["db"]

    auth_manager = SpotifyOAuth(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret,
        redirect_uri=spotify_redirect_uri,
        scope="user-library-read",
    )
    sp = spotipy.Spotify(auth_manager=auth_manager)

    with tqdm() as pbar:
        offset = 0
        while True:
            res = sp.current_user_saved_tracks(limit=page_size, offset=offset)
            nitems = len(res["items"])
            if nitems == 0:
                break

            pbar.total = res["total"]
            pbar.update()

            with db.conn:
                cursor = db.conn.cursor()
                cursor.executemany(
                    """INSERT INTO tracks (spotify_id, title, artists) VALUES (?, ?, ?)
                        ON CONFLICT(spotify_id) DO NOTHING
                    """,
                    [
                        (
                            item["track"]["id"],
                            item["track"]["name"],
                            ", ".join(a["name"] for a in item["track"]["artists"]),
                        )
                        for item in res["items"]
                    ],
                )

            offset += nitems
            pbar.update(nitems)


@dataclass
class SpotifyTrackDetails:
    tid: int
    title: str
    artists: str


@dataclass
class SearchResult:
    track: SpotifyTrackDetails
    genius_result: Any


@dataclass
class LyricsResult:
    track: SpotifyTrackDetails
    genius_result: Any
    lyrics: str


genius_client = None


def worker_genius_search(queue_track_details: Queue, queue_search_results: Queue):
    global genius_client
    if not genius_client:
        genius_client = Genius()  # shared per process

    logger.debug(f"[thread=genius search] booted; {genius_client=}")
    while track_details := queue_track_details.get():
        logger.debug(
            f"[thread=genius search] got request to search for {track_details = }"
        )
        res = genius_client.search_song(
            title=track_details.title, artist=track_details.artists
        )
        queue_search_results.put(SearchResult(track_details, res))


def get_lyrics(session: requests.Session, search_res: SearchResult) -> LyricsResult:
    if not search_res.genius_result:
        # TODO: mark this as "no lyrics on genius" in the db so we
        # don't constantly re-hit the search api endpoint for no reason
        # when doing another lyricspider pull
        return None

    lyrics_page_url = search_res.genius_result["url"]

    resp = session.get(lyrics_page_url)
    bs = BeautifulSoup(resp.content, "lxml")
    el_lyrics = bs.find(attrs={"data-lyrics-container": True})
    if not el_lyrics:
        # TODO: log this too - failure to find lyrics for a song that exists
        return None

    return el_lyrics.get_text("\n")


def worker_genius_lyrics(queue_search_results: Queue, queue_lyrics_results: Queue):
    logger.debug(f"[thread=genius lyrics] booted")
    session = requests.Session()

    while search_res := queue_search_results.get():
        res = LyricsResult(
            search_res.track, search_res.genius_result, get_lyrics(session, search_res)
        )
        queue_lyrics_results.put(res)


def make_pool(n_workers, target, args=(), kwargs={}, **proc_kwargs) -> list[Process]:
    return [
        Process(target=target, args=args, kwargs=kwargs, **proc_kwargs)
        for _ in range(n_workers)
    ]


@cli.command()
@click.pass_context
@click.option("--n-search-workers", default=4)
@click.option("--n-lyrics-workers", default=4)
@click.option("--search-delay", default=0.1)
@click.option("--lyrics-delay", default=0.1)
def pull(
    ctx,
    n_search_workers: int,
    n_lyrics_workers: int,
    search_delay: float,
    lyrics_delay: float,
):
    """
    download lyrics for all songs in the local database without them
    """
    db = ctx.obj["db"]

    queue_track_details = Queue()
    queue_search_results = Queue()
    queue_lyrics_results = Queue()
    search_workers = make_pool(
        n_search_workers,
        worker_genius_search,
        args=(queue_track_details, queue_search_results),
    )
    lyrics_workers = make_pool(
        n_lyrics_workers,
        worker_genius_lyrics,
        args=(queue_search_results, queue_lyrics_results),
    )
    for w in chain(search_workers, lyrics_workers):
        w.start()

    track_iter = db.conn.execute("""\
        SELECT tracks.id, tracks.title, tracks.artists
        FROM tracks
        LEFT JOIN lyrics ON tracks.id = lyrics.track_id
        WHERE lyrics.track_id IS NULL""")
    for row in track_iter:
        queue_track_details.put(SpotifyTrackDetails(*row))

    n_tracks = db.conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]

    with tqdm(total=n_tracks) as pb:
        while res := queue_lyrics_results.get():
            if not res.lyrics:
                continue
            tqdm.write(f"== LYRICS RESULT: {res}")
            with db.conn:
                db.conn.execute(
                    "INSERT INTO lyrics(track_id, genius_url, lyrics) VALUES (?, ?, ?)",
                    (res.track.tid, res.genius_result["url"], res.lyrics),
                )

            pb.update(1)

    for w in chain(search_workers, lyrics_workers):
        w.join()

    # get tracks from db  (id, title, artists) columns on table tracks
    #  send them to a queue to be looked up on the genius search api by worker threads
    #  send these results to a queue to be queried from each `url` by beautifulsoup
    # join on the result of these workers and (main thread) continually push them into sqlite
