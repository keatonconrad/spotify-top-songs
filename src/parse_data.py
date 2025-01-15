from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Song, HistoricalPlay, Base, Artist, Album
import os
from dotenv import load_dotenv
import logging
import json
from tqdm import tqdm
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time
import requests

logging.basicConfig(level=logging.INFO)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

logging.info("Connecting to database...")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played playlist-modify-private",
    )
)

def get_all_existing_data(session):
    # Pull all existing songs and plays at the start for quick membership checks
    all_existing_songs = {song.spotify_id: song for song in session.query(Song).all()}
    all_existing_historical_plays = {
        play.played_at: play for play in session.query(HistoricalPlay).all()
    }
    return all_existing_songs, all_existing_historical_plays

def parse_data(filename: str, session):
    with open(filename) as f:
        data = json.load(f)

    all_existing_songs, all_existing_historical_plays = get_all_existing_data(session)

    # Track ID -> list of plays
    # Each entry in the list is a dict with keys: ts, ms_played
    spotify_plays_by_id = {}
    ids_to_query = set()

    def process_batch():
        """Process the current batch of track IDs and their associated plays."""
        nonlocal ids_to_query, spotify_plays_by_id

        if not ids_to_query:
            return

        # Attempt Spotify API call with basic retry logic for rate limiting
        retry_count = 0
        while True:
            try:
                tracks = sp.tracks(list(ids_to_query))
                break
            except spotipy.SpotifyException as e:
                # If rate-limited, wait and retry
                if e.http_status == 429:
                    retry_after = int(e.headers.get("Retry-After", 5))
                    logging.warning(f"Rate limited by Spotify. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    retry_count += 1
                    if retry_count > 5:
                        logging.error("Failed after multiple retries due to rate limits.")
                        return
                else:
                    logging.error(f"Spotify API error: {e}")
                    return
            except requests.exceptions.RequestException as e:
                # Network error, try again
                logging.error(f"Network error: {e}, retrying in 5 seconds.")
                time.sleep(5)
                retry_count += 1
                if retry_count > 5:
                    logging.error("Too many retries, giving up.")
                    return

        # Build a dictionary for quick lookup
        track_data_map = {t["id"]: t for t in tracks["tracks"] if t}

        # Process each track
        for track_id in ids_to_query:
            if track_id not in track_data_map:
                # Track not found or invalid, skip
                logging.warning(f"Track {track_id} not found in Spotify response.")
                continue

            track = track_data_map[track_id]

            # If we already have the song in DB, just add plays
            if track_id in all_existing_songs:
                song = all_existing_songs[track_id]
            else:
                # Create album
                album_data = track["album"]
                album = Album.get_or_create(
                    session,
                    name=album_data["name"],
                    spotify_id=album_data["id"],
                    image=album_data["images"][0]["url"] if album_data["images"] else None,
                )

                # Create song
                song = Song(
                    name=track["name"],
                    album_id=album.id,
                    spotify_id=track_id,
                    duration=track["duration_ms"],
                )
                session.add(song)
                session.flush()
                all_existing_songs[track_id] = song

                # Create or get artists and associate with song
                for artist_obj in track["artists"]:
                    db_artist = Artist.get_or_create(
                        session, name=artist_obj["name"], spotify_id=artist_obj["id"]
                    )
                    song.artists.append(db_artist)

            # Add historical plays
            for play_data in spotify_plays_by_id.get(track_id, []):
                played_at = datetime.strptime(play_data["ts"], "%Y-%m-%dT%H:%M:%SZ")

                # Check if this exact play is already in the database
                # Using the cached dictionary might be out-of-date, so just do a quick query
                # or assume that if we started fresh, all_existing_historical_plays is enough.
                if played_at in all_existing_historical_plays and \
                   all_existing_historical_plays[played_at].ms_played == play_data["ms_played"]:
                    continue

                play_obj = HistoricalPlay(
                    song_id=song.id,
                    played_at=played_at,
                    ms_played=play_data["ms_played"],
                )
                session.add(play_obj)

        # Commit the batch
        try:
            session.commit()
        except Exception as e:
            logging.error(f"Error committing batch: {e}")
            session.rollback()

        # Clear for next batch
        ids_to_query.clear()
        spotify_plays_by_id.clear()

    # Iterate over each play in the file
    for play in tqdm(data, desc=filename, unit="play", position=1):
        played_at = datetime.strptime(play["ts"], "%Y-%m-%dT%H:%M:%SZ")

        # Check if this played_at date is in the database and matches ms_played
        if (
            played_at in all_existing_historical_plays
            and all_existing_historical_plays[played_at].ms_played == play["ms_played"]
        ):
            continue

        if not play.get("spotify_track_uri"):
            continue

        spotify_track_id = play["spotify_track_uri"].split(":")[-1]

        # If song is already known, just add the play now
        if spotify_track_id in all_existing_songs:
            play_obj = HistoricalPlay(
                song_id=all_existing_songs[spotify_track_id].id,
                played_at=played_at,
                ms_played=play["ms_played"],
            )
            session.add(play_obj)
            continue

        # Otherwise, queue this track for batch processing
        if spotify_track_id not in spotify_plays_by_id:
            spotify_plays_by_id[spotify_track_id] = []
        spotify_plays_by_id[spotify_track_id].append(play)
        ids_to_query.add(spotify_track_id)

        # Process in batches of up to 50 IDs
        if len(ids_to_query) >= 50:
            process_batch()

    # Process any remaining items
    if ids_to_query:
        process_batch()

    # Final commit after all plays
    try:
        session.commit()
    except Exception as e:
        logging.error(f"Error final commit: {e}")
        session.rollback()

if __name__ == "__main__":
    session = Session()
    data_dir = "data"
    for filename in tqdm(os.listdir(data_dir), desc="Files", unit="file", leave=True, position=0):
        if filename.endswith(".json"):
            parse_data(os.path.join(data_dir, filename), session)
    session.close()
