from datetime import datetime, timedelta
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from models import Song, Play, Base
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
PLAYLIST_ID = os.getenv("SPOTIPY_PLAYLIST_ID")

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


def main():
    session = Session()
    playlist = sp.playlist(PLAYLIST_ID)
    print(f"Playlist: {playlist['name']}")

    # Last Sunday at 00:00:00 to this past Sunday at 00:00:00
    end_date = (datetime.now() - timedelta(days=datetime.now().weekday() + 1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_date = datetime.now()  # DEBUG
    start_date = end_date - timedelta(weeks=1)

    print(f"Getting songs from {start_date} to {end_date}...")

    # Query to group plays by song and count them
    song_play_counts = (
        session.query(Song.spotify_id, func.count(Play.id).label("play_count"))
        .join(Song, Play.song_id == Song.id)
        .filter(Play.played_at >= start_date, Play.played_at < end_date)
        .group_by(Play.song_id, Song.spotify_id)
        .order_by(func.count(Play.id).desc())
    )

    # Print results
    print(f"Found {song_play_counts.count()} unique songs played.")

    # Add the top song to the playlist
    top_song = song_play_counts.first()
    print(f"Adding {top_song.spotify_id} to the playlist...")
    sp.playlist_add_items(PLAYLIST_ID, [top_song.spotify_id])


if __name__ == "__main__":
    main()
    print("Done!")
