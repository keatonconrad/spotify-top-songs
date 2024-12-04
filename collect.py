from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from models import Song, Play, Base, Artist, Album
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

Base.metadata.create_all(engine)

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played",
    )
)


def main():
    recently_played_tracks = sp.current_user_recently_played(limit=50)

    session = Session()

    for track in recently_played_tracks["items"]:
        played_at = datetime.strptime(track["played_at"], "%Y-%m-%dT%H:%M:%S.%fZ")

        # Check if this played_at date is in the database
        if session.query(Play).filter_by(played_at=played_at).scalar():
            # If it is, we can stop the loop, since the rest of the tracks are older
            break

        album = Album.get_or_create(
            session,
            name=track["track"]["album"]["name"],
            spotify_id=track["track"]["album"]["id"],
            image=track["track"]["album"]["images"][0]["url"],
        )

        # Check if the song is already in the database
        song = session.query(Song).filter_by(spotify_id=track["track"]["id"]).first()
        if not song:
            song = Song(
                name=track["track"]["name"],
                album_id=album.id,
                spotify_id=track["track"]["id"],
            )
            song.save(session)

            for artist in track["track"]["artists"]:
                artist = Artist.get_or_create(
                    session, name=artist["name"], spotify_id=artist["id"]
                )
                song.artists.append(artist)

        print(
            f"Played {song.name} by {', '.join([artist.name for artist in song.artists])} at {played_at}"
        )
        play = Play(song_id=song.id, played_at=played_at)
        play.save(session)

    session.close()


if __name__ == "__main__":
    main()
    print("Done!")
