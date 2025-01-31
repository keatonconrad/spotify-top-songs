from sqlalchemy import Column, Integer, String, ForeignKey, Table, DateTime
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# Many-to-Many Relationship for Songs and Artists
song_artists = Table(
    "song_artists",
    Base.metadata,
    Column("song_id", Integer, ForeignKey("song.id"), primary_key=True),
    Column("artist_id", Integer, ForeignKey("artist.id"), primary_key=True),
)


class BaseModel(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)

    @classmethod
    def get_or_create(cls, session, spotify_id, **kwargs):
        # Always filter by the unique identifier
        instance = session.query(cls).filter_by(spotify_id=spotify_id).first()
        if not instance:
            instance = cls(spotify_id=spotify_id, **kwargs)
            session.add(instance)
            session.flush()  # flush to make the record visible to subsequent queries
        return instance

    def save(self, session):
        session.add(self)
        session.flush()


class Album(BaseModel):
    __tablename__ = "album"
    name = Column(String, nullable=False)
    spotify_id = Column(String, unique=True, nullable=False)
    image = Column(String, nullable=True)

    songs = relationship("Song", backref="album")


class Song(BaseModel):
    __tablename__ = "song"
    name = Column(String, nullable=False)
    album_id = Column(Integer, ForeignKey("album.id"), nullable=False)
    spotify_id = Column(String, unique=True, nullable=False)
    duration = Column(Integer, nullable=True)

    artists = relationship("Artist", secondary=song_artists, backref="songs")


class Artist(BaseModel):
    __tablename__ = "artist"
    name = Column(String, nullable=False)
    spotify_id = Column(String, unique=True, nullable=False)


class Play(BaseModel):
    __tablename__ = "play"
    song_id = Column(Integer, ForeignKey("song.id"), nullable=False)
    played_at = Column(DateTime, nullable=False)

class HistoricalPlay(BaseModel):
    __tablename__ = "historical_play"
    song_id = Column(Integer, ForeignKey("song.id"), nullable=False)
    played_at = Column(DateTime, nullable=False)
    ms_played = Column(Integer, nullable=True)