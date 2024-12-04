from sqlalchemy import Column, Integer, String, ForeignKey, Table, DateTime
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# Many-to-Many Relationship for Songs and Artists
song_artists = Table(
    "song_artists",
    Base.metadata,
    Column("song_id", Integer, ForeignKey("song.id"), primary_key=True),
    Column("artist_id", Integer, ForeignKey("artist.id"), primary_key=True)
)

class BaseModel(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)

    @classmethod
    def get_or_create(cls, session, **kwargs):
        instance = session.query(cls).filter_by(**kwargs).first()
        if not instance:
            instance = cls(**kwargs)
            instance.save(session)
        return instance

    def save(self, session):
        session.add(self)
        session.commit()

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

    artists = relationship("Artist", secondary=song_artists, backref="songs")

class Artist(BaseModel):
    __tablename__ = "artist"
    name = Column(String, nullable=False)
    spotify_id = Column(String, unique=True, nullable=False)

class Play(BaseModel):
    __tablename__ = "play"
    song_id = Column(Integer, ForeignKey("song.id"), nullable=False)
    played_at = Column(DateTime, nullable=False)
