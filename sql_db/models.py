from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Schedule(Base):
    __tablename__ = "schedule"

    period = Column(Integer, primary_key=True)
    saturday = Column(Date)
    sunday = Column(Date)
    course = Column(String)
    layout = Column(String)
    travel = Column(Boolean)
    cycle = Column(Integer)
    event_url = Column(Integer)

    # Relationships
    scores = relationship("Score", back_populates="schedule")
    player_divisions_from = relationship("PlayerDivision", foreign_keys="[PlayerDivision.valid_from_period]", back_populates="valid_from_schedule")
    player_divisions_to = relationship("PlayerDivision", foreign_keys="[PlayerDivision.valid_to_period]", back_populates="valid_to_schedule")

class Division(Base):
    __tablename__ = "division"

    division_id = Column(Integer, primary_key=True)
    div_name = Column(String, unique=True, nullable=False)
    display_order = Column(Integer)

    # Relationships
    player_divisions = relationship("PlayerDivision", back_populates="division")

class Player(Base):
    __tablename__ = "player"

    player_id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    udisc_username = Column(String, unique=True)
    email = Column(String)

    # Relationships
    scores = relationship("Score", back_populates="player")
    player_divisions = relationship("PlayerDivision", back_populates="player")

class PlayerDivision(Base):
    __tablename__ = "player_division"

    player_div_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("player.player_id"), nullable=False)
    division_id = Column(Integer, ForeignKey("division.division_id"), nullable=False)
    valid_from_period = Column(Integer, ForeignKey("schedule.period"), nullable=False)
    valid_to_period = Column(Integer, ForeignKey("schedule.period"))

    # Relationships
    player = relationship("Player", back_populates="player_divisions")
    division = relationship("Division", back_populates="player_divisions")
    valid_from_schedule = relationship("Schedule", foreign_keys=[valid_from_period], back_populates="player_divisions_from")
    valid_to_schedule = relationship("Schedule", foreign_keys=[valid_to_period], back_populates="player_divisions_to")

class Score(Base):
    __tablename__ = "score"

    score_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("player.player_id"), nullable=False)
    period = Column(Integer, ForeignKey("schedule.period"), nullable=False)
    total_score = Column(Integer)
    relative_score = Column(Integer)
    round_rating = Column(Integer)

    # Relationships
    player = relationship("Player", back_populates="scores")
    schedule = relationship("Schedule", back_populates="scores")
    hole_scores = relationship("HoleScore", back_populates="score")

class HoleScore(Base):
    __tablename__ = "hole_score"

    hole_score_id = Column(Integer, primary_key=True)
    score_id = Column(Integer, ForeignKey("score.score_id"), nullable=False)
    hole_number = Column(String, nullable=False)
    hole_score = Column(Integer)

    # Relationships
    score = relationship("Score", back_populates="hole_scores")
