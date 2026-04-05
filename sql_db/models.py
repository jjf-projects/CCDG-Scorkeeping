from sqlalchemy import Column, Integer, String, Boolean, Date, ForeignKey, JSON
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Schedule(Base):
    """One row per scoring period (week). The schedule is the backbone of the system —
    everything else (scores, divisions, standings) hangs off period numbers."""

    __tablename__ = "schedule"

    period       = Column(Integer, primary_key=True)
    saturday     = Column(Date, nullable=False)          # window opens Saturday morning
    sunday       = Column(Date, nullable=False)          # window closes Sunday evening
    course       = Column(String, nullable=False)
    layout       = Column(String)
    travel       = Column(Boolean, default=False)        # True = travel round; scores loaded from local xlsx
    cycle        = Column(Integer, nullable=False)       # award cycle this period belongs to (1, 2, or 3)
    event_url    = Column(String)                        # UDisc event URL; None for travel rounds

    scores          = relationship("Score", back_populates="schedule")
    player_divisions = relationship(
        "PlayerDivision",
        primaryjoin="or_(Schedule.period == PlayerDivision.valid_from_period, "
                    "Schedule.period == PlayerDivision.valid_to_period)",
        viewonly=True,
    )


class Division(Base):
    """Defines the skill-level divisions for a season (e.g. Alpha, Bravo, …, Echo)."""

    __tablename__ = "division"

    division_id   = Column(Integer, primary_key=True)
    div_name      = Column(String, unique=True, nullable=False)
    display_order = Column(Integer, nullable=False)     # controls column order in standings output

    player_divisions = relationship("PlayerDivision", back_populates="division")


class Player(Base):
    """One row per registered player. Division is NOT stored here — use PlayerDivision."""

    __tablename__ = "player"

    player_id      = Column(Integer, primary_key=True)
    full_name      = Column(String, nullable=False)
    udisc_username = Column(String, unique=True)
    email          = Column(String)

    scores          = relationship("Score", back_populates="player")
    player_divisions = relationship("PlayerDivision", back_populates="player")


class PlayerDivision(Base):
    """Maps a player to a division for a range of periods.
    A player may have multiple rows here as their division changes after rebalancing.
    valid_to_period is None if the assignment is still current."""

    __tablename__ = "player_division"

    player_div_id     = Column(Integer, primary_key=True)
    player_id         = Column(Integer, ForeignKey("player.player_id"), nullable=False)
    division_id       = Column(Integer, ForeignKey("division.division_id"), nullable=False)
    valid_from_period = Column(Integer, ForeignKey("schedule.period"), nullable=False)
    valid_to_period   = Column(Integer, ForeignKey("schedule.period"))   # None = still active

    player   = relationship("Player", back_populates="player_divisions")
    division = relationship("Division", back_populates="player_divisions")


class Score(Base):
    """One row per player per scoring period.
    relative_score is the primary input to the points algorithm for regular rounds.
    round_rating is used for travel rounds (stored as a negative value so lower = better,
    consistent with relative_score — the scorekeeper negates UDisc ratings before submitting)."""

    __tablename__ = "score"

    score_id       = Column(Integer, primary_key=True)
    player_id      = Column(Integer, ForeignKey("player.player_id"), nullable=False)
    period         = Column(Integer, ForeignKey("schedule.period"), nullable=False)
    total_score    = Column(Integer)    # raw stroke count
    relative_score = Column(Integer)    # strokes relative to par; used for regular round points
    round_rating   = Column(Integer)    # negated UDisc round rating; used for travel round points
    hole_scores    = Column(JSON)       # {"hole_1": 3, "hole_2": 4, ...} — variable length, None if not captured

    player   = relationship("Player", back_populates="scores")
    schedule = relationship("Schedule", back_populates="scores")
