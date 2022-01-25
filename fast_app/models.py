from sqlalchemy import Column, ForeignKey, Integer, Float, String, DateTime
from sqlalchemy.orm import relationship

from .database import Base


class Earthquake(Base):
    __tablename__ = "earthquake"

    id = Column(Integer, primary_key=True, index=True)
    danger_level = Column(Integer, nullable=False)
    city = Column(String, nullable=False)
    canton = Column(String, nullable=False)
    magnitude = Column(Float, nullable=False)
    datetime = Column(DateTime, nullable=False)
    depth = Column(Float, nullable=False)
    intensity = Column(String, nullable=False)
    meta = Column(String, nullable=False)
    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)
    human_losses = Column(Float)
    cantonal_losses = relationship("Losses",
                                   back_populates="earthquake",
                                   cascade="all, delete", lazy="joined")


class Losses(Base):
    __tablename__ = "losses"

    id = Column(Integer, primary_key=True, index=True)
    canton = Column(String, nullable=False)
    loss_value = Column(Float)
    earthquake_id = Column(Integer, ForeignKey('earthquake.id'))
    earthquake = relationship("Earthquake", back_populates="cantonal_losses")
