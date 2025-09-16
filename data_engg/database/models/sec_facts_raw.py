from sqlalchemy import Column, Integer, String, Date, Numeric, Text, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class BronzeSecFacts(Base):
    """Bronze SEC Facts Data Table"""

    __tablename__ = "bronze_sec_facts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cik = Column(String(13), nullable=False)
    taxonomy = Column(String(64), nullable=False)
    tag = Column(String(256), nullable=False)
    unit = Column(String(32), nullable=False)
    val = Column(Numeric(precision=30, scale=2), nullable=False)
    fy = Column(Numeric, nullable=True)
    fp = Column(String(8), nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    frame = Column(String(128), nullable=True)
    form = Column(String(16), nullable=True)
    filed = Column(Date, nullable=True)
    accn = Column(String(32), nullable=True)

    __table_args__ = {"extend_existing": True}

    def __repr__(self):
        return f"<BronzeSecFacts(id={self.id}, cik={self.cik}, tag={self.tag}, val={self.val})>"


class BronzeSecFactsDict(Base):
    """Bronze SEC Facts Dictionary Table"""

    __tablename__ = "bronze_sec_facts_dict"

    # Business key (natural key)
    taxonomy = Column(String(64), nullable=False)
    tag = Column(String(256), nullable=False)

    # Data attributes
    label = Column(Text, nullable=True)
    description = Column(Text, nullable=True)

    # Audit attributes
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = {"extend_existing": True}

    def __repr__(self):
        return f"<BronzeSecFactsDict(id={self.id}, taxonomy={self.taxonomy}, tag={self.tag})>"
