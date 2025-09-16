from sqlalchemy import Column, Integer, String, Date, DateTime, Text, BigInteger
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BronzeSecSubmissions(Base):
    """Bronze SEC Submissions Data Table"""

    __tablename__ = "bronze_sec_submissions"

    # Composite Primary Key (based on your analysis)
    cik = Column(String(13), primary_key=True, nullable=False)
    accession_number = Column(String(25), primary_key=True, nullable=False)
    filing_date = Column(Date, primary_key=True, nullable=False)
    acceptance_datetime = Column(DateTime, primary_key=True, nullable=False)

    # Additional data fields
    report_date = Column(Date, nullable=True)
    act = Column(Text, nullable=True)
    form = Column(Text, nullable=True)
    file_number = Column(Text, nullable=True)
    film_number = Column(BigInteger, nullable=True)
    items = Column(Text, nullable=True)
    size = Column(Integer, nullable=True)
    is_xbrl = Column(Integer, nullable=True)
    is_inline_xbrl = Column(Integer, nullable=True)
    primary_document = Column(Text, nullable=True)
    primary_doc_description = Column(Text, nullable=True)

    __table_args__ = {"extend_existing": True}

    def __repr__(self):
        return f"<BronzeSecSubmissions(cik={self.cik}, accession_number={self.accession_number}, form={self.form})>"
