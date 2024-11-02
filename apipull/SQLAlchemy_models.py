from sqlalchemy import Column, Date, Float, Index, PrimaryKeyConstraint, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class CurrencyRate(Base):
    __tablename__ = "currency_rates"

    code = Column(String, nullable=False)
    effective_date = Column(Date, nullable=False)
    bid = Column(Float)
    ask = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint("code", "effective_date"),  # Составной первичный ключ
        Index(
            "idx_currency_code_date", "code", "effective_date"
        ),  # Индекс для ускорения поиска по валюте и дате
    )
