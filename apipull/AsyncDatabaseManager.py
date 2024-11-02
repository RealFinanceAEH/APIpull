import logging
from sqlite3 import Date, IntegrityError

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from SQLAlchemy_models import Base, CurrencyRate


class AsyncDatabaseManager:
    def __init__(self, db_url):
        self.engine = create_async_engine(db_url, echo=True)
        self.async_session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def add_currency_rate(
        self, effective_date: Date, currency: str, code: str, bid: float, ask: float
    ):
        async with self.async_session() as session:
            existing_rate = await session.execute(
                select(CurrencyRate).where(
                    CurrencyRate.code == code,
                    CurrencyRate.effective_date == effective_date,
                )
            )
            existing_rate = existing_rate.scalar_one_or_none()

            if existing_rate:
                logging.info(f"Updating rate for {code} on {effective_date}")
                existing_rate.currency = currency
                existing_rate.bid = bid
                existing_rate.ask = ask
            else:
                logging.info(f"Adding new rate for {code} on {effective_date}")
                new_rate = CurrencyRate(
                    effective_date=effective_date,
                    currency=currency,
                    code=code,
                    bid=bid,
                    ask=ask,
                )
                session.add(new_rate)

            await session.commit()

    async def get_currency_rate_by_date(self, code: str, date: Date):
        async with self.async_session() as session:
            result = await session.execute(
                select(CurrencyRate).where(
                    CurrencyRate.code == code, CurrencyRate.effective_date == date
                )
            )
            return result.scalar_one_or_none()

    async def bulk_add_currency_rates(self, currency_rates: list[CurrencyRate]):
        async with self.async_session() as session:
            try:
                # Цикл для добавления объектов по одному
                for rate in currency_rates:
                    session.add(rate)
                await session.commit()
                logging.info(
                    f"Successfully added {len(currency_rates)} currency rates to the database."
                )
            except IntegrityError as e:
                # В случае ошибки откатываем транзакцию и логируем проблему
                await session.rollback()
                logging.error(
                    "IntegrityError: Unable to bulk insert currency rates. Possible duplicate primary keys."
                )
                raise e

    async def get_currency_rates_in_date_range(
        self, code: str, start_date: Date, end_date: Date
    ):
        async with self.async_session() as session:
            result = await session.execute(
                select(CurrencyRate)
                .where(CurrencyRate.code == code)
                .where(CurrencyRate.effective_date.between(start_date, end_date))
                .order_by(CurrencyRate.effective_date)
            )
            return result.scalars().all()
