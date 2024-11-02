import argparse
import asyncio
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
from aiohttp import ClientSession
from jsonschema.exceptions import ValidationError
from loguru import logger
from pydantic import BaseModel


class Rate(BaseModel):
    country: Optional[str] = None
    symbol: Optional[str] = None
    code: str
    bid: float
    ask: float


class CurrencyData(BaseModel):
    table: Optional[str] = None
    no: Optional[str] = None
    tradingDate: Optional[str] = None
    effectiveDate: str
    rates: list[Rate]

    def remove_duplicate_rates(self):
        unique_rates = {rate.code: rate for rate in self.rates}
        self.rates = list(unique_rates.values())


class GoldRate(BaseModel):
    date: datetime
    rate: float


from apipull.AsyncDatabaseManager import AsyncDatabaseManager
from apipull.SQLAlchemy_models import CurrencyRate

API_URL_RATES = "https://api.nbp.pl/api/exchangerates/tables/c/{start_date}/{end_date}"
API_URL_GOLD = "https://api.nbp.pl/api/cenyzlota/{start_date}/{end_date}"


# Функция для создания интервалов по 90 дней
def generate_date_ranges(
    start_date: datetime, end_date: datetime, interval_days: int = 90
):
    date_ranges = []

    # Проверка на случай, если start_date и end_date совпадают
    if start_date == end_date:
        date_ranges.append(
            (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        )
    else:
        while start_date < end_date:
            interval_end = min(start_date + timedelta(days=interval_days), end_date)
            date_ranges.append(
                (start_date.strftime("%Y-%m-%d"), interval_end.strftime("%Y-%m-%d"))
            )
            start_date = interval_end + timedelta(
                days=1
            )  # Следующий день после интервала

    logger.debug(date_ranges)
    return date_ranges


async def fetch_data(session: ClientSession, url: str, retries: int = 3):
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.error(
                        f"Request failed with status {response.status} for URL: {url}"
                    )
        except aiohttp.ClientError as e:
            logger.error(f"Request error: {e} for URL: {url}")

        await asyncio.sleep(2**attempt)

    logger.error(f"Failed to fetch data from {url} after {retries} attempts.")
    return None


async def fetch_currency_data(start_date: datetime, end_date: datetime, api_url: str):
    date_ranges = generate_date_ranges(start_date, end_date)

    async with aiohttp.ClientSession() as session:
        tasks = []

        for start, end in date_ranges:
            url = api_url.format(start_date=start, end_date=end)
            tasks.append(fetch_data(session, url))

        results = await asyncio.gather(*tasks)
        return ([result for result in results if result is not None], api_url)


def reformat_instruments_dicts(json_data, validate_type: str = "currency"):
    try:
        if validate_type == "currency":
            currencies = [
                CurrencyData(**item) for sublist in json_data for item in sublist
            ]
            for currency in currencies:
                currency.remove_duplicate_rates()
            print("JSON is valid.")
            return currencies
        else:
            golds = []
            for sublist in json_data:
                for item in sublist:
                    gold = GoldRate(
                        date=datetime.strptime(item["data"], "%Y-%m-%d"),
                        rate=float(item["cena"]),
                    )
                    golds.append(gold)
            return golds
    except ValidationError as e:
        print("JSON is invalid:", e.json())


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Script to process dates and other parameters"
    )

    parser.add_argument(
        "--start_date", type=str, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose mode")

    args = parser.parse_args()
    if args.start_date:
        try:
            args.start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            print("Error: Invalid start_date format. Use YYYY-MM-DD format.")
            exit(1)

    return args


async def main():
    today = datetime.today()
    start_for_currency = datetime.strptime("2002-01-02", "%Y-%m-%d")
    start_for_gold = datetime.strptime("2013-01-02", "%Y-%m-%d")

    args = parse_arguments()
    if args.start_date:
        logger.debug(f"start_date: {args.start_date}")
        if start_for_currency < args.start_date:
            logger.debug(f"{start_for_currency}, {args.start_date}")
            start_for_currency = args.start_date
            if start_for_currency > start_for_gold:
                start_for_gold = start_for_currency
        else:
            logger.error(f"Error: start_date must be after start_for_currency")
            exit(1)

    tasks = [
        fetch_currency_data(start_for_currency, today, API_URL_RATES),
        fetch_currency_data(start_for_gold, today, API_URL_GOLD),
    ]

    db_url = "sqlite+aiosqlite:///currency_rates.db"  # замените на ваш URL базы данных
    db_manager = AsyncDatabaseManager(db_url)

    await db_manager.init_db()

    currency_data = []
    gold_rates = []

    for task in asyncio.as_completed(tasks):
        data = await task
        if data[1] == API_URL_RATES:
            currency_data = reformat_instruments_dicts(
                data[0], validate_type="currency"
            )

            currency_data_db = []
            for currency_item in currency_data:
                for rate in currency_item.rates:
                    currency_rate = CurrencyRate(
                        code=rate.code,
                        effective_date=datetime.strptime(
                            currency_item.effectiveDate, "%Y-%m-%d"
                        ).date(),
                        bid=rate.bid,
                        ask=rate.ask,
                    )
                    currency_data_db.append(currency_rate)

            await db_manager.bulk_add_currency_rates(currency_data_db)

        elif data[1] == API_URL_GOLD:
            gold_rates = reformat_instruments_dicts(data[0], validate_type="gold")

            gold_rate_db = []
            for gold_item in gold_rates:
                currency_rate = CurrencyRate(
                    code="GOLD_gold",
                    effective_date=gold_item.date,
                    bid=gold_item.rate,
                    ask=gold_item.rate,
                )
                gold_rate_db.append(currency_rate)

            await db_manager.bulk_add_currency_rates(gold_rate_db)


if __name__ == "__main__":
    asyncio.run(main())
