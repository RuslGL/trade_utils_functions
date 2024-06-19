import os
import requests

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base


from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, func
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = str(os.getenv('db_link'))


def get_pairs_params_spot():
    base_url = 'https://api.bybit.com'
    get_pair_info_url = '/v5/market/instruments-info'

    response = requests.get(url=base_url+get_pair_info_url, params={'category': 'spot'})
    result = response.json().get('result').get('list')

    result_usdt_only = [item for item in result if item.get('quoteCoin') == 'USDT']

    usdt_pair_params = {}
    for element in result_usdt_only:
        usdt_pair_params[element.get('symbol')] = str(element)

    return usdt_pair_params


Base = declarative_base()


class TradeParamsSpot(Base):
    __tablename__ = 'trade_params_spot'
    pair = Column(String, primary_key=True,  index=True, nullable=False)
    params = Column(String, unique=False, index=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


# Создаем асинхронный движок базы данных
engine = create_async_engine(DATABASE_URL, echo=False)

# # Создаем асинхронный сеанс
async_session = sessionmaker(engine, class_=AsyncSession)


# Функция для создания таблицы
async def create_table():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def insert_trade_params_bulk_acid(params_list):
    # writes to db all or nothing
    async with async_session() as session:
        async with session.begin():
            session.add_all([TradeParamsSpot(pair=key, params=value) for key, value in params_list.items()])
            await session.commit()


async def insert_trade_params_bulk_base(params_list):
    # writes new data, rewrite/renew old one
    async with async_session() as session:
        async with session.begin():
            for key, value in params_list.items():
                trade_params = TradeParamsSpot(pair=key, params=value)
                await session.merge(trade_params)
            await session.commit()


async def main():
    # await create_table()
    params_list = get_pairs_params_spot()
    await insert_trade_params_bulk_base(params_list)


if __name__ == "__main__":
    asyncio.run(main())