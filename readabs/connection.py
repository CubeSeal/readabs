from types import CoroutineType

import asyncio
import aiohttp

async def _get_with_session(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as response:
        response_text: str = await response.text()
        return response_text

async def get_one(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        return await _get_with_session(session, url)

async def get_many(urls: list[str]) -> list[str]:
    async with aiohttp.ClientSession() as session:
        tasks: list[CoroutineType[aiohttp.ClientSession, str, str]] = \
            [_get_with_session(session, url) for url in urls]

        return_list: list[str] = await asyncio.gather(*tasks)
        return return_list

async def _get_with_session_bytes(session: aiohttp.ClientSession, url: str) -> bytes:
    async with session.get(url) as response:
        response_text: bytes = await response.read()
        return response_text

async def get_one_bytes(url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        return await _get_with_session_bytes(session, url)
