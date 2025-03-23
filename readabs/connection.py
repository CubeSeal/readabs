from types import CoroutineType

import asyncio
import aiohttp

async def _get_with_session(session: aiohttp.ClientSession, url: str) -> str:
    """
    Get text response with a session.
    """
    async with session.get(url) as response:
        response_text: str = await response.text()
        return response_text

async def get_one(url: str) -> str:
    """
    Async get for text reponse with one url.
    """
    async with aiohttp.ClientSession() as session:
        return await _get_with_session(session, url)

async def get_many(urls: list[str]) -> list[str]:
    """
    Async get for text response with multiple urls.
    """
    async with aiohttp.ClientSession() as session:
        tasks: list[CoroutineType[aiohttp.ClientSession, str, str]] = \
            [_get_with_session(session, url) for url in urls]

        return_list: list[str] = await asyncio.gather(*tasks)
        return return_list

async def _get_with_session_bytes(session: aiohttp.ClientSession, url: str) -> bytes:
    """
    Get bytes response with a session.
    """
    async with session.get(url) as response:
        response_text: bytes = await response.read()
        return response_text

async def get_one_bytes(url: str) -> bytes:
    """
    Async get for bytes reponse with one url.
    """
    async with aiohttp.ClientSession() as session:
        return await _get_with_session_bytes(session, url)
