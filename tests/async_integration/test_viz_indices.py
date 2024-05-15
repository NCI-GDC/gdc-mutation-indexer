import asyncio
import contextlib
import unittest

from mutation_indexer.constants import build
from tests.async_integration import setup, __main__


class TestCaseCentric(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self._context = contextlib.AsyncExitStack()
        self.es_client = await self._context.enter_async_context(
            setup.get_es_client(__main__.config.elasticsearch.connection)
        )

    async def asyncTearDown(self) -> None:
        await self._context.aclose()

    # CASE CENTRIC
    async def test__case_centric__cases_loaded(self) -> None:
        case_ids = ("case-0",)
        exists = await asyncio.gather(
            *(
                self.es_client.exists(
                    index=__main__.config.elasticsearch.write.indices[
                        build.IndexType.CASE_CENTRIC
                    ],
                    id=i,
                )
                for i in case_ids
            )
        )

        for id, result in zip(case_ids, exists):
            self.assertTrue(result, f"Missing case: {id}")
