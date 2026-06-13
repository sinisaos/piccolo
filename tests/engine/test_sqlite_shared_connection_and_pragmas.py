from unittest import IsolatedAsyncioTestCase

from piccolo.engine.sqlite import SQLiteEngine
from tests.base import engines_only


@engines_only("sqlite")
class TestSQLiteEngine(IsolatedAsyncioTestCase):

    def setUp(self):
        self.engine = SQLiteEngine(
            path="test.sqlite",
            pragmas={"journal_mode": "MEMORY", "synchronous": "OFF"},
        )

    async def test_get_connection_and_pragmas(self):
        conn = await self.engine.get_connection()

        # Checking custom PRAGMA
        cursor = await conn.execute("PRAGMA journal_mode")
        row = await cursor.fetchone()

        self.assertEqual(row["journal_mode"], "memory")

        await conn.close()

    async def test_close_connection_pool_success(self):
        # Testing when the connection exists
        conn = await self.engine.get_connection()
        self.engine._shared_connection = conn

        self.assertIsNotNone(self.engine._shared_connection)

        await self.engine.close_connection_pool()

        # Check if it is set to None
        self.assertIsNone(self.engine._shared_connection)

        # Make sure the connection is actually closed
        with self.assertRaises(Exception):
            await conn.execute("SELECT 1")

    async def test_close_connection_pool_already_none(self):
        self.engine._shared_connection = None

        await self.engine.close_connection_pool()
        self.assertIsNone(self.engine._shared_connection)
