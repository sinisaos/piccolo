import os

from piccolo.columns.column_types import BigInt
from piccolo.table import Table
from piccolo.testing.test_case import TableTest
from tests.base import engines_only


class MyTable(Table):
    value = BigInt()


@engines_only("postgres", "cockroach")
class TestBigIntPostgres(TableTest):
    """
    Make sure a BigInt column in Postgres can store a large number.
    """

    tables = [MyTable]

    def _test_length(self):
        # Can store 8 bytes, but split between positive and negative values.
        max_value = int(2**64 / 2) - 1
        min_value = max_value * -1

        print("Testing max value")
        row = MyTable(value=max_value)
        row.save().run_sync()

        print("Testing min value")
        row.value = min_value
        row.save().run_sync()

        if "TRAVIS" not in os.environ:
            # This stalls out on Travis - not sure why.
            print("Test exceeding max value")
            with self.assertRaises(Exception):
                row.value = max_value + 100
                row.save().run_sync()
