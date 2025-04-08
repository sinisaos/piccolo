from unittest import TestCase

from piccolo.columns import ForeignKey, Varchar
from piccolo.table import Table, create_db_tables_sync, drop_db_tables_sync


class Manager(Table):
    name = Varchar()


class Band(Table):
    name = Varchar()
    manager = ForeignKey(Manager, null=False)


class TestNullFK(TestCase):
    """
    Make sure we can use null=False argument in ForeignKey.
    """

    def setUp(self):
        create_db_tables_sync(Manager, Band)

    def tearDown(self):
        drop_db_tables_sync(Manager, Band)

    def test_null_false(self):
        manager_1 = Manager.objects().create(name="Guido").run_sync()
        manager_2 = Manager.objects().create(name="Graydon").run_sync()

        Band.insert(
            Band(name="Pythonistas", manager=manager_1),
            Band(name="Rustaceans", manager=manager_2),
        ).run_sync()

        response = Band.select(Band.name, Band.manager.name).run_sync()
        self.assertEqual(
            response,
            [
                {"name": "Pythonistas", "manager.name": "Guido"},
                {"name": "Rustaceans", "manager.name": "Graydon"},
            ],
        )
        Band.update({Band.name: "Golangs"}).where(Band.id == 1).run_sync()

        response = (
            Band.select(Band.name, Band.manager.name)
            .where(Band.id == 1)
            .run_sync()
        )
        self.assertEqual(
            response,
            [
                {"name": "Golangs", "manager.name": "Guido"},
            ],
        )
