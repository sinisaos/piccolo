from unittest import TestCase

from piccolo.columns.column_types import (
    ForeignKey,
    LazyTableReference,
    Serial,
    Text,
    Varchar,
)
from piccolo.columns.m2m import M2M
from piccolo.engine.finder import engine_finder
from piccolo.table import Table, create_db_tables_sync, drop_db_tables_sync
from tests.base import engines_only

engine = engine_finder()


class Band(Table):
    id: Serial
    name = Varchar()
    genres = M2M(LazyTableReference("GenreToBand", module_path=__name__))


class Genre(Table):
    id: Serial
    name = Varchar()
    bands = M2M(LazyTableReference("GenreToBand", module_path=__name__))


class GenreToBand(Table):
    id: Serial
    band = ForeignKey(Band)
    genre = ForeignKey(Genre)
    reason = Text(help_text="For testing additional columns on join tables.")


@engines_only("mysql")
class M2MMySQLTestSerialPK(TestCase):
    """
    This allows us to test M2M when the tables are in different schemas
    (public vs non-public).
    """

    def setUp(self):
        create_db_tables_sync(*[Band, Genre, GenreToBand], if_not_exists=True)

        bands = Band.insert(
            Band(name="Pythonistas"),
            Band(name="Rustaceans"),
            Band(name="C-Sharps"),
        ).run_sync()

        genres = Genre.insert(
            Genre(name="Rock"),
            Genre(name="Folk"),
            Genre(name="Classical"),
        ).run_sync()

        GenreToBand.insert(
            GenreToBand(band=bands[0]["id"], genre=genres[0]["id"]),
            GenreToBand(band=bands[0]["id"], genre=genres[1]["id"]),
            GenreToBand(band=bands[1]["id"], genre=genres[1]["id"]),
            GenreToBand(band=bands[2]["id"], genre=genres[0]["id"]),
            GenreToBand(band=bands[2]["id"], genre=genres[2]["id"]),
        ).run_sync()

    def tearDown(self):
        drop_db_tables_sync(*[GenreToBand, Genre, Band])

    def test_select_name(self):
        response = Band.select(
            Band.name, Band.genres(Genre.name, as_list=True)
        ).run_sync()
        self.assertEqual(
            response,
            [
                {"name": "Pythonistas", "genres": ["Rock", "Folk"]},
                {"name": "Rustaceans", "genres": ["Folk"]},
                {"name": "C-Sharps", "genres": ["Rock", "Classical"]},
            ],
        )

        # Now try it in reverse.
        response = Genre.select(
            Genre.name, Genre.bands(Band.name, as_list=True)
        ).run_sync()
        self.assertEqual(
            response,
            [
                {"name": "Rock", "bands": ["Pythonistas", "C-Sharps"]},
                {"name": "Folk", "bands": ["Pythonistas", "Rustaceans"]},
                {"name": "Classical", "bands": ["C-Sharps"]},
            ],
        )

    def test_no_related(self):
        """
        Make sure it still works correctly if there are no related values.
        """

        GenreToBand.delete(force=True).run_sync()

        # Try it with a list response
        response = Band.select(
            Band.name, Band.genres(Genre.name, as_list=True)
        ).run_sync()

        self.assertEqual(
            response,
            [
                {"name": "Pythonistas", "genres": []},
                {"name": "Rustaceans", "genres": []},
                {"name": "C-Sharps", "genres": []},
            ],
        )

        # Also try it with a nested response
        response = Band.select(
            Band.name, Band.genres(Genre.id, Genre.name)
        ).run_sync()
        self.assertEqual(
            response,
            [
                {"name": "Pythonistas", "genres": []},
                {"name": "Rustaceans", "genres": []},
                {"name": "C-Sharps", "genres": []},
            ],
        )

    def test_select_multiple(self):

        response = Band.select(
            Band.name, Band.genres(Genre.id, Genre.name)
        ).run_sync()

        self.assertEqual(
            response,
            [
                {
                    "name": "Pythonistas",
                    "genres": [
                        {"id": 1, "name": "Rock"},
                        {"id": 2, "name": "Folk"},
                    ],
                },
                {"name": "Rustaceans", "genres": [{"id": 2, "name": "Folk"}]},
                {
                    "name": "C-Sharps",
                    "genres": [
                        {"id": 1, "name": "Rock"},
                        {"id": 3, "name": "Classical"},
                    ],
                },
            ],
        )

        # Now try it in reverse.
        response = Genre.select(
            Genre.name, Genre.bands(Band.id, Band.name)
        ).run_sync()

        self.assertEqual(
            response,
            [
                {
                    "name": "Rock",
                    "bands": [
                        {"id": 1, "name": "Pythonistas"},
                        {"id": 3, "name": "C-Sharps"},
                    ],
                },
                {
                    "name": "Folk",
                    "bands": [
                        {"id": 1, "name": "Pythonistas"},
                        {"id": 2, "name": "Rustaceans"},
                    ],
                },
                {
                    "name": "Classical",
                    "bands": [{"id": 3, "name": "C-Sharps"}],
                },
            ],
        )

    def test_select_id(self):

        response = Band.select(
            Band.name, Band.genres(Genre.id, as_list=True)
        ).run_sync()
        self.assertEqual(
            response,
            [
                {"name": "Pythonistas", "genres": [1, 2]},
                {"name": "Rustaceans", "genres": [2]},
                {"name": "C-Sharps", "genres": [1, 3]},
            ],
        )

        # Now try it in reverse.
        response = Genre.select(
            Genre.name, Genre.bands(Band.id, as_list=True)
        ).run_sync()
        self.assertEqual(
            response,
            [
                {"name": "Rock", "bands": [1, 3]},
                {"name": "Folk", "bands": [1, 2]},
                {"name": "Classical", "bands": [3]},
            ],
        )

    def test_select_all_columns(self):
        """
        Make sure ``all_columns`` can be passed in as an argument. ``M2M``
        should flatten the arguments. Reported here:

        https://github.com/piccolo-orm/piccolo/issues/728
        """

        response = Band.select(
            Band.name, Band.genres(Genre.all_columns(exclude=(Genre.id,)))
        ).run_sync()
        self.assertEqual(
            response,
            [
                {
                    "name": "Pythonistas",
                    "genres": [
                        {"name": "Rock"},
                        {"name": "Folk"},
                    ],
                },
                {"name": "Rustaceans", "genres": [{"name": "Folk"}]},
                {
                    "name": "C-Sharps",
                    "genres": [
                        {"name": "Rock"},
                        {"name": "Classical"},
                    ],
                },
            ],
        )

    def test_add_m2m(self):
        """
        Make sure we can add items to the joining table.
        """

        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None
        band.add_m2m(Genre(name="Punk Rock"), m2m=Band.genres).run_sync()

        self.assertTrue(
            Genre.exists().where(Genre.name == "Punk Rock").run_sync()
        )

        self.assertEqual(
            GenreToBand.count()
            .where(
                GenreToBand.band.name == "Pythonistas",
                GenreToBand.genre.name == "Punk Rock",
            )
            .run_sync(),
            1,
        )

    def test_extra_columns_str(self):
        """
        Make sure the ``extra_column_values`` parameter for ``add_m2m`` works
        correctly when the dictionary keys are strings.
        """

        reason = "Their second album was very punk rock."

        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None
        band.add_m2m(
            Genre(name="Punk Rock"),
            m2m=Band.genres,
            extra_column_values={
                "reason": "Their second album was very punk rock."
            },
        ).run_sync()

        Genreto_band = (
            GenreToBand.objects()
            .get(
                (GenreToBand.band.name == "Pythonistas")
                & (GenreToBand.genre.name == "Punk Rock")
            )
            .run_sync()
        )
        assert Genreto_band is not None

        self.assertEqual(Genreto_band.reason, reason)

    def test_extra_columns_class(self):
        """
        Make sure the ``extra_column_values`` parameter for ``add_m2m`` works
        correctly when the dictionary keys are ``Column`` classes.
        """

        reason = "Their second album was very punk rock."

        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None
        band.add_m2m(
            Genre(name="Punk Rock"),
            m2m=Band.genres,
            extra_column_values={
                GenreToBand.reason: "Their second album was very punk rock."
            },
        ).run_sync()

        Genreto_band = (
            GenreToBand.objects()
            .get(
                (GenreToBand.band.name == "Pythonistas")
                & (GenreToBand.genre.name == "Punk Rock")
            )
            .run_sync()
        )
        assert Genreto_band is not None

        self.assertEqual(Genreto_band.reason, reason)

    def test_add_m2m_existing(self):
        """
        Make sure we can add an existing element to the joining table.
        """

        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None

        genre = Genre.objects().get(Genre.name == "Classical").run_sync()
        assert genre is not None

        band.add_m2m(genre, m2m=Band.genres).run_sync()

        # We shouldn't have created a duplicate genre in the database.
        self.assertEqual(
            Genre.count().where(Genre.name == "Classical").run_sync(), 1
        )

        self.assertEqual(
            GenreToBand.count()
            .where(
                GenreToBand.band.name == "Pythonistas",
                GenreToBand.genre.name == "Classical",
            )
            .run_sync(),
            1,
        )

    def test_get_m2m(self):
        """
        Make sure we can get related items via the joining table.
        """

        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None

        genres = band.get_m2m(Band.genres).run_sync()

        self.assertTrue(all(isinstance(i, Table) for i in genres))

        self.assertEqual([i.name for i in genres], ["Rock", "Folk"])

    def test_get_m2m_no_rows(self):
        """
        If there are no matching objects, then an empty list should be
        returned.

        https://github.com/piccolo-orm/piccolo/issues/1090

        """
        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None

        Genre.delete(force=True).run_sync()

        genres = band.get_m2m(Band.genres).run_sync()
        self.assertEqual(genres, [])

    def test_remove_m2m(self):
        """
        Make sure we can remove related items via the joining table.
        """

        band = Band.objects().get(Band.name == "Pythonistas").run_sync()
        assert band is not None

        genre = Genre.objects().get(Genre.name == "Rock").run_sync()
        assert genre is not None

        band.remove_m2m(genre, m2m=Band.genres).run_sync()

        self.assertEqual(
            GenreToBand.count()
            .where(
                GenreToBand.band.name == "Pythonistas",
                GenreToBand.genre.name == "Rock",
            )
            .run_sync(),
            0,
        )

        # Make sure the others weren't removed:
        self.assertEqual(
            GenreToBand.count()
            .where(
                GenreToBand.band.name == "Pythonistas",
                GenreToBand.genre.name == "Folk",
            )
            .run_sync(),
            1,
        )

        self.assertEqual(
            GenreToBand.count()
            .where(
                GenreToBand.band.name == "C-Sharps",
                GenreToBand.genre.name == "Rock",
            )
            .run_sync(),
            1,
        )


# A schema using self-reference tables


class Member(Table):
    name = Varchar()
    # self-reference many to many
    followers = M2M(
        LazyTableReference("MemberToFollower", module_path=__name__)
    )
    followings = M2M(
        LazyTableReference("MemberToFollower", module_path=__name__)
    )


class MemberToFollower(Table):
    follower_id = ForeignKey(Member)
    following_id = ForeignKey(Member)


SELF_REFERENCE_SCHEMA = [Member, MemberToFollower]


class TestM2MSelfReference(TestCase):
    """
    Make sure the M2M functionality works correctly when the tables is
    the same (self-reference tables).
    """

    def setUp(self):
        create_db_tables_sync(*SELF_REFERENCE_SCHEMA, if_not_exists=True)

        bob = Member.objects().create(name="Bob").run_sync()
        sally = Member.objects().create(name="Sally").run_sync()
        fred = Member.objects().create(name="Fred").run_sync()
        john = Member.objects().create(name="John").run_sync()
        mia = Member.objects().create(name="Mia").run_sync()

        MemberToFollower.insert(
            MemberToFollower(follower_id=fred, following_id=bob),
            MemberToFollower(follower_id=bob, following_id=sally),
            MemberToFollower(follower_id=fred, following_id=sally),
            MemberToFollower(follower_id=john, following_id=bob),
            MemberToFollower(follower_id=mia, following_id=bob),
            MemberToFollower(follower_id=bob, following_id=john),
        ).run_sync()

    def tearDown(self):
        drop_db_tables_sync(*SELF_REFERENCE_SCHEMA)

    @engines_only("mysql")
    def test_select_bidirectional(self):
        """
        Make sure we can select related items for self-reference table.
        """
        followings = (
            Member.select(Member.followings(Member.name, as_list=True))
            .where(Member.name == "Bob")
            .run_sync()
        )

        self.assertEqual(followings, [{"followings": ["Sally", "John"]}])

        # Now we use the bidirectional argument to get the correct result.
        # Without it, we cannot get the correct result for symmetric
        # self-referencing many to many relations.
        followers = (
            Member.select(
                Member.followers(Member.name, as_list=True, bidirectional=True)
            )
            .where(Member.name == "Bob")
            .run_sync()
        )

        self.assertEqual(followers, [{"followers": ["Fred", "John", "Mia"]}])

    def test_get_m2m_bidirectional(self):
        """
        Make sure we can get related items for self-reference table.
        """
        member = Member.objects().get(Member.name == "Bob").run_sync()
        assert member is not None

        followings = member.get_m2m(Member.followings).run_sync()

        self.assertTrue(all(isinstance(i, Table) for i in followings))

        self.assertCountEqual([i.name for i in followings], ["Sally", "John"])

        # Now we use the bidirectional argument to get the correct result.
        # Without it, we cannot get the correct result for symmetric
        # self-referencing many to many relations.
        followers = member.get_m2m(
            Member.followers, bidirectional=True
        ).run_sync()

        self.assertTrue(all(isinstance(i, Table) for i in followers))

        self.assertCountEqual(
            [i.name for i in followers], ["Fred", "John", "Mia"]
        )
