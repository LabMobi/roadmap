from rmp.application import ItemDataSourceApplication
from datetime import datetime


class TestApplication:
    def test_item_application(self) -> None:
        app = ItemDataSourceApplication()
        app.create_item(
            "http://example.com/1",
            datetime.now(),
            "KEY-1",
            "Summary 1",
            "To Do",
            0,
            "abc",
            [],
            [],
        )
        assert app.get_item("http://example.com/1") == {
            "url": "http://example.com/1",
            "identifier": "KEY-1",
            "summary": "Summary 1",
            "status": "To Do",
            "hierarchy_level": 0,
            "rank": "abc",
            "sprints": [],
            "milestones": [],
            "changelog_tracking_id": None,
        }

        app.change_status(
            "http://example.com/1",
            datetime.now(),
            "In Progress",
            changelog_tracking_id=1,
        )
        app.change_hierarchy_level(
            "http://example.com/1",
            datetime.now(),
            1,
            changelog_tracking_id=2,
        )
        app.change_rank(
            "http://example.com/1",
            datetime.now(),
            "def",
            changelog_tracking_id=3,
        )
        app.change_summary(
            "http://example.com/1",
            datetime.now(),
            "New Summary 1",
            changelog_tracking_id=4,
        )
        app.add_milestone(
            "http://example.com/1",
            datetime.now(),
            1,
            changelog_tracking_id=5,
        )
        app.add_sprint(
            "http://example.com/1",
            datetime.now(),
            10,
            changelog_tracking_id=6,
        )
        assert app.get_item("http://example.com/1") == {
            "url": "http://example.com/1",
            "identifier": "KEY-1",
            "summary": "New Summary 1",
            "status": "In Progress",
            "hierarchy_level": 1,
            "rank": "def",
            "sprints": [10],
            "milestones": [1],
            "changelog_tracking_id": 6,
        }
