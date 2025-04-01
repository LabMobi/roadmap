import pytest
from rmp.jira import JiraCloudCredentials, PageTracker


@pytest.fixture
def credentials():
    return JiraCloudCredentials("my_domain", None, None)


class TestJiraPageTracker:
    @pytest.fixture(scope="class")
    def page_tracker(self) -> PageTracker:
        return PageTracker()

    @pytest.mark.parametrize(
        "paged_response,results_count",
        [
            ({"startAt": 0, "maxResults": 10, "isLast": True}, 5),
            ({"startAt": 0, "maxResults": 10, "total": 5}, 5),
            ({"startAt": 0, "maxResults": 10, "total": 10}, 10),
            ({"nextPageToken": None}, 5),
            ({}, 5),
        ],
    )
    def test_page_tracker_last_page(
        self, paged_response: dict, results_count: int
    ) -> None:
        page_tracker = PageTracker()
        assert not page_tracker.tracking()
        page_tracker.track(paged_response, results_count)
        assert page_tracker.tracking()
        assert not page_tracker.next_page()

    @pytest.mark.parametrize(
        "paged_response,results_count,next_page_params",
        [
            ({"startAt": 0, "maxResults": 10, "total": 20}, 10, {"startAt": 10}),
            ({"startAt": 0, "maxResults": 10, "isLast": False}, 10, {"startAt": 10}),
            ({"nextPageToken": "abc"}, 10, {"nextPageToken": "abc"}),
        ],
    )
    def test_page_tracker_has_next_page(
        self, paged_response: dict, results_count: int, next_page_params: dict
    ) -> None:
        page_tracker = PageTracker()
        assert not page_tracker.tracking()
        page_tracker.track(paged_response, results_count)
        assert page_tracker.tracking()
        assert page_tracker.next_page()
        assert page_tracker.next_page_params == next_page_params
