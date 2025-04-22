import json
from datetime import datetime
from typing import Any, Callable, Generator, Iterable

import pytz
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from typing_extensions import override

from rmp.application import (
    ItemDataSourceApplication,
    MilestoneDataSourceApplication,
    SprintDataSourceApplication,
)
from rmp.backend import DataSourceConnector


class PageTracker:
    def __init__(self) -> None:
        self._start_at: int = 0
        self._total: int | None = None
        self._is_last: bool = False
        self._results_count = 0
        self._tracking = False
        self._next_page_token = None

    def track(self, paged_response: dict[str, Any], results_count: int) -> None:
        # Paged response with startAt, total and isLast parameters
        if "startAt" in paged_response:
            self._start_at = paged_response["startAt"]
            if "total" in paged_response:
                self._total = paged_response["total"]
            if "isLast" in paged_response:
                self._is_last = paged_response["isLast"]
        # Paged response with nextPageToken parameter
        elif (
            "nextPageToken" in paged_response
            and paged_response["nextPageToken"] is not None
        ):
            self._next_page_token = paged_response["nextPageToken"]
        else:
            self._is_last = True
            self._next_page_token = None

        self._tracking = True
        self._results_count = results_count

    def next_page(self) -> bool:
        if self._is_last is True:
            return False
        if (
            self._total is not None
            and self._start_at + self._results_count >= self._total
        ):
            return False
        if self._next_page_token is not None:
            return True
        self._start_at = self._start_at + self._results_count
        return True

    @property
    def results_count(self) -> int:
        return self._results_count

    @property
    def total(self) -> int | None:
        return self._total

    def tracking(self) -> bool:
        return self._tracking

    @property
    def next_page_params(self) -> dict[str, str | int]:
        if self._next_page_token is not None:
            return {"nextPageToken": self._next_page_token}
        if self._start_at > 0:
            return {"startAt": self._start_at}
        return {}


class JiraCloudCredentials:
    def __init__(self, domain: str, username: str, api_token: str) -> None:
        self._domain = domain
        self._username = username
        self._api_token = api_token

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def username(self) -> str:
        return self._username

    @property
    def api_token(self) -> str:
        return self._api_token


class JiraCloudRequests:
    URL_BASE = "https://%s.atlassian.net/"

    def __init__(self, credentials: JiraCloudCredentials) -> None:
        self._credentials = credentials

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        paged_data_key: str | None = None,
        progress_desc: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        return self.execute(
            self._get_request,
            path,
            params=params,
            paged_data_key=paged_data_key,
            progress_desc=progress_desc,
        )

    def _get_request(
        self,
        url: str,
        params: dict[str, Any],
        auth: HTTPBasicAuth,
        headers: dict[str, str],
    ) -> requests.Response:
        return requests.get(url, params=params, auth=auth, headers=headers)

    def post(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        paged_data_key: str | None = None,
        progress_desc: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        return self.execute(
            self._post_request,
            path,
            params=params,
            paged_data_key=paged_data_key,
            progress_desc=progress_desc,
        )

    def _post_request(
        self,
        url: str,
        params: dict[str, Any],
        auth: HTTPBasicAuth,
        headers: dict[str, str],
    ) -> requests.Response:
        return requests.post(url, data=json.dumps(params), auth=auth, headers=headers)

    def execute(
        self,
        request_call: Callable[
            [str, dict[str, Any], HTTPBasicAuth, dict[str, str]], requests.Response
        ],
        path: str,
        params: dict[str, Any] | None = None,
        paged_data_key: str | None = None,
        progress_desc: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        auth = HTTPBasicAuth(self._credentials.username, self._credentials.api_token)
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        params = {} if params is None else params

        # Create URL
        url = self.URL_BASE % self._credentials.domain + path

        if paged_data_key is None:
            # This is not a paged request
            response = request_call(url, params, auth, headers)
            if not response.ok:
                raise ValueError(response.content)
            yield response.json()

        t = PageTracker()
        with tqdm(desc=progress_desc) as pbar:
            while not t.tracking() or t.next_page():
                params = params | t.next_page_params
                response = request_call(url, params, auth, headers)
                if not response.ok:
                    raise ValueError(response.content)
                r = response.json()
                t.track(r, len(r[paged_data_key]))
                pbar.total = t.total
                pbar.update(t.results_count)
                yield from r[paged_data_key]


class JiraCloudConnector(DataSourceConnector):
    PATH_SEARCH_ISSUES = "rest/api/3/search/jql"
    PATH_GET_ISSUE_CHANGELOG = "rest/api/3/issue/%s/changelog"
    PATH_BOARD = "rest/agile/1.0/board/%s"
    PATH_BOARD_SPRINTS = "rest/agile/1.0/board/%s/sprint"
    PATH_BOARD_VERSIONS = "rest/agile/1.0/board/%s/version"

    FIELDID_RANK = "customfield_10019"
    FIELDID_SPRINTS = "customfield_10020"
    FIELDID_ISSUETYPE = "issuetype"
    FIELDID_SUMMARY = "summary"
    FIELDID_STATUS = "status"
    FIELDID_FIX_VERSIONS = "fixVersions"

    def __init__(
        self,
        name: str,
        domain: str,
        username: str,
        api_token: str,
        jql: str,
        board_id: str,
    ) -> None:
        self._jira_requests = JiraCloudRequests(
            JiraCloudCredentials(domain, username, api_token)
        )
        self._jql = jql
        self._board_id = board_id
        super().__init__(
            name=name,
            config={
                "jql": jql,
                "board_id": board_id,
            },
        )

    @override
    def load_milestones(self, app: MilestoneDataSourceApplication) -> None:
        versions = self._jira_requests.get(
            self.PATH_BOARD_VERSIONS % self._board_id,
            paged_data_key="values",
            progress_desc="Loading Jira versions",
        )

        for version in versions:
            release_date = None
            if "releaseDate" in version:
                release_date = (
                    datetime.strptime(version["releaseDate"], "%Y-%m-%dT%H:%M:%S.%f%z")
                    .astimezone(pytz.utc)
                    .replace(tzinfo=None)
                )
            app.create_or_update_milestone(
                url=version["self"],
                identifier=version["id"],
                name=version["name"],
                description=(
                    version["description"] if "description" in version else None
                ),
                released=version["released"],
                release_date=release_date,
            )

    @override
    def load_sprints(self, app: SprintDataSourceApplication) -> None:
        board = next(
            self._jira_requests.get(
                self.PATH_BOARD % self._board_id, progress_desc="Getting Jira board"
            )
        )

        if board["type"] != "scrum":
            # Jira board is not a scrum board
            return

        sprints = self._jira_requests.get(
            self.PATH_BOARD_SPRINTS % self._board_id,
            paged_data_key="values",
            progress_desc="Loading Jira sprints",
        )

        for index, sprint in enumerate(sprints):
            app.create_or_update_sprint(
                sprint["self"], sprint["id"], sprint["state"], sprint["name"], index
            )

    def _append(self, ls: list[str], value: str) -> None:
        ls.append(value)

    def _remove(self, ls: list[str], value: str) -> None:
        ls.remove(value)

    def _apply_operations(
        self, operations: Iterable[tuple[str, str]], ls: list[str]
    ) -> None:
        for operation, value in operations:
            if operation == "append":
                ls.append(value)
            elif operation == "remove":
                if value in ls:  # TODO: fix the problem with CDP-6, DO NOT COMMIT!
                    ls.remove(value)
            else:
                raise ValueError(f"Invalid operation: {operation}")

    @override
    def load_items(self, app: ItemDataSourceApplication) -> None:
        last_updated_str = self.option_storage.get_option("last_updated_milliseconds")

        jql = self._jql
        if last_updated_str:
            # The search results will be relative configured time zone (which is by default the Jira server's time zone).
            # To ensure we do not miss any updates, we'll query all 24h earlier updates again.
            last_updated_milliseconds = int(last_updated_str) - 1000 * 60 * 60 * 24
            jql = f"{self._jql} AND (updated >= {last_updated_milliseconds} OR created >= {last_updated_milliseconds})"

        print(jql)
        last_updated_milliseconds = int(datetime.now(pytz.utc).timestamp() * 1000)

        items = self._jira_requests.post(
            self.PATH_SEARCH_ISSUES,
            params={
                "fields": [
                    "summary",
                    "status",
                    "issuetype",
                    "created",
                    self.FIELDID_RANK,
                    self.FIELDID_SPRINTS,
                    self.FIELDID_FIX_VERSIONS,
                ],
                "jql": jql,
            },
            paged_data_key="issues",
            progress_desc="Loading Jira issues",
        )

        for item in items:
            existing_item = app.get_item(item["self"])
            params = {}
            changelog_tracking_id = 0
            if existing_item:
                changelog_tracking_id = existing_item["changelog_tracking_id"] or 0
                params = {"startAt": changelog_tracking_id}

            changelogs = list(
                self._jira_requests.get(
                    self.PATH_GET_ISSUE_CHANGELOG % item["key"],
                    params=params,
                    paged_data_key="values",
                    progress_desc=f"Loading Jira issue {item['key']} changelog",
                )
            )

            url = item["self"]

            # Changelog does contain event for rank change, but actual value is stored only in the issue, so we do not have
            # historical ranking changelog available.
            rank = item["fields"][self.FIELDID_RANK]

            # For simplicity we do not currently support changelog based hierarchy level, but use the latest value
            # stored in issue. The drawback of this is that issue hierarchy level changes get out of sync with our
            # internal hierarchy level events when updates are fetched from Jira. As a workaround one needs to full
            # re-fetch all issues to get the latest and correct hierarchy level.
            #
            # To support hierarchy level change events correctly changelog fields IssueParentAssociation and/or issuetype
            # should be handled. The most straightforward implementation would be using issuetype field with configurable
            # mapping of issue types to hierarchy levels.
            hierarchy_level = item["fields"]["issuetype"]["hierarchyLevel"]

            if not existing_item:
                initial_summary = None
                initial_status = None
                initial_issue_type = None

                # Initially this will be the last known set of sprints for item
                # Reversed changelog changes will be undone on this value, resulting in
                # the initial set of sprints
                initial_sprints: list[str] = (
                    [str(s["id"]) for s in item["fields"][self.FIELDID_SPRINTS]]
                    if item["fields"][self.FIELDID_SPRINTS] is not None
                    else []
                )
                changelog_sprint_ops: list[tuple[str, str]] = []

                # Initially this will be the last known set of fix versions for item
                # Reversed changelog changes will be undone on this value, resulting in
                # the initial set of fix versions
                initial_fix_versions: list[str] = [
                    v["id"] for v in item["fields"][self.FIELDID_FIX_VERSIONS]
                ]
                changelog_fix_version_ops: list[tuple[str, str]] = []

                for changelog in changelogs:
                    # print(changelog)
                    for changelog_item in changelog["items"]:
                        # print(changelog_item)

                        if (
                            initial_summary is None
                            and "fieldId" in changelog_item
                            and changelog_item["fieldId"] == self.FIELDID_SUMMARY
                        ):
                            initial_summary = changelog_item["fromString"]

                        if (
                            initial_status is None
                            and "fieldId" in changelog_item
                            and changelog_item["fieldId"] == self.FIELDID_STATUS
                        ):
                            initial_status = changelog_item["fromString"]

                        if (
                            initial_issue_type is None
                            and "fieldId" in changelog_item
                            and changelog_item["fieldId"] == self.FIELDID_ISSUETYPE
                        ):
                            initial_issue_type = changelog_item["fromString"]

                        from_value: str = changelog_item["from"]
                        to_value: str = changelog_item["to"]

                        if (
                            "fieldId" in changelog_item
                            and changelog_item["fieldId"] == self.FIELDID_SPRINTS
                        ):
                            from_sprints = (
                                set(from_value.split(", ")) if from_value else set()
                            )
                            to_sprints = (
                                set(to_value.split(", ")) if to_value else set()
                            )

                            additions = {s.strip() for s in to_sprints - from_sprints}
                            removals = {s.strip() for s in from_sprints - to_sprints}

                            # Create lambda functions that can be executed later in reverse order
                            # to reconstruct the initial state by undoing the changelog changes
                            for added in additions:
                                changelog_sprint_ops.append(("remove", added))
                            for removed in removals:
                                changelog_sprint_ops.append(("append", removed))

                        if (
                            "fieldId" in changelog_item
                            and changelog_item["fieldId"] == self.FIELDID_FIX_VERSIONS
                        ):
                            # Create lambda functions that can be executed later in reverse order
                            # to reconstruct the initial state by undoing the changelog changes
                            if from_value and not to_value:
                                # Version was removed - append it back in reverse
                                changelog_fix_version_ops.append(("append", from_value))
                            elif to_value and not from_value:
                                # Version was added - remove it in reverse
                                changelog_fix_version_ops.append(("remove", to_value))
                            else:
                                raise ValueError(
                                    f"Unexpected changelog state for {self.FIELDID_FIX_VERSIONS} field"
                                )

                # If there was no changes in changelog, set initial values to current values
                if initial_summary is None:
                    initial_summary = item["fields"]["summary"]

                if initial_status is None:
                    initial_status = item["fields"]["status"]["name"]

                if initial_issue_type is None:
                    initial_issue_type = item["fields"]["issuetype"]["name"]

                # Undo changes to reconstruct initial state for sprints
                self._apply_operations(reversed(changelog_sprint_ops), initial_sprints)

                # Undo changes to reconstruct initial state for sprints
                self._apply_operations(
                    reversed(changelog_fix_version_ops), initial_fix_versions
                )

                timestamp = (
                    datetime.strptime(
                        item["fields"]["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                    .astimezone(pytz.utc)
                    .replace(tzinfo=None)
                )

                app.create_item(
                    url=url,
                    timestamp=timestamp,
                    identifier=item["key"],
                    summary=initial_summary,
                    status=initial_status,
                    hierarchy_level=hierarchy_level,
                    rank=rank,
                    sprints=initial_sprints,
                    milestones=initial_fix_versions,
                    item_type=initial_issue_type,
                )

            last_changelog_timestamp = None
            for changelog in changelogs:
                timestamp = (
                    datetime.strptime(changelog["created"], "%Y-%m-%dT%H:%M:%S.%f%z")
                    .astimezone(pytz.utc)
                    .replace(tzinfo=None)
                )
                changelog_tracking_id += 1
                last_changelog_timestamp = timestamp

                for changelog_item in changelog["items"]:
                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.FIELDID_SUMMARY
                    ):
                        app.change_summary(
                            url,
                            timestamp,
                            changelog_item["toString"],
                            changelog_tracking_id=changelog_tracking_id,
                        )
                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.FIELDID_STATUS
                    ):
                        app.change_status(
                            url,
                            timestamp,
                            changelog_item["toString"],
                            changelog_tracking_id=changelog_tracking_id,
                        )
                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.FIELDID_ISSUETYPE
                    ):
                        app.change_type(
                            url,
                            timestamp,
                            changelog_item["toString"],
                            changelog_tracking_id=changelog_tracking_id,
                        )
                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.FIELDID_SPRINTS
                    ):
                        from_value = changelog_item["from"]  # e.g. '114, 147'
                        to_value = changelog_item["to"]  # e.g. '114'

                        from_sprints = (
                            set(from_value.split(", ")) if from_value else set()
                        )
                        to_sprints = set(to_value.split(", ")) if to_value else set()

                        added_sprints = to_sprints - from_sprints
                        removed_sprints = from_sprints - to_sprints

                        for sprint_identifier in removed_sprints:
                            app.remove_sprint(
                                url,
                                timestamp,
                                int(sprint_identifier),
                                changelog_tracking_id=changelog_tracking_id,
                            )

                        for sprint_identifier in added_sprints:
                            app.add_sprint(
                                url,
                                timestamp,
                                int(sprint_identifier),
                                changelog_tracking_id=changelog_tracking_id,
                            )

                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.FIELDID_FIX_VERSIONS
                    ):
                        from_value = changelog_item["from"]
                        to_value = changelog_item["to"]

                        if from_value and not to_value:
                            app.remove_milestone(
                                url,
                                timestamp,
                                int(from_value),
                                changelog_tracking_id=changelog_tracking_id,
                            )
                        elif to_value and not from_value:
                            app.add_milestone(
                                url,
                                timestamp,
                                int(to_value),
                                changelog_tracking_id=changelog_tracking_id,
                            )
                        else:
                            raise ValueError(
                                f"Unexpected changelog state for {self.FIELDID_FIX_VERSIONS} field"
                            )

                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.FIELDID_RANK
                    ):
                        app.change_rank(
                            url,
                            timestamp,
                            rank,
                            changelog_tracking_id=changelog_tracking_id,
                        )

            if last_changelog_timestamp:
                app.set_changelog_tracking_id(
                    url,
                    last_changelog_timestamp,
                    changelog_tracking_id=changelog_tracking_id,
                )

        self.option_storage.set_option(
            "last_updated_milliseconds", str(last_updated_milliseconds)
        )
