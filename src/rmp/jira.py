import json
from datetime import datetime
from typing import Callable, Generator, Iterable

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

    def track(self, paged_response: dict, results_count: int) -> None:
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
    def next_page_params(self) -> dict:
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

    def __init__(self, credentials: JiraCloudCredentials, **kwargs) -> None:
        self._credentials = credentials

    def get(
        self,
        path,
        params=None,
        paged_callback: Callable | None = None,
        paged_data_key=None,
        progress_desc=None,
        **kwargs,
    ) -> Generator[dict, None, None]:
        return self.execute(
            self._get_request,
            path,
            params=params,
            paged_callback=paged_callback,
            paged_data_key=paged_data_key,
            progress_desc=progress_desc,
            **kwargs,
        )

    def _get_request(self, url, params, auth, headers) -> requests.Response:
        return requests.get(url, params=params, auth=auth, headers=headers)

    def post(
        self,
        path,
        params=None,
        paged_callback: Callable | None = None,
        paged_data_key=None,
        progress_desc=None,
        **kwargs,
    ) -> Generator[dict, None, None]:
        return self.execute(
            self._post_request,
            path,
            params=params,
            paged_callback=paged_callback,
            paged_data_key=paged_data_key,
            progress_desc=progress_desc,
            **kwargs,
        )

    def _post_request(self, url: str, params, auth, headers) -> requests.Response:
        return requests.post(url, data=json.dumps(params), auth=auth, headers=headers)

    def execute(
        self,
        request_call: Callable,
        path,
        params=None,
        paged_data_key=None,
        progress_desc=None,
        **kwargs,
    ) -> Generator[dict, None, None]:
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
            return response.json()

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
    PATH_BOARD_SPRINTS = "rest/agile/1.0/board/%s/sprint"
    PATH_BOARD_VERSIONS = "rest/agile/1.0/board/%s/version"

    RANK_FIELD = "customfield_10019"
    SPRINTS_FIELD = "customfield_10020"
    FIX_VERSIONS_FIELD = "fixVersions"

    def __init__(
        self,
        name: str,
        domain: str,
        username: str,
        api_token: str,
        jql: str,
        board_id: int,
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
            release_date = (
                datetime.strptime(version["releaseDate"], "%Y-%m-%dT%H:%M:%S.%f%z")
                .astimezone(pytz.utc)
                .replace(tzinfo=None)
            )
            app.create_or_update_milestone(
                version["self"],
                version["id"],
                version["name"],
                version["description"],
                release_date,
                version["released"],
            )

    @override
    def load_sprints(self, app: SprintDataSourceApplication) -> None:
        sprints = self._jira_requests.get(
            self.PATH_BOARD_SPRINTS % self._board_id,
            paged_data_key="values",
            progress_desc="Loading Jira sprints",
        )

        for index, sprint in enumerate(sprints):
            app.create_or_update_sprint(
                sprint["self"], sprint["id"], sprint["state"], sprint["name"], index
            )

    def _append(self, ls: list, value):
        ls.append(value)

    def _remove(self, ls: list, value):
        ls.remove(value)

    def _apply_operations(self, operations: Iterable[tuple[str, str]], ls: list[str]):
        for operation, value in operations:
            if operation == "append":
                ls.append(value)
            elif operation == "remove":
                ls.remove(value)
            else:
                raise ValueError(f"Invalid operation: {operation}")

    @override
    def load_items(self, app: ItemDataSourceApplication) -> None:
        items = self._jira_requests.post(
            self.PATH_SEARCH_ISSUES,
            params={
                "fields": [
                    "summary",
                    "status",
                    "issuetype",
                    "created",
                    self.RANK_FIELD,
                    self.SPRINTS_FIELD,
                    self.FIX_VERSIONS_FIELD,
                ],
                "jql": self._jql,
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

            if not existing_item:
                initial_summary = None
                initial_status = None
                initial_hierarchy_level = None

                # Initially this will be the last known set of sprints for item
                # Reversed changelog changes will be undone on this value, resulting in
                # the initial set of sprints
                initial_sprints: list[str] = (
                    [str(s["id"]) for s in item["fields"][self.SPRINTS_FIELD]]
                    if item["fields"][self.SPRINTS_FIELD] is not None
                    else []
                )
                changelog_sprint_ops: list[tuple[str, str]] = []

                # Initially this will be the last known set of fix versions for item
                # Reversed changelog changes will be undone on this value, resulting in
                # the initial set of fix versions
                initial_fix_versions: list[str] = [
                    v["id"] for v in item["fields"][self.FIX_VERSIONS_FIELD]
                ]
                changelog_fix_version_ops: list[tuple[str, str]] = []

                for changelog in changelogs:
                    # print(changelog)
                    for changelog_item in changelog["items"]:
                        # print(changelog_item)

                        if (
                            initial_summary is None
                            and changelog_item["field"] == "summary"
                        ):
                            initial_summary = changelog_item["fromString"]

                        if (
                            initial_status is None
                            and changelog_item["field"] == "status"
                        ):
                            initial_status = changelog_item["fromString"]

                        if (
                            initial_hierarchy_level is None
                            and changelog_item["field"] == "hierarchyLevel"
                        ):
                            initial_hierarchy_level = changelog_item["fromString"]

                        from_value: str = changelog_item["from"]
                        to_value: str = changelog_item["to"]

                        if (
                            "fieldId" in changelog_item
                            and changelog_item["fieldId"] == self.SPRINTS_FIELD
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
                            and changelog_item["fieldId"] == self.FIX_VERSIONS_FIELD
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
                                    f"Unexpected changelog state for {self.FIX_VERSIONS_FIELD} field"
                                )

                # If there was no changes in changelog, set initial values to current values
                if initial_summary is None:
                    initial_summary = item["fields"]["summary"]

                if initial_status is None:
                    initial_status = item["fields"]["status"]["name"]

                if initial_hierarchy_level is None:
                    initial_hierarchy_level = item["fields"]["issuetype"][
                        "hierarchyLevel"
                    ]

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

                # Changelog does contain event for rank change, but actual value is stored only in the issue, so we do not have
                # historical ranking changelog available.
                rank = item["fields"][self.RANK_FIELD]

                app.create_item(
                    url,
                    timestamp,
                    item["key"],
                    initial_summary,
                    initial_status,
                    initial_hierarchy_level,
                    rank,
                    initial_sprints,
                    initial_fix_versions,
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
                    if changelog_item["field"] == "summary":
                        app.change_summary(
                            url,
                            timestamp,
                            changelog_item["toString"],
                            changelog_tracking_id=changelog_tracking_id,
                        )
                    if changelog_item["field"] == "status":
                        app.change_status(
                            url,
                            timestamp,
                            changelog_item["toString"],
                            changelog_tracking_id=changelog_tracking_id,
                        )
                    if changelog_item["field"] == "hierarchyLevel":
                        app.change_hierarchy_level(
                            url,
                            timestamp,
                            changelog_item["toString"],
                            changelog_tracking_id=changelog_tracking_id,
                        )
                    if (
                        "fieldId" in changelog_item
                        and changelog_item["fieldId"] == self.SPRINTS_FIELD
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
                        and changelog_item["fieldId"] == self.FIX_VERSIONS_FIELD
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
                                f"Unexpected changelog state for {self.FIX_VERSIONS_FIELD} field"
                            )

            if last_changelog_timestamp:
                app.set_changelog_tracking_id(
                    url,
                    last_changelog_timestamp,
                    changelog_tracking_id=changelog_tracking_id,
                )
