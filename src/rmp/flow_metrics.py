from __future__ import annotations  # https://github.com/pandas-dev/pandas/issues/54494
from typing import Any, List, TypedDict, Unpack
from matplotlib import pyplot as plt
from matplotlib import dates as mdates
from matplotlib.axes import Axes
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp
from sqlalchemy import Engine, Select, and_, or_, case, literal
from sqlalchemy.orm import Session
from rmp.sql_model import (
    Item,
    ItemRankChangelog,
    ItemStatusChangelog,
    Milestone,
    Sprint,
    SprintItem,
    MilestoneItem,
)
from sqlalchemy import func, union_all, select
from datetime import datetime, timezone
from datetimerange import DateTimeRange
import matplotlib.ticker as tck
from pandas.io.formats.style import Styler

pd.options.mode.copy_on_write = True


class FilterKwArgs(TypedDict, total=False):
    include_hierarchy_levels: set[int] | None
    exclude_item_types: set[str] | None
    exclude_ranges: list[DateTimeRange] | None
    as_of: datetime | None


class Workflow:
    def __init__(
        self,
        not_started: list[str] = [],
        in_progress: list[str] = [],
        finished: list[str] = [],
    ) -> None:
        self._not_started = not_started
        self._in_progress = in_progress
        self._finished = finished

    def to_snake_case(self, text: str) -> str:
        return text.lower().replace(" ", "_")


class FlowMetrics:
    def __init__(self, engine: Engine, workflow: Workflow) -> None:
        self._engine = engine
        self._workflow = workflow
        self._validate_workflow_statuses()

    def _select_status_changelog_distinct_arrival(
        self, **kwargs: Unpack[FilterKwArgs]
    ) -> Select[tuple[Any, ...]]:
        include_hierarchy_levels = kwargs.get("include_hierarchy_levels")
        exclude_item_types = kwargs.get("exclude_item_types")

        with_filtered = (
            select(
                ItemStatusChangelog.item_id,
                ItemStatusChangelog.start_time,
                ItemStatusChangelog.status,
            )
            .join(Item, Item.id == ItemStatusChangelog.item_id)
            .where(
                Item.hierarchy_level.in_(
                    include_hierarchy_levels if include_hierarchy_levels else {0}
                ),
                Item.item_type.notin_(exclude_item_types if exclude_item_types else {}),
            )
        ).cte("_filtered")

        # Merge finished statuses into one since we are only interested of arrivals into
        # any of the finished statuses
        stmt = select(
            with_filtered.c.item_id,
            with_filtered.c.start_time,
            case(
                (
                    with_filtered.c.status.in_(self._workflow._finished),
                    "/".join(self._workflow._finished),
                ),
                else_=with_filtered.c.status,
            ).label("status"),
        ).cte("_merged_statuses")

        # Mark the order in which the status appears in workflow
        statuses = (
            self._workflow._not_started
            + self._workflow._in_progress
            + ["/".join(self._workflow._finished)]
        )
        whens = [(stmt.c.status == status, i + 1) for i, status in enumerate(statuses)]
        stmt = select(stmt, case(*whens).label("status_order")).cte()

        # Mark status discarded if there exists later backwards transition
        # into status that precedes the status's order in workflow.
        # E.g. in case of To Do -> In Progress -> To Do transition, the
        # In Progress will be discarded.
        c1 = stmt.alias("c1")
        c2 = stmt.alias("c2")
        stmt = (
            select(
                c1.c.item_id,
                c1.c.status,
                c1.c.start_time,
                c1.c.status_order,
                # Mark whether this transition should be discarded because
                # there is a later status change transitioning back in workflow order
                func.max(
                    and_(
                        c2.c.status_order < c1.c.status_order,
                        c2.c.start_time > c1.c.start_time,
                    )
                )
                .over(partition_by=[c1.c.item_id, c1.c.start_time, c1.c.status_order])
                .label("discard"),
                c2.c.status.label("status_test"),
                c2.c.start_time.label("start_time_test"),
                c2.c.status_order.label("status_order_test"),
                and_(
                    c2.c.status_order < c1.c.status_order,
                    c2.c.start_time > c1.c.start_time,
                ).label("discard_test"),
            )
            .select_from(c1)
            .join(c2, c1.c.item_id == c2.c.item_id)
            .order_by(c1.c.item_id, c1.c.status_order, c1.c.start_time, c2.c.start_time)
            .cte("_mark_discarded")
        )

        # Select only statuses not marked as discarded.
        # This still may contain duplicate statuses if there are
        # multiple transitions into the status
        not_discarded = (
            select(
                stmt.c.item_id, stmt.c.status, stmt.c.start_time, stmt.c.status_order
            )
            .where(stmt.c.discard == 0)
            .distinct()
            .cte("_not_discarded")
        )

        status_selects = [
            select(
                literal(status).label("status"),
                literal(None).label("start_time"),
                literal(i + 1).label("status_order"),
            )
            for i, status in enumerate(statuses)
        ]
        generated_statuses = union_all(*status_selects).cte("_generated_statuses")

        joined_generated = (
            select(
                not_discarded.c.item_id,
                generated_statuses.c.status,
                generated_statuses.c.start_time,
                generated_statuses.c.status_order,
            )
            .select_from(generated_statuses)
            .join(not_discarded, literal(True))
            .cte("_joined_generated")
        )

        combined = union_all(
            select(
                joined_generated.c.item_id,
                joined_generated.c.status,
                joined_generated.c.start_time,
                joined_generated.c.status_order,
            ).distinct(),
            select(
                not_discarded.c.item_id,
                not_discarded.c.status,
                not_discarded.c.start_time,
                not_discarded.c.status_order,
            ),
        ).cte("_combined")

        # Keep only those status changes that have arrived earliest into each status
        mark_earliest_per_status = select(
            combined,
            func.rank()
            .over(
                partition_by=[combined.c.item_id, combined.c.status],
                order_by=[
                    combined.c.start_time.is_(None).asc(),
                    combined.c.start_time.asc(),
                ],
            )
            .label("rank_earliest_per_status"),
        ).cte("_mark_earliest_per_status")

        earliest_selected = (
            select(
                mark_earliest_per_status.c.item_id,
                mark_earliest_per_status.c.status,
                mark_earliest_per_status.c.start_time,
                mark_earliest_per_status.c.status_order,
            )
            .where(mark_earliest_per_status.c.rank_earliest_per_status == 1)
            .order_by(
                mark_earliest_per_status.c.item_id,
                mark_earliest_per_status.c.status_order,
                mark_earliest_per_status.c.start_time,
            )
        )

        return earliest_selected

    def _select_status_changelog(
        self,
        **kwargs: Unpack[FilterKwArgs],
    ) -> Select[tuple[Any, ...]]:
        with_changelog = select(
            ItemStatusChangelog.item_id,
            ItemStatusChangelog.status,
            ItemStatusChangelog.start_time,
            func.coalesce(
                ItemStatusChangelog.end_time,
                datetime.now(),
            ).label("end_time"),
        ).cte("_changelog")

        # Rank statuses over time with an intention to find the last one
        rank_status = (
            func.rank()
            .over(
                partition_by=[with_changelog.c.item_id],
                order_by=with_changelog.c.start_time.desc(),
            )
            .label("rank_status")
        )

        # The first ranked status is the last status
        last_status = case(
            (rank_status == 1, with_changelog.c.status),
            else_=None,
        ).label("last_status")
        last_finished_time = case(
            (
                last_status.in_(self._workflow._finished),
                with_changelog.c.start_time,
            )
        ).label("last_finished_time")

        with_last_status = select(
            with_changelog,
            rank_status,
            last_status,
            last_finished_time,
        ).cte("_last_status")

        # Propagate current status to all rows for item
        current_status = (
            func.max(with_last_status.c.last_status)
            .over(partition_by=with_last_status.c.item_id)
            .label("current_status")
        )
        finished_time = (
            func.max(with_last_status.c.last_finished_time)
            .over(partition_by=with_last_status.c.item_id)
            .label("finished_time")
        )
        with_current_status = select(
            with_last_status,
            current_status,
            finished_time,
        ).cte("_current_status")

        # Join with item table for additional item information and final filtering
        include_hierarchy_levels = kwargs.get("include_hierarchy_levels")
        exclude_item_types = kwargs.get("exclude_item_types")

        filtered = (
            select(with_current_status, Item.identifier.label("item_identifier"))
            .join(Item, Item.id == with_current_status.c.item_id)
            .where(
                Item.hierarchy_level.in_(
                    include_hierarchy_levels if include_hierarchy_levels else {0}
                ),
                Item.item_type.notin_(exclude_item_types if exclude_item_types else {}),
            )
        )

        return filtered

    def _select_items_timeline(
        self, **kwargs: Unpack[FilterKwArgs]
    ) -> Select[tuple[Any, ...]]:
        include_hierarchy_levels = kwargs.get("include_hierarchy_levels")
        exclude_item_types = kwargs.get("exclude_item_types")
        as_of = kwargs.get("as_of")

        if not as_of:
            as_of = datetime.now(timezone.utc)

        finished_time = case(
            (
                ItemStatusChangelog.status.in_(self._workflow._finished),
                ItemStatusChangelog.start_time,
            )
        ).label("finished_time")

        items = (
            select(
                Item.id,
                Item.identifier,
                ItemStatusChangelog.status,
                ItemRankChangelog.rank,
                finished_time,
                Item.item_type,  # TODO: query from history
                Item.hierarchy_level,  # TODO: query from history
                Item.summary,  # TODO: query from history
            )
            .join(ItemStatusChangelog)
            .join(ItemRankChangelog)
            .order_by(ItemRankChangelog.rank)
            .where(
                # Include only item status effective at as_of
                ItemStatusChangelog.start_time <= as_of,
                or_(
                    ItemStatusChangelog.end_time > as_of,
                    ItemStatusChangelog.end_time.is_(None),
                ),
                # Include only item rank effective at as_of
                ItemRankChangelog.start_time <= as_of,
                or_(
                    ItemRankChangelog.end_time > as_of,
                    ItemRankChangelog.end_time.is_(None),
                ),
                # Include only items created at or later than as_of
                Item.created_time <= as_of,
                Item.hierarchy_level.in_(
                    include_hierarchy_levels if include_hierarchy_levels else {0}
                ),  # TODO: query from history
                Item.item_type.notin_(
                    exclude_item_types if exclude_item_types else {}
                ),  # TODO: query from history
            )
        ).cte("_items")

        # Item can be in multiple sprints, we are interested only in the latest sprint the item appears
        # Use window function to partition by the item_id and order the item's belonging to sprints
        # by sprint order so we can filter out duplicates later
        sprints_items_all = (
            select(
                Sprint,
                SprintItem,
                func.row_number()
                .over(
                    partition_by=SprintItem.item_id,
                    order_by=Sprint.order.desc().nulls_last(),
                )
                .label("row_num"),
            )
            .join(
                SprintItem,
                and_(
                    Sprint.id == SprintItem.sprint_id,
                    SprintItem.add_time <= as_of,
                    or_(
                        SprintItem.remove_time > as_of, SprintItem.remove_time.is_(None)
                    ),
                ),
            )
            .order_by(Sprint.order.desc().nulls_last())
        ).cte("_sprint_items_all")

        sprint_items = (
            select(sprints_items_all)
            .where(
                sprints_items_all.c.row_num == 1,
                or_(
                    sprints_items_all.c.complete_time >= as_of,
                    sprints_items_all.c.complete_time.is_(None),
                ),
            )
            .cte("_sprint_items")
        )

        items_with_sprints = (
            select(
                items,
                sprint_items.c.name.label("sprint_name"),
                sprint_items.c.state.label("sprint_state"),
                sprint_items.c.order.label("sprint_order"),
            )
            .select_from(items)
            .outerjoin(sprint_items, sprint_items.c.item_id == items.c.id)
            .order_by(sprint_items.c.order.desc().nulls_last(), items.c.rank)
        ).cte("_items_with_sprints")

        milestone_items = (
            select(Milestone, MilestoneItem)
            .join(MilestoneItem, Milestone.id == MilestoneItem.milestone_id)
            .where(
                MilestoneItem.add_time <= as_of,
                or_(
                    MilestoneItem.remove_time > as_of,
                    MilestoneItem.remove_time.is_(None),
                ),
            )
        ).cte("_milestone_items")

        items_with_milestones = (
            select(
                items_with_sprints,
                milestone_items.c.id.label("milestone_id"),
                milestone_items.c.name.label("milestone_name"),
                milestone_items.c.release_date.label("milestone_release_date"),
            )
            .select_from(items_with_sprints)
            .outerjoin(
                milestone_items,
                and_(
                    milestone_items.c.item_id == items_with_sprints.c.id,
                    milestone_items.c.remove_time.is_(None),
                ),
            )
        ).cte("_items_with_milestones")

        # The ordering below creates timeline of past (completed) and future items
        ordered = (
            select(items_with_milestones)
            # .where(return_cond)
            .order_by(
                items_with_milestones.c.finished_time.nulls_last(),
                items_with_milestones.c.sprint_order.desc().nulls_last(),
                items_with_milestones.c.rank,
            )
        )

        return ordered

    def df_cycle_time(self, status_changelog_with_durations: DataFrame) -> DataFrame:
        df = status_changelog_with_durations

        ct_df = (
            df[
                (df["current_status"].isin(self._workflow._finished))
                & (df["status"].isin(self._workflow._in_progress))
            ]
            .groupby(["item_id"])
            .agg(
                {
                    "duration": "sum",
                    "current_status": "max",
                    "finished_time": "max",
                    "item_identifier": "max",
                }
            )
            .rename(columns={"duration": "cycle_time"})
            .reset_index()
        )

        # Drop rows with zero cycle time, this normal when exclude ranges cancel out durations
        ct_df = ct_df[ct_df["cycle_time"] != pd.Timedelta(0)].reset_index(drop=True)

        return ct_df

    def df_status_cycle_time(
        self, status_changelog_with_durations: DataFrame
    ) -> DataFrame:
        df = status_changelog_with_durations
        ct_status_df = (
            df[
                (df["current_status"].isin(self._workflow._finished))
                & (df["status"].isin(self._workflow._in_progress))
            ]
            .groupby(["item_id", "status"])
            .agg(
                {
                    "duration": "sum",
                    "current_status": "max",
                    "finished_time": "max",
                    "item_identifier": "max",
                }
            )
            .rename(columns={"duration": "cycle_time"})
            .reset_index()
        )

        return ct_status_df

    def df_wip_age(self, status_changelog_with_durations: DataFrame) -> DataFrame:
        df = status_changelog_with_durations
        wip_age_df = (
            df[
                (df["current_status"].isin(self._workflow._in_progress))
                & (df["status"].isin(self._workflow._in_progress))
            ]
            .groupby(["item_id"])
            .agg({"duration": "sum", "current_status": "max", "item_identifier": "max"})
            .rename(columns={"duration": "wip_age"})
            .reset_index()
        )

        return wip_age_df

    def df_workflow_cum_arrivals(
        self,
        include_not_started_statuses: bool = False,
        **kwargs: Unpack[FilterKwArgs],
    ) -> DataFrame:
        df = pd.read_sql(
            sql=self._select_status_changelog_distinct_arrival(**kwargs),
            con=self._engine,
        )

        # Backfill start_time to consider arrival into any of preceding statuses
        df["start_time"] = df.groupby("item_id")["start_time"].bfill()

        # Rows which remained null represent statuses the item has not arrived, drop these
        df = df.dropna(subset=["start_time"])

        # Convert the 'start_time' column to datetime, including null values
        df["start_time"] = pd.to_datetime(df["start_time"])

        # Keep only the date part for day-precision
        df["start_time"] = df["start_time"].dt.normalize()

        # Aggregate by status and start_time, count rows per each group
        df = (
            df.groupby(["status", "start_time"])
            .size()
            .reset_index(name="arrival_count")
        )

        # For status calculate cumulative sum of arrivals over asc sorted date
        df["cumulative_count"] = (
            df.sort_values("start_time").groupby("status")["arrival_count"].cumsum()
        )

        # Widen the table
        df = df.pivot(index="start_time", columns="status", values="cumulative_count")

        # When there are no arrivals into status for certain date, the widened table contains nulls, ffill them with
        # previous date values
        df = df.ffill()

        # Sort columns according to the workflow
        statuses = self._workflow._in_progress + ["/".join(self._workflow._finished)]
        if include_not_started_statuses:
            statuses = self._workflow._not_started + statuses
        df = df.reindex(columns=statuses)

        return df

    def df_status_changelog_with_durations(
        self, **kwargs: Unpack[FilterKwArgs]
    ) -> DataFrame:
        df = pd.read_sql(
            sql=self._select_status_changelog(**kwargs),
            con=self._engine,
        )

        df["duration"] = df["end_time"] - df["start_time"]

        return df

    def df_throughput(
        self,
        resample_exclude_ranges: bool = True,
        **kwargs: Unpack[FilterKwArgs],
    ) -> Series[int]:
        df = self.df_cycle_time(self.df_status_changelog_with_durations(**kwargs))

        exclude_ranges = kwargs.get("exclude_ranges")
        if exclude_ranges:
            # Create a mask to keep rows not in any exclude range
            keep_mask = pd.Series(True, index=df.index)
            for exclude_range in exclude_ranges:
                if (
                    exclude_range.start_datetime is None
                    or exclude_range.end_datetime is None
                ):
                    raise ValueError(
                        "Both start_datetime and end_datetime must be provided in the DateTimeRange."
                    )

                range_mask = ~df["finished_time"].between(
                    exclude_range.start_datetime,
                    exclude_range.end_datetime,
                    inclusive="both",
                )
                keep_mask &= range_mask

            # Filter the dataframe
            df = df[keep_mask]

        # Prepare throughput data
        tp = df["finished_time"].value_counts().resample("D").sum()

        if not exclude_ranges:
            return tp

        if not resample_exclude_ranges:
            # Remove excluded ranges from throughput
            for range in exclude_ranges:
                # Remove elements from the Series that fall within the excluded range
                if range.start_datetime is not None and range.end_datetime is not None:
                    tp = tp[
                        ~tp.index.to_series().between(
                            range.start_datetime, range.end_datetime
                        )
                    ]

        return tp

    def df_monte_carlo_when(
        self,
        runs: int = 10000,
        item_count: int = 10,
        **kwargs: Unpack[FilterKwArgs],
    ) -> Series[int]:
        tp = self.df_throughput(resample_exclude_ranges=False, **kwargs)

        as_of = kwargs.get("as_of")
        if not as_of:
            as_of = datetime.now(timezone.utc)

        # Convert throughput series to numpy array for faster sampling
        tp_values = np.asarray(tp.values)

        multiplier = 2

        while True:
            # Estimate max days needed based on average throughput
            max_days = int(
                item_count / max(tp_values.mean(), 1) * multiplier
            )  # multiply by multiplier for safety margin

            # Pre-generate all random samples at once
            rng = np.random.default_rng()
            all_samples = rng.choice(tp_values, size=(runs, max_days))

            # Calculate cumulative sums for each run
            cumulative_sums = np.cumsum(all_samples, axis=1)

            # Check if any array has the last element as False
            if not np.any(cumulative_sums[:, -1] < item_count):
                # If all of the cumulative sums are reaching item count then we have reached the necessary shape of our result array
                break

            multiplier *= 2

        completion_indices = np.argmax(cumulative_sums >= item_count, axis=1)

        # Convert indices to dates
        completion_dates = [
            as_of + pd.DateOffset(days=int(idx + 1)) for idx in completion_indices
        ]

        # Convert to Series and count frequencies
        result = Series(completion_dates).value_counts().sort_index()

        return result

    def df_monte_carlo_how_many(
        self,
        target_date: datetime,
        runs: int = 10000,
        **kwargs: Unpack[FilterKwArgs],
    ) -> Series[int]:
        tp = self.df_throughput(resample_exclude_ranges=False, **kwargs)

        target_date_df = pd.Timestamp(target_date, tz=timezone.utc)
        start_date = pd.Timestamp.now(tz=timezone.utc)

        # Calculate the number of days to simulate
        days_to_simulate = (target_date_df - start_date).days

        # Convert throughput series to numpy array for faster sampling
        tp_values = np.asarray(tp.values)

        # Pre-generate all random samples at once
        rng = np.random.default_rng()
        all_samples = rng.choice(tp_values, size=(runs, days_to_simulate))

        # Calculate cumulative sums for each run
        cumulative_sums = np.cumsum(all_samples, axis=1)

        # Get the final counts for each run
        final_counts = cumulative_sums[:, -1]

        # Count frequencies of final counts
        result = Series(final_counts).value_counts().sort_index()

        return result

    def df_timeline_items(
        self,
        mc_when: bool = False,
        mc_when_runs: int = 1000,
        mc_when_percentile: int = 85,
        max_finished_items: int = 10,
        sort_in_progress_first: bool = True,
        **kwargs: Unpack[FilterKwArgs],
    ) -> DataFrame:
        df = pd.read_sql(
            sql=self._select_items_timeline(**kwargs),
            con=self._engine,
        )

        # Get unique milestone IDs, names and release dates
        m_df = (
            df[["milestone_id", "milestone_name", "milestone_release_date"]]
            .dropna(how="all")
            .drop_duplicates()
            .sort_values(
                by=[
                    "milestone_release_date",
                    "milestone_name",
                    "milestone_id",
                ]
            )
            .reset_index(drop=True)
        )

        # Create multiindex for columns
        df.columns = pd.MultiIndex.from_tuples([(c, "", "") for c in df.columns])

        # Add a column per milestone: ('milestones', milestone_name) with release_date if item has that milestone_id
        for _, m in m_df.iterrows():
            mid = m["milestone_id"]
            mname = m["milestone_name"]
            mdate = m["milestone_release_date"]
            # Ensure the milestone date is in UTC
            if mdate is not pd.NaT:
                mdate = pd.to_datetime(mdate, utc=True)

            mcol_date = ("milestones", mname, "date")
            mcol_isin = ("milestones", mname, "isin")
            mask = df["milestone_id"] == mid
            df[mcol_isin] = False
            df[mcol_date] = pd.NaT
            df[mcol_date] = pd.to_datetime(df[mcol_date], utc=True)

            df.loc[mask, mcol_isin] = True
            if mdate is not pd.NaT:
                df.loc[mask, mcol_date] = mdate

        # Aggregate all level 1 columns under 'milestone' using max,
        # this gets us the wide format of milestone columns
        milestone_cols = [col for col in df.columns if col[0] == "milestones"]
        agg_dict = {col: "max" for col in milestone_cols}

        df = (
            df.groupby(
                [
                    ("identifier", "", ""),
                    ("item_type", "", ""),
                    ("status", "", ""),
                    ("summary", "", ""),
                    ("sprint_order", "", ""),
                    ("finished_time", "", ""),
                    ("rank", "", ""),
                ],
                dropna=False,
            )
            .agg(agg_dict)
            .reset_index()
        )

        # Ensure finished_time is UTC
        df[("finished_time", "", "")] = pd.to_datetime(
            df[("finished_time", "", "")], utc=True
        )

        # Prepare column for Monte Carlo "when" simulation results
        df[("mc_when", "", "")] = pd.NaT

        statuses = (
            self._workflow._not_started
            + self._workflow._in_progress
            + self._workflow._finished
        )
        df[("status_order", "", "")] = df[("status", "", "")].apply(
            lambda s: statuses.index(s) if s in statuses else -1
        )

        df_finished = df[df[("finished_time", "", "")].notna()]
        df_finished = df_finished.sort_values(
            by=["finished_time"], na_position="last", ignore_index=True
        )

        # Limit the number of finished items to max_finished_items
        if max_finished_items is not None and max_finished_items >= 0:
            df_finished = df_finished.tail(max_finished_items)

        df_unfinished = df[df[("finished_time", "", "")].isna()]
        sort_cols = (
            ["status_order", "sprint_order", "rank"]
            if sort_in_progress_first
            else ["sprint_order", "rank"]
        )
        sort_asc = [False, True, True] if sort_in_progress_first else [True, True]
        df_unfinished = df_unfinished.sort_values(
            by=sort_cols,
            ascending=sort_asc,
            na_position="last",
            ignore_index=True,
        )

        # Run Monte Carlo "when" simulation for each backlog item row
        if mc_when:
            df_unfinished[("mc_when", "", "")] = df_unfinished.index.map(
                lambda x: self._get_mc_when_date(
                    self.df_monte_carlo_when(
                        mc_when_runs,
                        item_count=x + 1,
                        **kwargs,
                    ),
                    percentile=mc_when_percentile,
                )
            )
            df_unfinished[("mc_when", "", "")] = df_unfinished[
                ("mc_when", "", "")
            ].dt.normalize()  # Keep only day precision

        # Combine finished and unfinished items so they are in timeline order
        df = pd.concat([df_finished, df_unfinished], ignore_index=True)

        # Ensure mc_when is UTC
        df[("mc_when", "", "")] = pd.to_datetime(df[("mc_when", "", "")], utc=True)

        return df

    def styled_timeline_items(self, df: DataFrame) -> Styler:
        # Create styled DataFrame
        df[("date", "", "")] = df[("finished_time", "", "")].combine_first(
            df[("mc_when", "", "")]
        )

        df["y", "", ""] = df["date", "", ""].dt.year
        df["m", "", ""] = df["date", "", ""].dt.month
        df["w", "", ""] = df["date", "", ""].dt.isocalendar().week

        milestone_cols = []
        for col in [c for c in df.columns if c[0] == "milestones" and c[2] == "isin"]:
            milestone = col[1]

            isin_col = ("milestones", milestone, "isin")
            date_col = ("milestones", milestone, "date")
            diff_col = ("milestones", milestone, "diff")
            timeline_date_col = ("date", "", "")

            isin = df[isin_col]
            milestone_date = df[date_col]
            timeline_date = df[timeline_date_col]

            # Default to NaN
            df[diff_col] = pd.NA

            # Where isin is True and either milestone date or timeline date is nan, set to True
            mask_empty = isin & (milestone_date.isna() | timeline_date.isna())
            df.loc[mask_empty, diff_col] = True

            # Where isin is True and both dates are present, set to diff in days
            mask_diff = isin & milestone_date.notna() & timeline_date.notna()
            df.loc[mask_diff, diff_col] = (
                timeline_date[mask_diff] - milestone_date[mask_diff]
            ).dt.days.astype(int)

            df[(milestone, "", "")] = df[diff_col]

            # Keep only milestones that have items in them
            if df[diff_col].notna().any():
                milestone_cols.append(milestone)

        # Keep only topmost level of column multiindex
        df.columns = df.columns.get_level_values(0)

        df["date"] = df["date"].dt.date
        df = df[
            ["y", "m", "w", "date", "identifier", "item_type", "status", "summary"]
            + milestone_cols
        ]

        return df.style.pipe(
            FlowMetrics._style_timeline,
            workflow=self._workflow,
            milestone_cols=milestone_cols,
        )

    @staticmethod
    def _style_timeline(
        style: Styler, workflow: Workflow, milestone_cols: list[str]
    ) -> Styler:
        # Color statuses
        style.apply(
            lambda row: [
                "background-color: lightgreen"
                if col == "status" and row[col] in workflow._finished
                else ""
                for col in row.index
            ],
            axis=1,
        )
        style.apply(
            lambda row: [
                "background-color: orange"
                if col == "status" and row[col] in workflow._in_progress
                else ""
                for col in row.index
            ],
            axis=1,
        )
        style.apply(
            lambda row: [
                "background-color: pink"
                if col == "status" and row[col] in workflow._not_started
                else ""
                for col in row.index
            ],
            axis=1,
        )

        # milestone_cols = [col for col in style.columns if col[0] == "milestones"]
        styled_milestone_cols = [
            {
                "selector": f"th.col_heading.level0.col{style.columns.get_loc(col)}",
                "props": [
                    ("writing-mode", "vertical-rl"),
                    ("transform", "rotate(180deg)"),
                    ("white-space", "nowrap"),
                    ("text-align", "left"),
                ],
            }
            for col in milestone_cols
        ]
        style.set_table_styles(styled_milestone_cols, axis=1, overwrite=False)  # type: ignore[arg-type]

        # Format and highlight milestone data values
        style.format(
            FlowMetrics._format_milestone_diff, subset=milestone_cols, na_rep=""
        )

        style.map(FlowMetrics._highlight_milestone_diff, subset=milestone_cols)

        # Prevent wrapping of text in all cells
        style.set_properties(subset=None, **{"white-space": "nowrap"})

        return style

    @staticmethod
    def _format_milestone_diff(val: object) -> str:
        if isinstance(val, bool):
            return ""
        if isinstance(val, (np.integer, int, float, np.floating)):
            sign = ""
            if val >= 0:
                sign = "+"
            return f"{sign}{int(val)}"
        return str(val)

    @staticmethod
    def _highlight_milestone_diff(val: Any) -> str | None:
        if isinstance(val, bool) and val is True:
            return "background-color: lightblue; color: black;"
        if isinstance(val, int):
            if val <= 0:
                return "background-color: green; color: white;"
            elif val > 0:
                return "background-color: red; color: white;"
        return None

    def plot_cfd(
        self,
        include_not_started_statuses: bool = False,
        **kwargs: Unpack[FilterKwArgs],
    ) -> None:
        df = self.df_workflow_cum_arrivals(
            include_not_started_statuses=include_not_started_statuses, **kwargs
        )

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()

        df.plot(ax=ax, kind="area", stacked=False, alpha=1)

        highlight_ranges = kwargs.get("exclude_ranges")
        if highlight_ranges:
            self._highlight_exclude_ranges(ax, highlight_ranges, None)

    def plot_cycle_time_scatter(
        self,
        percentiles: list[int] = [50, 70, 85, 95],
        annotate_item_ids: bool = True,
        **kwargs: Unpack[FilterKwArgs],
        # only_hierarchy_levels: set[int] = {0},
    ) -> None:
        df = self.df_status_changelog_with_durations(**kwargs)
        ct_df = self.df_cycle_time(df)

        df_ranges_excluded = self._df_exclude_ranges_from_durations(df, **kwargs)
        ct_df_ranges_excluded = self.df_cycle_time(df_ranges_excluded)

        # Calculate quantile values for cycle time, useful for forecasting single item cycle time
        ct_q = ct_df_ranges_excluded["cycle_time"].quantile(
            [p / 100 for p in percentiles]
        )

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()

        ax.scatter(
            x=ct_df["finished_time"],
            y=ct_df["cycle_time"].dt.ceil("D").dt.days,
            label="Cycle Time (days)",
        )

        # Highlight excluded ranges
        exclude_ranges = kwargs.get("exclude_ranges")
        if exclude_ranges:
            self._highlight_exclude_ranges(ax, exclude_ranges, "Ranges excluded")

        if annotate_item_ids:
            for _, row in ct_df.iterrows():
                ax.annotate(
                    row["item_identifier"],
                    (row["finished_time"], row["cycle_time"].ceil("D").days),
                )

        # Plot cycle time quantiles
        min_finished = ct_df["finished_time"].min()
        max_finished = ct_df["finished_time"].max()
        for p in ct_q.index:
            ct_q_value = ct_q[p].ceil("D").days
            ax.plot((min_finished, max_finished), (ct_q_value, ct_q_value), "--")
            ax.annotate(f"{ct_q_value}", (min_finished, ct_q_value))
            ax.annotate(f"{int(p * 100)}%", (max_finished, ct_q_value))

        ax.legend()

    def plot_cycle_time_histogram(
        self,
        percentiles: list[int] = [50, 85, 95],
        **kwargs: Unpack[FilterKwArgs],
    ) -> None:
        df = self.df_status_changelog_with_durations(**kwargs)
        df_ranges_excluded = self._df_exclude_ranges_from_durations(df, **kwargs)
        ct_df = self.df_cycle_time(df_ranges_excluded)

        # Calculate quantile values for cycle time, useful for forecasting single item cycle time
        ct_q = ct_df["cycle_time"].quantile([p / 100 for p in percentiles])

        cycle_times = ct_df["cycle_time"].dt.ceil("D").dt.days
        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.hist(cycle_times, bins=int(cycle_times.max()))
        ax.yaxis.set_major_locator(tck.MultipleLocator())

        for p in ct_q.index:
            ct_q_value = ct_q[p].ceil("D").days
            ax.plot(
                (ct_q_value, ct_q_value),
                [0, cycle_times.value_counts().max()],
                label=f"{int(p * 100)}%",
            )

        exclude_ranges = kwargs.get("exclude_ranges")
        if exclude_ranges:
            self._annotate_exclude_ranges(ax, exclude_ranges)

        ax.legend(loc="upper right")

    def plot_aging_wip(
        self,
        percentiles: list[int] = [50, 70, 85, 95],
        **kwargs: Unpack[FilterKwArgs],
    ) -> None:
        df = self.df_status_changelog_with_durations(**kwargs)
        df_ranges_excluded = self._df_exclude_ranges_from_durations(df, **kwargs)

        # Item cycle time
        ct_df_ranges_excluded = self.df_cycle_time(df_ranges_excluded)

        # Item status cycle time
        ct_status_df = self.df_status_cycle_time(df_ranges_excluded)

        # Item wip age
        wip_age_df = self.df_wip_age(df_ranges_excluded)

        # Calculate quantile values for cycle time
        ct_q = ct_df_ranges_excluded["cycle_time"].quantile(
            [p / 100 for p in percentiles]
        )

        # The plot will be generated for each "in progress" and "finished" status
        statuses = self._workflow._in_progress + self._workflow._finished

        # Calculate the plot max height
        plot_h = max(wip_age_df["wip_age"].max(), ct_q.max()).days * 1.1

        fig = plt.figure(figsize=(16, 9))
        index = 0
        include_statuses = []

        exclude_ranges = kwargs.get("exclude_ranges")

        for status in statuses:
            index += 1
            # For each status, add subplot
            ax = fig.add_subplot(1, len(statuses), index)
            ax.set_xlabel(status)
            ax.set_xticks([])

            ax.set_ylim(bottom=0, top=plot_h)

            if index == 1:
                if exclude_ranges:
                    self._annotate_exclude_ranges(ax, exclude_ranges)

            if index != 1:
                ax.get_yaxis().set_visible(False)

            # Plot pace percentiles (status based cycle times)
            if status in self._workflow._in_progress:
                include_statuses.append(status)

                cur_status_ct_df = (
                    ct_status_df[ct_status_df["status"].isin(include_statuses)]
                    .groupby(["item_id"])
                    .agg({"cycle_time": "sum"})
                    .reset_index()
                )

                pace_q = cur_status_ct_df["cycle_time"].quantile(
                    [p / 100 for p in percentiles]
                )

                pace_colors = [
                    "xkcd:dark mint",
                    "lightgreen",
                    "xkcd:pale yellow",
                    "orange",
                    "salmon",
                ]
                # Fill entire height with red bar
                ax.bar(0.5, plot_h, color=pace_colors.pop())

                # Cover the red bar with rest of pace percentiles
                for p in pace_q.index.sort_values(ascending=False):
                    h = pace_q.loc[p].ceil("D").days
                    ax.bar(0.5, h, color=pace_colors.pop())

            # Filter data for this status
            df_status = wip_age_df[wip_age_df["current_status"] == status]

            # Plot items
            df_status["x"] = 0.5
            ax.scatter(df_status["x"], df_status["wip_age"].dt.ceil("D").dt.days)
            for _, row in df_status.reset_index().iterrows():
                ax.annotate(
                    row["item_identifier"],
                    (row["x"], row["wip_age"].ceil("D").days),
                )

            # return ct_q
            # Plot cycle time quantiles
            for p in ct_q.index:
                ct_q_value = ct_q[p].ceil("D").days
                ax.plot(
                    [0, 1], [ct_q_value, ct_q_value], "--", color="gray", label=str(p)
                )
                if index == 1:
                    ax.annotate(f"{ct_q_value}", (0, ct_q_value))
                if index == len(statuses):
                    ax.annotate(f"{int(p * 100)}%", (0.5, ct_q_value))

    def plot_throughput_run_chart(
        self,
        **kwargs: Unpack[FilterKwArgs],
    ) -> None:
        highlight_exclude_ranges = kwargs.get("exclude_ranges")
        if highlight_exclude_ranges:
            # We want to display all throughput values, even those that are excluded by the ranges
            kwargs.pop("exclude_ranges")
        df = self.df_throughput(**kwargs)

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.plot(df, marker="o")

        # Highlight ranges that are excluded
        if highlight_exclude_ranges:
            self._highlight_exclude_ranges(
                ax, highlight_exclude_ranges, "Excluded ranges"
            )
            ax.legend()

    def plot_monte_carlo_when_hist(
        self,
        percentiles: list[int] = [50, 85, 95],
        runs: int = 10000,
        item_count: int = 10,
        **kwargs: Unpack[FilterKwArgs],
    ) -> None:
        s = self.df_monte_carlo_when(runs=runs, item_count=item_count, **kwargs)

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.bar(s.index, s)

        # Find percentiles for the given *sorted* result
        pct = s.cumsum() / s.sum()
        p_dates = []
        for p in percentiles:
            # Get the first date that matches given percentage
            p_date = pct[pct >= p / 100].index[0]
            ax.plot([p_date, p_date], [0, s.max() * 1.1], label=f"{p}%")
            p_dates += [p_date]

        ax.set_xticks(p_dates)

        exclude_ranges = kwargs.get("exclude_ranges")
        if exclude_ranges:
            self._annotate_exclude_ranges(ax, exclude_ranges)
        ax.legend()

    def plot_monte_carlo_how_many_hist(
        self,
        target_date: datetime,
        runs: int = 10000,
        percentiles: list[int] = [50, 85, 95],
        **kwargs: Unpack[FilterKwArgs],
    ) -> None:
        s = self.df_monte_carlo_how_many(target_date=target_date, runs=runs, **kwargs)
        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.bar(s.index, s)

        # Find percentiles for the given *sorted* result
        pct = 1 - s.cumsum() / s.sum()
        how_manys = []
        for p in percentiles:
            # Get the *last* how_many that matches given percentage
            idx = pct[pct >= p / 100].index
            if idx.size == 0:
                how_many = 0
            else:
                how_many = idx[idx.size - 1]
            ax.plot(
                [how_many, how_many],
                [0, s.max() * 1.1],
                label=f"{p}%",
            )
            how_manys += [how_many]

        ax.set_xticks(how_manys)

        exclude_ranges = kwargs.get("exclude_ranges")
        if exclude_ranges:
            self._annotate_exclude_ranges(ax, exclude_ranges)
        ax.legend()

    def _validate_workflow_statuses(self) -> None:
        # Query the database for unique status values from Changelog
        stmt = select(ItemStatusChangelog.status).distinct()

        with Session(self._engine) as session:
            result = session.execute(stmt)
            unique_statuses = [row[0] for row in result]

        # Combine all workflow statuses into a single list
        workflow_statuses = set(
            self._workflow._not_started
            + self._workflow._in_progress
            + self._workflow._finished
        )

        # Find statuses that are in the database but not in the workflow
        invalid_statuses = set(unique_statuses) - workflow_statuses

        # Issue warnings for invalid statuses
        if invalid_statuses:
            print(
                f"Warning: The following statuses are in the database but not in the workflow: {invalid_statuses}"
            )

    def _get_mc_when_date(self, s: Series[int], percentile: int) -> datetime:
        # Find percentiles for the given *sorted* result
        pct = s.cumsum() / s.sum()
        # Get the first date that matches given percentage
        p_date: Timestamp = pct[pct >= percentile / 100].index[0]
        return p_date

    def _validate_exclude_ranges(
        self, exclude_ranges: List[DateTimeRange] | None
    ) -> None:
        if exclude_ranges is None:
            return
        for i in range(len(exclude_ranges)):
            for j in range(i + 1, len(exclude_ranges)):
                if exclude_ranges[i].is_intersection(exclude_ranges[j]) and not (
                    exclude_ranges[i].start_datetime == exclude_ranges[j].end_datetime
                    or exclude_ranges[i].end_datetime
                    == exclude_ranges[j].start_datetime
                ):
                    raise ValueError(
                        f"Exclude ranges {exclude_ranges[i]} and {exclude_ranges[j]} overlap."
                    )

    def _highlight_exclude_ranges(
        self, ax: Axes, ranges: list[DateTimeRange], label: str | None
    ) -> None:
        """Highlights exclude ranges on a plot."""

        for date_range in ranges:
            start_time = date_range.start_datetime
            end_time = date_range.end_datetime
            ax.axvspan(
                mdates.date2num(start_time),  # type: ignore
                mdates.date2num(end_time),  # type: ignore
                alpha=0.5,
                color="gray",
                label=label,
            )
            label = None  # Only show label for the first range

    def _annotate_exclude_ranges(self, ax: Axes, ranges: list[DateTimeRange]) -> None:
        text = "; ".join(
            [
                f"{r.start_datetime.strftime('%Y-%m-%d') if r.start_datetime else 'None'} - {r.end_datetime.strftime('%Y-%m-%d') if r.end_datetime else 'None'}"
                for r in ranges
            ]
        )

        ax.text(
            0.0,
            1.0,
            f"Ranges excluded: {text}",
            transform=ax.transAxes,
            verticalalignment="bottom",
            horizontalalignment="left",
        )

    def _df_exclude_ranges_from_durations(
        self,
        status_changelog_with_durations: DataFrame,
        **kwargs: Unpack[FilterKwArgs],
    ) -> DataFrame:
        exclude_ranges = kwargs.get("exclude_ranges")
        if exclude_ranges is None:
            return status_changelog_with_durations

        self._validate_exclude_ranges(exclude_ranges)

        # Avoid modifying the original DataFrame, make a copy
        df = status_changelog_with_durations.copy()

        for range in exclude_ranges:
            if range.start_datetime is None or range.end_datetime is None:
                raise ValueError(
                    "Both start_datetime and end_datetime must be provided in the DateTimeRange."
                )

            # Create boolean mask for overlapping ranges
            mask = (df["start_time"] <= range.end_datetime) & (
                df["end_time"] >= range.start_datetime
            )

            if mask.any():
                start = df.loc[mask, "start_time"]
                lower_bound = pd.Timestamp(range.start_datetime)
                overlap_start = start.where(start >= lower_bound, lower_bound)

                end = df.loc[mask, "end_time"]
                upper_bound = pd.Timestamp(range.end_datetime)
                overlap_end = end.where(end <= upper_bound, upper_bound)

                # Calculate overlap duration and subtract from original duration
                overlap_duration = overlap_end - overlap_start

                df.loc[mask, "duration"] -= overlap_duration

        return df
