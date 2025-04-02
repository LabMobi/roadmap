from __future__ import annotations  # https://github.com/pandas-dev/pandas/issues/54494
from typing import Any, TypedDict, Unpack
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Timestamp
from sqlalchemy import Engine, Select, and_, case, literal, or_
from sqlalchemy.orm import Session
from rmp.sql_model import (
    Item,
    ItemStatusChangelog,
    Milestone,
    Sprint,
    sprint_item_association,
    milestone_item_association,
)
from sqlalchemy import func, union_all, select
from datetime import datetime, timezone
from datetimerange import DateTimeRange
import matplotlib.ticker as tck

pd.options.mode.copy_on_write = True


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


class FilterCFDKwargs(TypedDict, total=False):
    include_not_started_statuses: bool


class FilterStatusChangelogKwargs(TypedDict, total=False):
    excludes_ranges: list[DateTimeRange] | None
    only_hierarchy_levels: set[int]


class MCSimulationKwargs(TypedDict):
    runs: int


class MCWhenKwargs(MCSimulationKwargs):
    item_count: int


class MCHowManyKwargs(MCSimulationKwargs):
    target_date: datetime


class FlowMetrics:
    def __init__(self, engine: Engine, workflow: Workflow) -> None:
        self._engine = engine
        self._workflow = workflow
        self._validate_workflow_statuses()

    def _select_status_changelog_distinct_arrival(self) -> Select[tuple[Any, ...]]:
        # Merge finished statuses into one since we are only interested of arrivals into
        # any of the finished statuses
        stmt = select(
            ItemStatusChangelog.item_id,
            ItemStatusChangelog.start_time,
            case(
                (
                    ItemStatusChangelog.status.in_(self._workflow._finished),
                    "/".join(self._workflow._finished),
                ),
                else_=ItemStatusChangelog.status,
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

    def _select_status_changelog_with_durations(
        self,
        excludes_ranges: list[DateTimeRange] | None = None,
        only_hierarchy_levels: set[int] = {0},
    ) -> Select[tuple[Any, ...]]:
        end_time = func.coalesce(ItemStatusChangelog.end_time, datetime.now()).label(
            "end_time"
        )
        duration = (
            func.strftime("%s", end_time)
            - func.strftime("%s", ItemStatusChangelog.start_time)
        ).label("duration")

        with_duration = select(
            ItemStatusChangelog.item_id,
            ItemStatusChangelog.status,
            ItemStatusChangelog.start_time,
            end_time,
            duration,
            # Item.identifier,
            # Item.hierarchy_level
        ).cte("_duration")

        if excludes_ranges is not None:
            # Ensure exclude_ranges do not overlap each other
            for i in range(len(excludes_ranges)):
                for j in range(i + 1, len(excludes_ranges)):
                    if excludes_ranges[i].is_intersection(excludes_ranges[j]) and not (
                        excludes_ranges[i].start_datetime
                        == excludes_ranges[j].end_datetime
                        or excludes_ranges[i].end_datetime
                        == excludes_ranges[j].start_datetime
                    ):
                        raise ValueError(
                            f"Exclude ranges {excludes_ranges[i]} and {excludes_ranges[j]} overlap."
                        )

            # Start correcting original durations with the exclude ranges
            # Create CTE first for all provided exclude ranges
            exclude_ranges_stmts = [
                select(
                    literal(range.start_datetime).label("exclude_start_time"),
                    literal(range.end_datetime).label("exclude_end_time"),
                )
                for range in excludes_ranges
            ]
            excludes = union_all(*exclude_ranges_stmts).cte(name="_exclude_ranges")

            # Find overlapping ranges and calculate correction
            # This may produce multiple changelog rows when more than one exclude
            # range is provided, therefore results need aggregating in next step
            overlap_sq = (
                select(
                    with_duration,
                    excludes.c.exclude_start_time,
                    excludes.c.exclude_end_time,
                    # Find overlap of original duration range and exclude range
                    func.max(
                        excludes.c.exclude_start_time, with_duration.c.start_time
                    ).label("correction_start_time"),
                    func.min(
                        excludes.c.exclude_end_time, with_duration.c.end_time
                    ).label("correction_end_time"),
                    # Calculate correction that will be used to modify the original duration
                    (
                        func.strftime(
                            "%s",
                            func.max(
                                excludes.c.exclude_start_time,
                                with_duration.c.start_time,
                            ),
                        )
                        - func.strftime(
                            "%s",
                            func.min(
                                excludes.c.exclude_end_time, with_duration.c.end_time
                            ),
                        )
                    ).label("duration_correction"),
                )
                .join(
                    excludes,
                    and_(
                        with_duration.c.start_time < excludes.c.exclude_end_time,
                        with_duration.c.end_time >= excludes.c.exclude_start_time,
                    ),
                    isouter=True,
                )
                .subquery("_overlap")
            )

            # Aggregate duration data with corrections
            with_corrected = select(
                overlap_sq.c.item_id,
                overlap_sq.c.status,
                overlap_sq.c.start_time,
                overlap_sq.c.end_time,
                (
                    func.max(overlap_sq.c.duration)
                    + func.coalesce(func.sum(overlap_sq.c.duration_correction), 0)
                ).label("duration"),
                func.max(overlap_sq.c.duration).label("duration_wo_exclude_ranges"),
            ).group_by(
                overlap_sq.c.item_id,
                overlap_sq.c.status,
                overlap_sq.c.start_time,
                overlap_sq.c.end_time,
            )
            with_duration = with_corrected.cte("_corrected_duration")

        # If status has some duration, consider this as effective status
        # this helps to figure out last active status
        with_mark_eff_status = select(
            with_duration,
            case((with_duration.c.duration > 0, True), else_=False).label(
                "effective_status"
            ),
        ).cte("_mark_effective_status")

        # Rank effective statuses over time with an intetion to find the last one
        rank_eff_status = (
            func.rank()
            .over(
                partition_by=[
                    with_mark_eff_status.c.item_id,
                    with_mark_eff_status.c.effective_status,
                ],
                order_by=with_mark_eff_status.c.start_time.desc(),
            )
            .label("rank_eff_status")
        )
        # The first ranked effective status is the last effective status
        last_eff_status = case(
            (
                and_(
                    rank_eff_status == 1,
                    with_mark_eff_status.c.effective_status,
                ),
                with_mark_eff_status.c.status,
            ),
            else_=None,
        ).label("last_eff_status")
        last_finished_time = case(
            (
                last_eff_status.in_(self._workflow._finished),
                with_mark_eff_status.c.start_time,
            )
        ).label("last_finished_time")

        with_last_eff_status = select(
            with_mark_eff_status,
            rank_eff_status,
            last_eff_status,
            last_finished_time,
        ).cte("_last_eff_status")

        # Propagate current status to all rows for item
        current_status = (
            func.max(with_last_eff_status.c.last_eff_status)
            .over(partition_by=with_last_eff_status.c.item_id)
            .label("current_status")
        )
        finished_time = (
            func.max(with_last_eff_status.c.last_finished_time)
            .over(partition_by=with_last_eff_status.c.item_id)
            .label("finished_time")
        )
        with_current_status = select(
            with_last_eff_status,
            current_status,
            finished_time,
        ).cte("_current_status")

        stmt = (
            select(with_current_status, Item.identifier.label("item_identifier"))
            .join(Item, Item.id == with_current_status.c.item_id)
            .where(Item.hierarchy_level.in_(only_hierarchy_levels))
        )

        return stmt

    def _select_status_durations(
        self, current_statuses: list[str]
    ) -> Select[tuple[Any, ...]]:
        stmt = self._select_status_changelog_with_durations()

        stmt = (
            select(
                stmt.c.item_id,
                stmt.c.status,
                func.sum(stmt.c.duration).label("duration"),
                func.max(stmt.c.finished_time).label("finished_time"),
                stmt.c.item_identifier,
            )
            .where(
                stmt.c.current_status.in_(current_statuses),
                stmt.c.status.in_(self._workflow._in_progress),
            )
            .group_by(stmt.c.item_id, stmt.c.status)
        )

        return stmt

    def _select_backlog_items(self) -> Select[tuple[Any, ...]]:
        stmt = (
            select(
                Item,
                Sprint.name.label("sprint_name"),
                Sprint.state.label("sprint_state"),
                Sprint.order.label("sprint_order"),
                Milestone.name.label("milestone_name"),
            )
            .select_from(Item)
            .outerjoin(sprint_item_association)
            .outerjoin(Sprint)
            .outerjoin(milestone_item_association)
            .outerjoin(Milestone)
            .order_by(Sprint.order.nulls_last(), Item.rank)
            .where(
                Item.hierarchy_level == 0,
                Item.status.in_(
                    self._workflow._not_started + self._workflow._in_progress
                ),
                or_(Sprint.state != "closed", Sprint.state.is_(None)),
            )
        )
        return stmt

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
        ct_df["cycle_time"] = (ct_df["cycle_time"] / 60 / 60 / 24).apply(np.ceil)
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
        ct_status_df["cycle_time"] = (ct_status_df["cycle_time"] / 60 / 60 / 24).apply(
            np.ceil
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
        wip_age_df["wip_age"] = (wip_age_df["wip_age"] / 60 / 60 / 24).apply(np.ceil)
        return wip_age_df

    def df_workflow_cum_arrivals(
        self, include_not_started_statuses: bool = False
    ) -> DataFrame:
        df = pd.read_sql(
            sql=self._select_status_changelog_distinct_arrival(), con=self._engine
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

    def df_status_durations(self, current_statuses: list[str]) -> DataFrame:
        return pd.read_sql(
            sql=self._select_status_durations(current_statuses), con=self._engine
        )

    def df_status_changelog_with_durations(
        self, **kwargs: Unpack[FilterStatusChangelogKwargs]
    ) -> DataFrame:
        return pd.read_sql(
            sql=self._select_status_changelog_with_durations(**kwargs), con=self._engine
        )

    def df_throughput(self) -> Series[int]:
        df = self.df_cycle_time(self.df_status_changelog_with_durations())
        return df["finished_time"].value_counts().resample("D").sum()

    def df_monte_carlo_when(
        self, runs: int = 10000, item_count: int = 10
    ) -> Series[int]:
        tp = self.df_throughput()
        start_date = pd.Timestamp.now(tz=timezone.utc)

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
            start_date + pd.DateOffset(days=int(idx + 1)) for idx in completion_indices
        ]

        # Convert to Series and count frequencies
        result = Series(completion_dates).value_counts().sort_index()

        return result

    def df_monte_carlo_how_many(
        self, target_date: datetime, runs: int = 10000
    ) -> Series[int]:
        tp = self.df_throughput()

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

    def df_backlog_items(
        self,
        mc_when: bool = False,
        mc_when_runs: int = 1000,
        mc_when_percentile: int = 85,
    ) -> DataFrame:
        df = pd.read_sql(sql=self._select_backlog_items(), con=self._engine)

        # Doing the grouping with DataFrame (not with SQL) as it allows to convert
        # multiple milestone names into list data type directly
        df = (
            df.groupby(
                [
                    "identifier",
                    "status",
                    "summary",
                    "sprint_name",
                    "sprint_order",
                    "rank",
                ],
                dropna=False,
            )["milestone_name"]
            .apply(lambda x: list(filter(None, x)) or None)
            .reset_index()
        )
        df = df.sort_values(
            by=["sprint_order", "rank"], na_position="last", ignore_index=True
        )
        df = df.rename(columns={"milestone_name": "milestones"})
        df = df.drop(columns=["sprint_order", "rank"])

        # Run Monte Carlo "when" simulation for each backlog item row
        if mc_when:
            df["mc_when"] = df.index.map(
                lambda x: self._get_mc_when_date(
                    self.df_monte_carlo_when(mc_when_runs, item_count=x + 1),
                    percentile=mc_when_percentile,
                )
            )
            df["mc_when"] = df["mc_when"].dt.normalize()  # Keep only day precision

        return df

    def plot_cfd(self, **kwargs: Unpack[FilterCFDKwargs]) -> None:
        df = self.df_workflow_cum_arrivals(**kwargs)
        df.plot.area(stacked=False, figsize=(20, 10), alpha=1)

    def plot_cycle_time_scatter(
        self,
        percentiles: list[int] = [50, 70, 85, 95],
        annotate_item_ids: bool = True,
        **kwargs: Unpack[FilterStatusChangelogKwargs],
    ) -> None:
        df = self.df_status_changelog_with_durations(**kwargs)
        ct_df = self.df_cycle_time(df)

        # Calculate quantile values for cycle time, useful for forecasting single item cycle time
        ct_q = ct_df["cycle_time"].quantile([p / 100 for p in percentiles])

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()

        ax.scatter(
            x=ct_df["finished_time"], y=ct_df["cycle_time"], label="Cycle Time (days)"
        )

        if annotate_item_ids:
            for _, row in ct_df.iterrows():
                ax.annotate(
                    row["item_identifier"], (row["finished_time"], row["cycle_time"])
                )

        # Plot cycle time quantiles
        min_finished = ct_df["finished_time"].min()
        max_finished = ct_df["finished_time"].max()
        for p in ct_q.index:
            ax.plot((min_finished, max_finished), (ct_q[p], ct_q[p]), "--")
            ax.annotate(f"{int(ct_q[p])}", (min_finished, ct_q[p]))
            ax.annotate(f"{int(p * 100)}%", (max_finished, ct_q[p]))

    def plot_cycle_time_histogram(
        self,
        percentiles: list[int] = [50, 85, 95],
        **kwargs: Unpack[FilterStatusChangelogKwargs],
    ) -> None:
        df = self.df_status_changelog_with_durations(**kwargs)
        ct_df = self.df_cycle_time(df)

        # Calculate quantile values for cycle time, useful for forecasting single item cycle time
        ct_q = ct_df["cycle_time"].quantile([p / 100 for p in percentiles])

        cycle_times = ct_df["cycle_time"]
        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.hist(cycle_times, bins=int(cycle_times.max()))
        ax.yaxis.set_major_locator(tck.MultipleLocator())

        for p in ct_q.index:
            ax.plot(
                (ct_q[p], ct_q[p]), [0, cycle_times.value_counts().max()], label=str(p)
            )

        ax.legend()

    def plot_aging_wip(
        self,
        percentiles: list[int] = [50, 70, 85, 95],
        **kwargs: Unpack[FilterStatusChangelogKwargs],
    ) -> None:
        df = self.df_status_changelog_with_durations(**kwargs)

        # Item cycle time
        ct_df = self.df_cycle_time(df)

        # Item status cycle time
        ct_status_df = self.df_status_cycle_time(df)

        # Item wip age
        wip_age_df = self.df_wip_age(df)

        # Calculate quantile values for cycle time, useful for forecasting single item cycle time
        ct_q = ct_df["cycle_time"].quantile([p / 100 for p in percentiles])

        # The plot will be generated for each "in progress" and "finished" status
        statuses = self._workflow._in_progress + self._workflow._finished

        # Calculate the plot max height
        plot_h = max(wip_age_df["wip_age"].max(), ct_q.max()) * 1.1

        fig = plt.figure(figsize=(16, 9))
        index = 0
        include_statuses = []
        for status in statuses:
            index += 1

            # For each status, add subplot
            ax = fig.add_subplot(1, len(statuses), index)
            ax.set_xlabel(status)
            ax.set_xticks([])
            ax.set_ylim(bottom=0, top=plot_h)
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

                # status_ct = self.items_cycle_time(include_statuses=include_statuses)
                pace_q = cur_status_ct_df["cycle_time"].quantile(
                    [p / 100 for p in percentiles]
                )
                # col = f'ct_{self._workflow.to_snake_case(status)}'
                # pace_q = df[col].quantile([p / 100 for p in percentiles])

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
                    ax.bar(0.5, pace_q.loc[p], color=pace_colors.pop())

            # Filter data for this status
            df_status = wip_age_df[wip_age_df["current_status"] == status]

            # Plot items
            df_status["x"] = 0.5
            ax.scatter(df_status["x"], df_status["wip_age"])
            for _, row in df_status.reset_index().iterrows():
                ax.annotate(row["item_identifier"], (row["x"], row["wip_age"]))

            # Plot cycle time quantiles
            for p in ct_q.index:
                ax.plot([0, 1], [ct_q[p], ct_q[p]], "--", color="gray", label=str(p))
                if index == 1:
                    ax.annotate(ct_q[p], (0, ct_q[p]))
                if index == len(statuses):
                    ax.annotate(f"{int(p * 100)}%", (0.5, ct_q[p]))

    def plot_throughput_run_chart(self) -> None:
        df = self.df_throughput()

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.plot(df, marker="o")

    def plot_monte_carlo_when_hist(
        self,
        percentiles: list[int] = [50, 85, 95],
        **kwargs: Unpack[MCWhenKwargs],
    ) -> None:
        s = self.df_monte_carlo_when(**kwargs)

        fig = plt.figure(figsize=(16, 9))
        ax = fig.subplots()
        ax.bar(s.index, s)

        # Find percentiles for the given *sorted* result
        pct = s.cumsum() / s.sum()
        p_dates = []
        for p in percentiles:
            # Get the first date that matches given percentage
            p_date = pct[pct >= p / 100].index[0]
            ax.plot(
                [p_date, p_date], [0, s.max() * 1.1], label=str(p) + "th percentile"
            )
            p_dates += [p_date]

        ax.set_xticks(p_dates)
        ax.legend()

    def plot_monte_carlo_how_many_hist(
        self,
        percentiles: list[int] = [50, 85, 95],
        **kwargs: Unpack[MCHowManyKwargs],
    ) -> None:
        s = self.df_monte_carlo_how_many(**kwargs)
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
                label=str(p) + "th percentile",
            )
            how_manys += [how_many]

        ax.set_xticks(how_manys)
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
