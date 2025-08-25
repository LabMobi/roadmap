import typing

from typing import Any
from uuid import UUID
from eventsourcing.application import Application
from eventsourcing.system import ProcessApplication
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing_sqlalchemy.recorders import SQLAlchemyProcessRecorder
from sqlalchemy import select, update
from rmp.domain import Item, Sprint, Milestone
from datetime import datetime
from rmp.sql_model import (
    ItemRankChangelog,
    ItemStatusChangelog,
    Item as SqlItem,
    Sprint as SqlSprint,
    SprintItem as SqlSprintItem,
    Milestone as SqlMilestone,
    MilestoneItem as SqlMilestoneItem,
)
from eventsourcing.application import AggregateNotFoundError, ProcessingEvent
from eventsourcing_sqlalchemy.datastore import Transaction
from eventsourcing.domain import DomainEventProtocol


class ItemDataSourceApplication(Application):
    def create_item(
        self,
        url: str,
        timestamp: datetime,
        identifier: str,
        summary: str,
        status: str,
        hierarchy_level: int,
        rank: str,
        sprints: list[str],
        milestones: list[str],
        item_type: str | None = None,
    ) -> UUID:
        item = Item.create(
            url,
            timestamp,
            identifier,
            summary,
            status,
            hierarchy_level,
            rank,
            sprints,
            milestones,
            item_type,
        )
        self.save(item)
        return item.id

    def get_item_by_id(self, id: UUID) -> dict[str, Any] | None:
        try:
            item: Item = self.repository.get(id)
            return {
                "url": item.url,
                "identifier": item.identifier,
                "summary": item.summary,
                "status": item.status,
                "hierarchy_level": item.hierarchy_level,
                "rank": item.rank,
                "sprints": item.sprints,
                "milestones": item.milestones,
                "changelog_tracking_id": item.changelog_tracking_id,
            }
        except AggregateNotFoundError:
            return None

    def get_item(self, url: str) -> dict[str, Any] | None:
        return self.get_item_by_id(Item.create_id(url))

    def change_summary(
        self,
        url: str,
        timestamp: datetime,
        summary: str,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.change_summary(
            timestamp, summary, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def change_status(
        self,
        url: str,
        timestamp: datetime,
        to_status: str,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.change_status(
            timestamp,
            item.status,
            to_status,
            changelog_tracking_id=changelog_tracking_id,
        )
        self.save(item)

    def change_hierarchy_level(
        self,
        url: str,
        timestamp: datetime,
        hierarchy_level: int,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.change_hierarchy_level(
            timestamp, hierarchy_level, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def change_rank(
        self,
        url: str,
        timestamp: datetime,
        rank: str,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.change_rank(timestamp, rank, changelog_tracking_id=changelog_tracking_id)
        self.save(item)

    def change_type(
        self,
        url: str,
        timestamp: datetime,
        item_type: str | None,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.change_type(
            timestamp, item_type, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def add_sprint(
        self,
        url: str,
        timestamp: datetime,
        sprint_identifier: int,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.add_sprint(
            timestamp, sprint_identifier, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def remove_sprint(
        self,
        url: str,
        timestamp: datetime,
        sprint_identifier: int,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.remove_sprint(
            timestamp, sprint_identifier, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def add_milestone(
        self,
        url: str,
        timestamp: datetime,
        milestone_identifier: int,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.add_milestone(
            timestamp, milestone_identifier, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def remove_milestone(
        self,
        url: str,
        timestamp: datetime,
        milestone_identifier: int,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.remove_milestone(
            timestamp, milestone_identifier, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)

    def set_changelog_tracking_id(
        self,
        url: str,
        timestamp: datetime,
        changelog_tracking_id: int | None = None,
    ) -> None:
        item: Item = self.repository.get(Item.create_id(url))
        item.set_changelog_tracking_id(
            timestamp, changelog_tracking_id=changelog_tracking_id
        )
        self.save(item)


class SprintDataSourceApplication(Application):
    def create_sprint(
        self,
        url: str,
        timestamp: datetime,
        identifier: str,
        state: str,
        name: str,
        order: int,
        start_time: datetime,
        end_time: datetime,
        complete_time: datetime | None = None,
    ) -> UUID:
        sprint = Sprint.create(
            url,
            timestamp,
            identifier,
            state,
            name,
            order,
            start_time,
            end_time,
            complete_time,
        )
        self.save(sprint)
        return sprint.id

    def create_or_update_sprint(
        self,
        url: str,
        timestamp: datetime,
        identifier: str,
        state: str,
        name: str,
        order: int,
        start_time: datetime,
        end_time: datetime,
        complete_time: datetime | None = None,
    ) -> UUID:
        try:
            sprint: Sprint = self.repository.get(Sprint.create_id(url))
        except AggregateNotFoundError:
            return self.create_sprint(
                url=url,
                timestamp=timestamp,
                identifier=identifier,
                state=state,
                name=name,
                order=order,
                start_time=start_time,
                end_time=end_time,
                complete_time=complete_time,
            )

        sprint.update(
            state=state,
            name=name,
            order=order,
            start_time=start_time,
            end_time=end_time,
            complete_time=complete_time,
        )
        self.save(sprint)
        return sprint.id


class MilestoneDataSourceApplication(Application):
    def create_milestone(
        self,
        url: str,
        identifier: str,
        name: str,
        released: bool,
        description: str | None = None,
        release_date: datetime | None = None,
    ) -> UUID:
        milestone = Milestone.create(
            url, identifier, name, released, description, release_date
        )
        self.save(milestone)
        return milestone.id

    def create_or_update_milestone(
        self,
        url: str,
        identifier: str,
        name: str,
        released: bool,
        description: str | None = None,
        release_date: datetime | None = None,
    ) -> UUID:
        try:
            milestone: Milestone = self.repository.get(Milestone.create_id(url))
        except AggregateNotFoundError:
            return self.create_milestone(
                url=url,
                identifier=identifier,
                name=name,
                description=description,
                released=released,
                release_date=release_date,
            )

        milestone.update(
            name=name,
            released=released,
            release_date=release_date,
            description=description,
        )
        self.save(milestone)
        return milestone.id


class AnalyticsDbApplication(ProcessApplication):
    def _get_transaction(self) -> Transaction:
        return typing.cast(SQLAlchemyProcessRecorder, self.recorder).transaction()

    # @singledispatchmethod  # type: ignore[override]
    def policy(
        self, domain_event: DomainEventProtocol, processing_event: ProcessingEvent
    ) -> None:
        self._policy(domain_event, processing_event)

    @singledispatchmethod
    def _policy(
        self, domain_event: DomainEventProtocol, processing_event: ProcessingEvent
    ) -> None:
        """Handles different event types"""

    @_policy.register
    def _(self, domain_event: Item.Created, process_event: ProcessingEvent) -> None:
        with self._get_transaction() as session:
            item = SqlItem(
                id=domain_event.originator_id,
                identifier=domain_event.identifier,
                url=domain_event.url,
                status=domain_event.status,
                hierarchy_level=domain_event.hierarchy_level,
                rank=domain_event.rank,
                summary=domain_event.summary,
                created_time=domain_event.timestamp,
                item_type=domain_event.item_type,
            )
            session.add(item)

            for sprint_identifier in domain_event.sprints:
                sprint = session.execute(
                    select(SqlSprint).where(SqlSprint.identifier == sprint_identifier)
                ).scalar_one_or_none()
                if sprint is not None:
                    sprint_item = SqlSprintItem(
                        sprint_id=sprint.id,
                        item_id=domain_event.originator_id,
                        add_time=domain_event.timestamp,
                    )
                    session.add(sprint_item)

            for milestone_identifier in domain_event.milestones:
                milestone = session.execute(
                    select(SqlMilestone).where(
                        SqlMilestone.identifier == milestone_identifier
                    )
                ).scalar_one_or_none()
                if milestone is not None:
                    milestone_item = SqlMilestoneItem(
                        milestone_id=milestone.id,
                        item_id=domain_event.originator_id,
                        add_time=domain_event.timestamp,
                    )
                    session.add(milestone_item)

            item_status_changelog = ItemStatusChangelog(
                item_id=domain_event.originator_id,
                status=domain_event.status,
                start_time=domain_event.timestamp,
                end_time=None,
            )
            session.add(item_status_changelog)

            item_rank_changelog = ItemRankChangelog(
                item_id=domain_event.originator_id,
                rank=domain_event.rank,
                start_time=domain_event.timestamp,
                end_time=None,
            )
            session.add(item_rank_changelog)

            session.commit()

    @_policy.register
    def _(
        self, domain_event: Item.SummaryChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(summary=domain_event.summary)
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Item.HierarchyLevelChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(hierarchy_level=domain_event.hierarchy_level)
            )
            session.commit()

    @_policy.register
    def _(self, domain_event: Item.RankChanged, process_event: ProcessingEvent) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(rank=domain_event.rank)
            )

            session.execute(
                update(ItemRankChangelog)
                .where(
                    ItemRankChangelog.item_id == domain_event.originator_id,
                    ItemRankChangelog.end_time.is_(None),
                )
                .values(end_time=domain_event.timestamp)
            )

            item_rank_changelog = ItemRankChangelog(
                item_id=domain_event.originator_id,
                rank=domain_event.rank,
                start_time=domain_event.timestamp,
                end_time=None,
            )
            session.add(item_rank_changelog)

            session.commit()

    @_policy.register
    def _(
        self, domain_event: Item.StatusChanged, process_event: ProcessingEvent
    ) -> None:
        to_status = domain_event.to_status
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(status=domain_event.to_status)
            )

            changelog = session.execute(
                select(ItemStatusChangelog).where(
                    ItemStatusChangelog.item_id == domain_event.originator_id,
                    ItemStatusChangelog.end_time.is_(None),
                )
            ).scalar_one()
            changelog.end_time = domain_event.timestamp

            # Add row for new status change
            changelog = ItemStatusChangelog(
                item_id=domain_event.originator_id,
                status=to_status,
                start_time=domain_event.timestamp,
            )
            session.add(changelog)
            session.commit()

    @_policy.register
    def _(self, domain_event: Item.TypeChanged, process_event: ProcessingEvent) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(item_type=domain_event.item_type)
            )
            session.commit()

    @_policy.register
    def _(self, domain_event: Item.SprintAdded, process_event: ProcessingEvent) -> None:
        with self._get_transaction() as session:
            sprint = session.execute(
                select(SqlSprint).where(
                    SqlSprint.identifier == domain_event.sprint_identifier
                )
            ).scalar_one_or_none()

            # Sprint may not exist in case they are not loaded
            if sprint is None:
                return

            # Get the existing active (the one without remove_time) association
            existing_association = session.execute(
                select(SqlSprintItem).where(
                    SqlSprintItem.sprint_id.is_(sprint.id),
                    SqlSprintItem.item_id.is_(domain_event.item_id),
                    SqlSprintItem.remove_time.is_(None),
                )
            ).scalar_one_or_none()
            if existing_association is not None:
                raise ValueError(
                    f"Item {domain_event.item_id} already has an active association with sprint {sprint.id}."
                )

            # Create a new association for the sprint and item
            new_association = SqlSprintItem(
                sprint_id=sprint.id,
                item_id=domain_event.item_id,
                add_time=domain_event.timestamp,
            )
            session.add(new_association)
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Item.SprintRemoved, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            sprint = session.execute(
                select(SqlSprint).where(
                    SqlSprint.identifier == domain_event.sprint_identifier
                )
            ).scalar_one_or_none()

            # Sprint may not exist in case they are not loaded
            if sprint is None:
                return

            # Get the existing active association
            existing_association = session.execute(
                select(SqlSprintItem).where(
                    SqlSprintItem.sprint_id.is_(sprint.id),
                    SqlSprintItem.item_id.is_(domain_event.item_id),
                    SqlSprintItem.remove_time.is_(None),
                )
            ).scalar_one_or_none()

            if existing_association is None:
                raise ValueError(
                    f"Item {domain_event.item_id} does not have an active association with sprint {sprint.id}."
                )

            existing_association.remove_time = domain_event.timestamp
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Item.MilestoneAdded, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            milestone = session.execute(
                select(SqlMilestone).where(
                    SqlMilestone.identifier == domain_event.milestone_identifier
                )
            ).scalar_one_or_none()

            if milestone is None:
                return

            # Get the existing active (the one without remove_time) association for
            # given item-milestone pair
            existing_association = session.execute(
                select(SqlMilestoneItem).where(
                    SqlMilestoneItem.milestone_id.is_(milestone.id),
                    SqlMilestoneItem.item_id.is_(domain_event.item_id),
                    SqlMilestoneItem.remove_time.is_(None),
                )
            ).scalar_one_or_none()
            if existing_association is not None:
                raise ValueError(
                    f"Item {domain_event.item_id} already has an active association with milestone {milestone.id}."
                )
                # latest_association.remove_time = domain_event.timestamp

            # Create a new association for the milestone and item
            new_association = SqlMilestoneItem(
                milestone_id=milestone.id,
                item_id=domain_event.item_id,
                add_time=domain_event.timestamp,
            )
            session.add(new_association)

            session.commit()

    @_policy.register
    def _(
        self, domain_event: Item.MilestoneRemoved, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            milestone = session.execute(
                select(SqlMilestone).where(
                    SqlMilestone.identifier == domain_event.milestone_identifier
                )
            ).scalar_one_or_none()

            if milestone is None:
                return

            # Get the existing active (the one without remove_time) association for
            # given item-milestone pair
            existing_association = session.execute(
                select(SqlMilestoneItem).where(
                    SqlMilestoneItem.milestone_id.is_(milestone.id),
                    SqlMilestoneItem.item_id.is_(domain_event.item_id),
                    SqlMilestoneItem.remove_time.is_(None),
                )
            ).scalar_one_or_none()

            if existing_association is None:
                raise ValueError(
                    f"Item {domain_event.item_id} does not have an active association with milestone {milestone.id}."
                )

            existing_association.remove_time = domain_event.timestamp

            session.commit()

    @_policy.register
    def _(self, domain_event: Sprint.Created, process_event: ProcessingEvent) -> None:
        with self._get_transaction() as session:
            sprint = SqlSprint(
                id=domain_event.originator_id,
                create_time=domain_event.timestamp,
                identifier=domain_event.identifier,
                url=domain_event.url,
                state=domain_event.state,
                name=domain_event.name,
                order=domain_event.order,
                start_time=domain_event.start_time,
                end_time=domain_event.end_time,
                complete_time=domain_event.complete_time,
            )
            session.add(sprint)
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Sprint.StateChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    state=domain_event.state,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Sprint.NameChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    name=domain_event.name,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Sprint.OrderChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    order=domain_event.order,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Sprint.StartTimeChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    start_time=domain_event.start_time,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Sprint.EndTimeChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    end_time=domain_event.end_time,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Sprint.CompleteTimeChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    complete_time=domain_event.complete_time,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Milestone.Created, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            milestone = SqlMilestone(
                id=domain_event.originator_id,
                identifier=domain_event.identifier,
                url=domain_event.url,
                name=domain_event.name,
                description=domain_event.description,
                release_date=domain_event.release_date,
                released=domain_event.released,
            )
            session.add(milestone)
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Milestone.NameChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    name=domain_event.name,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Milestone.DescriptionChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    description=domain_event.description,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self, domain_event: Milestone.ReleaseDateChanged, process_event: ProcessingEvent
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    release_date=domain_event.release_date,
                )
            )
            session.commit()

    @_policy.register
    def _(
        self,
        domain_event: Milestone.ReleasedStateChanged,
        process_event: ProcessingEvent,
    ) -> None:
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    released=domain_event.released,
                )
            )
            session.commit()
