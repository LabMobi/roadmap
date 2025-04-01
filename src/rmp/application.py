import typing
from uuid import UUID
from eventsourcing.application import Application
from eventsourcing.system import ProcessApplication
from eventsourcing.dispatch import singledispatchmethod
from eventsourcing_sqlalchemy.recorders import SQLAlchemyProcessRecorder
from sqlalchemy import select, update
from rmp.domain import Item, Sprint, Milestone
from datetime import datetime
from rmp.sql_model import (
    ItemStatusChangelog,
    Item as SqlItem,
    Sprint as SqlSprint,
    Milestone as SqlMilestone,
)
from eventsourcing.application import AggregateNotFoundError
from eventsourcing_sqlalchemy.datastore import Transaction


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
    ):
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
        )
        self.save(item)
        return item.id

    def get_item_by_id(self, id: UUID):
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
        }

    def get_item(self, url: str):
        return self.get_item_by_id(Item.create_id(url))

    def change_summary(self, url: str, timestamp: datetime, summary: str):
        item: Item = self.repository.get(Item.create_id(url))
        item.change_summary(timestamp, summary)
        self.save(item)

    def change_status(self, url: str, timestamp: datetime, to_status: str):
        item: Item = self.repository.get(Item.create_id(url))
        item.change_status(timestamp, item.status, to_status)
        self.save(item)

    def change_hierarchy_level(
        self, url: str, timestamp: datetime, hierarchy_level: int
    ):
        item: Item = self.repository.get(Item.create_id(url))
        item.change_hierarchy_level(timestamp, hierarchy_level)
        self.save(item)

    def change_rank(self, url: str, timestamp: datetime, rank: str):
        item: Item = self.repository.get(Item.create_id(url))
        item.change_rank(timestamp, rank)
        self.save(item)

    def add_sprint(self, url: str, timestamp: datetime, sprint_identifier: int):
        item: Item = self.repository.get(Item.create_id(url))
        item.add_sprint(timestamp, sprint_identifier)
        self.save(item)

    def remove_sprint(self, url: str, timestamp: datetime, sprint_identifier: int):
        item: Item = self.repository.get(Item.create_id(url))
        item.remove_sprint(timestamp, sprint_identifier)
        self.save(item)

    def add_milestone(self, url: str, timestamp: datetime, milestone_identifier: int):
        item: Item = self.repository.get(Item.create_id(url))
        item.add_milestone(timestamp, milestone_identifier)
        self.save(item)

    def remove_milestone(
        self, url: str, timestamp: datetime, milestone_identifier: int
    ):
        item: Item = self.repository.get(Item.create_id(url))
        item.remove_milestone(timestamp, milestone_identifier)
        self.save(item)


class SprintDataSourceApplication(Application):
    def create_sprint(
        self, url: str, identifier: str, state: str, name: str, order: int
    ):
        sprint = Sprint.create(url, identifier, state, name, order)
        self.save(sprint)
        return sprint.id

    def create_or_update_sprint(
        self, url: str, identifier: str, state: str, name: str, order: int
    ):
        try:
            sprint: Sprint = self.repository.get(Sprint.create_id(url))
        except AggregateNotFoundError:
            return self.create_sprint(
                url=url, identifier=identifier, state=state, name=name, order=order
            )

        sprint.update(state=state, name=name, order=order)
        self.save(sprint)
        return sprint.id


class MilestoneDataSourceApplication(Application):
    def create_milestone(
        self,
        url: str,
        identifier: str,
        name: str,
        description: str,
        release_date: datetime,
        released: bool,
    ):
        milestone = Milestone.create(
            url, identifier, name, description, release_date, released
        )
        self.save(milestone)
        return milestone.id

    def create_or_update_milestone(
        self,
        url: str,
        identifier: str,
        name: str,
        description: str,
        release_date: datetime,
        released: bool,
    ):
        try:
            milestone: Milestone = self.repository.get(Milestone.create_id(url))
        except AggregateNotFoundError:
            return self.create_milestone(
                url=url,
                identifier=identifier,
                name=name,
                description=description,
                release_date=release_date,
                released=released,
            )

        milestone.update(
            name=name,
            description=description,
            release_date=release_date,
            released=released,
        )
        self.save(milestone)
        return milestone.id


class AnalyticsDbApplication(ProcessApplication):
    def _get_transaction(self) -> Transaction:
        return typing.cast(SQLAlchemyProcessRecorder, self.recorder).transaction()

    @singledispatchmethod
    def policy(self, domain_event, processing_event):
        pass

    @policy.register(Item.Created)
    def _(self, domain_event: Item.Created, process_event):
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
            )
            session.add(item)

            for sprint_identifier in domain_event.sprints:
                sprint = session.execute(
                    select(SqlSprint).where(SqlSprint.identifier == sprint_identifier)
                ).scalar_one_or_none()
                if sprint is not None:
                    item.sprints.append(sprint)

            for milestone_identifier in domain_event.milestones:
                milestone = session.execute(
                    select(SqlMilestone).where(
                        SqlMilestone.identifier == milestone_identifier
                    )
                ).scalar_one_or_none()
                if milestone is not None:
                    item.milestones.append(milestone)

            item_status_changelog = ItemStatusChangelog(
                item_id=domain_event.originator_id,
                status=domain_event.status,
                start_time=domain_event.timestamp,
                end_time=None,
            )
            session.add(item_status_changelog)

            session.commit()

    @policy.register(Item.SummaryChanged)
    def _(self, domain_event: Item.SummaryChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(summary=domain_event.summary)
            )
            session.commit()

    @policy.register(Item.HierarchyLevelChanged)
    def _(self, domain_event: Item.HierarchyLevelChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(hierarchy_level=domain_event.hierarchy_level)
            )
            session.commit()

    @policy.register(Item.RankChanged)
    def _(self, domain_event: Item.RankChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlItem)
                .where(SqlItem.id == domain_event.originator_id)
                .values(rank=domain_event.rank)
            )
            session.commit()

    @policy.register(Item.StatusChanged)
    def _(self, domain_event: Item.StatusChanged, process_event):
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

    @policy.register(Item.SprintAdded)
    def _(self, domain_event: Item.SprintAdded, process_event):
        with self._get_transaction() as session:
            sprint = session.execute(
                select(SqlSprint).where(
                    SqlSprint.identifier == domain_event.sprint_identifier
                )
            ).scalar_one_or_none()

            # Sprint may not exist (any more), but item still has the event in its event history
            if sprint is None:
                return

            item = session.get_one(SqlItem, domain_event.item_id)
            item.sprints.append(sprint)
            session.commit()

    @policy.register(Item.SprintRemoved)
    def _(self, domain_event: Item.SprintRemoved, process_event):
        with self._get_transaction() as session:
            sprint = session.execute(
                select(SqlSprint).where(
                    SqlSprint.identifier == domain_event.sprint_identifier
                )
            ).scalar_one_or_none()

            # Sprint may not exist (any more), but item still has the event in its event history
            if sprint is None:
                return

            item = session.get_one(SqlItem, domain_event.item_id)
            item.sprints.remove(sprint)
            session.commit()

    @policy.register(Item.MilestoneAdded)
    def _(self, domain_event: Item.MilestoneAdded, process_event):
        with self._get_transaction() as session:
            milestone = session.execute(
                select(SqlMilestone).where(
                    SqlMilestone.identifier == domain_event.milestone_identifier
                )
            ).scalar_one_or_none()

            # Milestone may not exist (any more), but item still has the event in its event history
            if milestone is None:
                return

            item = session.get_one(SqlItem, domain_event.item_id)
            item.milestones.append(milestone)
            session.commit()

    @policy.register(Item.MilestoneRemoved)
    def _(self, domain_event: Item.MilestoneRemoved, process_event):
        with self._get_transaction() as session:
            milestone = session.execute(
                select(SqlMilestone).where(
                    SqlMilestone.identifier == domain_event.milestone_identifier
                )
            ).scalar_one_or_none()

            # Milestone may not exist (any more), but item still has the event in its event history
            if milestone is None:
                return

            item = session.get_one(SqlItem, domain_event.item_id)
            item.milestones.remove(milestone)
            session.commit()

    @policy.register(Sprint.Created)
    def _(self, domain_event: Sprint.Created, process_event):
        with self._get_transaction() as session:
            sprint = SqlSprint(
                id=domain_event.originator_id,
                identifier=domain_event.identifier,
                url=domain_event.url,
                state=domain_event.state,
                name=domain_event.name,
                order=domain_event.order,
            )
            session.add(sprint)
            session.commit()

    @policy.register(Sprint.StateChanged)
    def _(self, domain_event: Sprint.StateChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    state=domain_event.state,
                )
            )
            session.commit()

    @policy.register(Sprint.NameChanged)
    def _(self, domain_event: Sprint.NameChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    name=domain_event.name,
                )
            )
            session.commit()

    @policy.register(Sprint.OrderChanged)
    def _(self, domain_event: Sprint.OrderChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlSprint)
                .where(SqlSprint.id == domain_event.originator_id)
                .values(
                    order=domain_event.order,
                )
            )
            session.commit()

    @policy.register(Milestone.Created)
    def _(self, domain_event: Milestone.Created, process_event):
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

    @policy.register(Milestone.NameChanged)
    def _(self, domain_event: Milestone.NameChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    name=domain_event.name,
                )
            )
            session.commit()

    @policy.register(Milestone.DescriptionChanged)
    def _(self, domain_event: Milestone.DescriptionChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    description=domain_event.description,
                )
            )
            session.commit()

    @policy.register(Milestone.ReleaseDateChanged)
    def _(self, domain_event: Milestone.ReleaseDateChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    release_date=domain_event.release_date,
                )
            )
            session.commit()

    @policy.register(Milestone.ReleasedStateChanged)
    def _(self, domain_event: Milestone.ReleasedStateChanged, process_event):
        with self._get_transaction() as session:
            session.execute(
                update(SqlMilestone)
                .where(SqlMilestone.id == domain_event.originator_id)
                .values(
                    released=domain_event.released,
                )
            )
            session.commit()
