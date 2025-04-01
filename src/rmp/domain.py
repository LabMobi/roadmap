from __future__ import annotations

from eventsourcing.domain import Aggregate
from uuid import UUID, uuid5, NAMESPACE_URL
from datetime import datetime
from eventsourcing.dispatch import singledispatchmethod
from typing import cast


class Item(Aggregate):
    class Event(Aggregate.Event):
        def apply(self, aggregate: Aggregate) -> None:
            cast(Item, aggregate).apply(self)

    class Created(Event, Aggregate.Created):
        url: str
        identifier: str
        summary: str
        status: str
        hierarchy_level: int
        rank: str
        sprints: list[int]
        milestones: list[int]

    class SummaryChanged(Aggregate.Event):
        summary: str

    class StatusChanged(Aggregate.Event):
        from_status: str
        to_status: str

    class HierarchyLevelChanged(Aggregate.Event):
        hierarchy_level: int

    class RankChanged(Aggregate.Event):
        rank: str

    class SprintAdded(Aggregate.Event):
        item_id: str
        sprint_identifier: int

    class SprintRemoved(Aggregate.Event):
        item_id: str
        sprint_identifier: int

    class MilestoneAdded(Aggregate.Event):
        item_id: str
        milestone_identifier: int

    class MilestoneRemoved(Aggregate.Event):
        item_id: str
        milestone_identifier: int

    @classmethod
    def create(
        cls,
        url: str,
        timestamp: datetime,
        identifier: str,
        summary: str,
        status: str,
        hierarchy_level: int,
        rank: str,
        sprints: list[str],
        milestones: list[str],
    ) -> Item:
        return cls._create(
            cls.Created,
            url=url,
            timestamp=timestamp,
            identifier=identifier,
            summary=summary,
            status=status,
            hierarchy_level=hierarchy_level,
            rank=rank,
            sprints=sprints,
            milestones=milestones,
        )

    @classmethod
    def create_id(cls, url: str) -> UUID:
        return uuid5(NAMESPACE_URL, url)

    def change_summary(self, timestamp: datetime, summary: str) -> None:
        self.trigger_event(self.SummaryChanged, timestamp=timestamp, summary=summary)

    def change_status(
        self, timestamp: datetime, from_status: str, to_status: str
    ) -> None:
        self.trigger_event(
            self.StatusChanged,
            timestamp=timestamp,
            from_status=from_status,
            to_status=to_status,
        )

    def change_hierarchy_level(self, timestamp: datetime, hierarchy_level: int) -> None:
        self.trigger_event(
            self.HierarchyLevelChanged,
            timestamp=timestamp,
            hierarchy_level=hierarchy_level,
        )

    def change_rank(self, timestamp: datetime, rank: str) -> None:
        self.trigger_event(self.RankChanged, timestamp=timestamp, rank=rank)

    def add_sprint(self, timestamp: datetime, sprint_identifier: int) -> None:
        self.trigger_event(
            self.SprintAdded,
            item_id=self.id,
            timestamp=timestamp,
            sprint_identifier=sprint_identifier,
        )

    def remove_sprint(self, timestamp: datetime, sprint_identifier: int) -> None:
        self.trigger_event(
            self.SprintRemoved,
            item_id=self.id,
            timestamp=timestamp,
            sprint_identifier=sprint_identifier,
        )

    def add_milestone(self, timestamp: datetime, milestone_identifier: int) -> None:
        self.trigger_event(
            self.MilestoneAdded,
            item_id=self.id,
            timestamp=timestamp,
            milestone_identifier=milestone_identifier,
        )

    def remove_milestone(self, timestamp: datetime, milestone_identifier: int) -> None:
        self.trigger_event(
            self.MilestoneRemoved,
            item_id=self.id,
            timestamp=timestamp,
            milestone_identifier=milestone_identifier,
        )

    @singledispatchmethod
    def apply(self, event: Event) -> None:
        """Applies event to aggregate."""

    @apply.register
    def _(self, event: Item.Created) -> None:
        self.url = event.url
        self.identifier = event.identifier
        self.summary = event.summary
        self.status = event.status
        self.hierarchy_level = event.hierarchy_level
        self.rank = event.rank
        self.sprints = event.sprints
        self.milestones = event.milestones

    @apply.register
    def _(self, event: Item.SummaryChanged) -> None:
        self.summary = event.summary

    @apply.register
    def _(self, event: Item.StatusChanged) -> None:
        self.status = event.to_status

    @apply.register
    def _(self, event: Item.HierarchyLevelChanged) -> None:
        self.hierarchy_level = event.hierarchy_level

    @apply.register
    def _(self, event: Item.RankChanged) -> None:
        self.rank = event.rank

    @apply.register
    def _(self, event: Item.SprintAdded) -> None:
        self.sprints.append(event.sprint_identifier)

    @apply.register
    def _(self, event: Item.SprintRemoved) -> None:
        if event.sprint_identifier in self.sprints:
            self.sprints.remove(event.sprint_identifier)

    @apply.register
    def _(self, event: Item.MilestoneAdded) -> None:
        self.milestones.append(event.milestone_identifier)

    @apply.register
    def _(self, event: Item.MilestoneRemoved) -> None:
        if event.milestone_identifier in self.milestones:
            self.milestones.remove(event.milestone_identifier)


class Sprint(Aggregate):
    class Event(Aggregate.Event):
        def apply(self, aggregate: Aggregate) -> None:
            cast(Sprint, aggregate).apply(self)

    class Created(Event, Aggregate.Created):
        url: str
        identifier: str
        state: str
        name: str
        order: int

    class OrderChanged(Aggregate.Event):
        order: int

    class StateChanged(Aggregate.Event):
        state: str

    class NameChanged(Aggregate.Event):
        name: str

    @classmethod
    def create(
        cls, url: str, identifier: str, state: str, name: str, order: int
    ) -> Sprint:
        return cls._create(
            cls.Created,
            url=url,
            identifier=identifier,
            state=state,
            name=name,
            order=order,
        )

    @classmethod
    def create_id(cls, url: str) -> UUID:
        return uuid5(NAMESPACE_URL, url)

    def update(self, state: str, name: str, order: int) -> None:
        if self.state != state:
            self.trigger_event(self.StateChanged, state=state)
        if self.order != order:
            self.trigger_event(self.OrderChanged, order=order)
        if self.name != name:
            self.trigger_event(self.NameChanged, name=name)

    @singledispatchmethod
    def apply(self, event: Event) -> None:
        """Applies event to aggregate."""

    @apply.register
    def _(self, event: Sprint.Created) -> None:
        self.url: str = event.url
        self.identifier: str = event.identifier
        self.state: str = event.state
        self.name: str = event.name
        self.order: int = event.order

    @apply.register
    def _(self, event: Sprint.StateChanged) -> None:
        self.state = event.state

    @apply.register
    def _(self, event: Sprint.OrderChanged) -> None:
        self.order = event.order

    @apply.register
    def _(self, event: Sprint.NameChanged) -> None:
        self.name = event.name


class Milestone(Aggregate):
    class Event(Aggregate.Event):
        def apply(self, aggregate: Aggregate) -> None:
            cast(Milestone, aggregate).apply(self)

    class Created(Event, Aggregate.Created):
        url: str
        identifier: str
        name: str
        description: str
        release_date: datetime
        released: bool

    class NameChanged(Aggregate.Event):
        name: str

    class DescriptionChanged(Aggregate.Event):
        description: str

    class ReleaseDateChanged(Aggregate.Event):
        release_date: datetime

    class ReleasedStateChanged(Aggregate.Event):
        released: bool

    @classmethod
    def create(
        cls,
        url: str,
        identifier: str,
        name: str,
        description: str,
        release_date: datetime,
        released: bool,
    ) -> Milestone:
        return cls._create(
            cls.Created,
            url=url,
            identifier=identifier,
            name=name,
            description=description,
            release_date=release_date,
            released=released,
        )

    @classmethod
    def create_id(cls, url: str) -> UUID:
        return uuid5(NAMESPACE_URL, url)

    def update(
        self, name: str, description: str, release_date: datetime, released: bool
    ) -> None:
        if self.name != name:
            self.trigger_event(self.NameChanged, name=name)
        if self.description != description:
            self.trigger_event(self.DescriptionChanged, description=description)
        if self.release_date != release_date:
            self.trigger_event(self.ReleaseDateChanged, release_date=release_date)
        if self.released != released:
            self.trigger_event(self.ReleasedStateChanged, released=released)

    @singledispatchmethod
    def apply(self, event: Event) -> None:
        """Applies event to aggregate."""

    @apply.register
    def _(self, event: Milestone.Created) -> None:
        self.url: str = event.url
        self.identifier: str = event.identifier
        self.name: str = event.name
        self.description: str = event.description
        self.release_date: datetime = event.release_date
        self.released: bool = event.released

    @apply.register
    def _(self, event: Milestone.NameChanged) -> None:
        self.name = event.name

    @apply.register
    def _(self, event: Milestone.DescriptionChanged) -> None:
        self.description = event.description

    @apply.register
    def _(self, event: Milestone.ReleaseDateChanged) -> None:
        self.release_date = event.release_date

    @apply.register
    def _(self, event: Milestone.ReleasedStateChanged) -> None:
        self.released = event.released
