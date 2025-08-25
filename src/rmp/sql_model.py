import datetime
from uuid import UUID
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, ForeignKey, UniqueConstraint


class Base(DeclarativeBase):
    pass


class Config(Base):
    __tablename__ = "config"

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column()
    value: Mapped[str] = mapped_column()
    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))

    def __repr__(self) -> str:
        return f"Config(id={self.id!r}, key={self.key!r}, value={self.value!r})"


class Option(Base):
    __tablename__ = "option"

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column()
    value: Mapped[str] = mapped_column()
    data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))

    # Define the relationship to DataSource
    data_source: Mapped["DataSource"] = relationship(
        "DataSource", back_populates="options"
    )

    def __repr__(self) -> str:
        return f"Option(id={self.id!r}, key={self.key!r}, value={self.value!r})"


class DataSource(Base):
    __tablename__ = "data_source"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(16), unique=True)
    connector_module: Mapped[str] = mapped_column(String(32))
    connector_class: Mapped[str] = mapped_column(String(16))

    # Define the relationship to SourceConfig
    # configs: Mapped[list["SourceConfig"]] = relationship("SourceConfig", back_populates="source")
    configs: Mapped[list["Config"]] = relationship()
    options: Mapped[list["Option"]] = relationship()

    # Define the relationship to Item
    # items: Mapped[list["Item"]] = relationship("Item", back_populates="source")

    def __repr__(self) -> str:
        return f"DataSource(id={self.id!r}, name={self.name!r})"


class Item(Base):
    __tablename__ = "item"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    identifier: Mapped[str] = mapped_column()
    url: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column(String(16))
    hierarchy_level: Mapped[int] = mapped_column()
    rank: Mapped[str] = mapped_column()
    summary: Mapped[str] = mapped_column()
    item_type: Mapped[str] = mapped_column(String(16), nullable=True)
    created_time: Mapped[datetime.datetime] = mapped_column()
    # data_source_id: Mapped[int] = mapped_column(ForeignKey("data_source.id"))

    # Define the relationship to Source
    # data_source: Mapped["DataSource"] = relationship()

    # __table_args__ = (UniqueConstraint("identifier", "data_source_id", name='unique_identifier'),)

    # Define the relationship to Status Changelog
    status_changelogs: Mapped[list["ItemStatusChangelog"]] = relationship(
        "ItemStatusChangelog", back_populates="item"
    )
    sprints: Mapped[list["SprintItem"]] = relationship(back_populates="item")
    milestones: Mapped[list["MilestoneItem"]] = relationship(back_populates="item")

    def __repr__(self) -> str:
        return f"Item(id={self.id!r}, identifier={self.identifier!r}, status={self.status!r}, summary={self.summary!r})"


class ItemStatusChangelog(Base):
    __tablename__ = "item_status_changelog"
    id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[UUID] = mapped_column(ForeignKey("item.id"))
    status: Mapped[str] = mapped_column()
    start_time: Mapped[datetime.datetime] = mapped_column()
    end_time: Mapped[datetime.datetime] = mapped_column(nullable=True)
    item: Mapped["Item"] = relationship("Item", back_populates="status_changelogs")

    __table_args__ = (
        UniqueConstraint(
            "item_id", "status", "start_time", name="unique_status_changelog"
        ),
    )


class ItemRankChangelog(Base):
    __tablename__ = "item_rank_changelog"
    id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[UUID] = mapped_column(ForeignKey("item.id"))
    rank: Mapped[str] = mapped_column()
    start_time: Mapped[datetime.datetime] = mapped_column()
    end_time: Mapped[datetime.datetime] = mapped_column(nullable=True)
    item: Mapped["Item"] = relationship("Item")

    __table_args__ = (
        UniqueConstraint("item_id", "rank", "start_time", name="unique_rank_changelog"),
    )


class Sprint(Base):
    __tablename__ = "sprint"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    identifier: Mapped[str] = mapped_column()
    url: Mapped[str] = mapped_column()
    state: Mapped[str] = mapped_column(String(16))
    name: Mapped[str] = mapped_column(String(256))
    order: Mapped[int] = mapped_column()
    create_time: Mapped[datetime.datetime] = mapped_column()
    start_time: Mapped[datetime.datetime] = mapped_column()
    end_time: Mapped[datetime.datetime] = mapped_column()
    complete_time: Mapped[datetime.datetime] = mapped_column(nullable=True)

    # Define the relationship to Item
    items: Mapped[list["SprintItem"]] = relationship(back_populates="sprint")

    def __repr__(self) -> str:
        return f"Sprint(id={self.id!r}, identifier={self.identifier!r}, state={self.state!r})"


class SprintItem(Base):
    __tablename__ = "sprint_item_association"
    sprint_id: Mapped[UUID] = mapped_column(ForeignKey("sprint.id"), primary_key=True)
    item_id: Mapped[UUID] = mapped_column(ForeignKey("item.id"), primary_key=True)
    add_time: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    remove_time: Mapped[datetime.datetime] = mapped_column(nullable=True)

    # Relationships
    item: Mapped["Item"] = relationship(back_populates="sprints")
    sprint: Mapped["Sprint"] = relationship(back_populates="items")

    def __repr__(self) -> str:
        return f"SprintItem(sprint_id={self.sprint_id!r}, item_id={self.item_id!r})"


class Milestone(Base):
    __tablename__ = "milestone"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    identifier: Mapped[str] = mapped_column()
    url: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column(String(256))
    description: Mapped[str] = mapped_column(String(256), nullable=True)
    release_date: Mapped[datetime.datetime] = mapped_column(nullable=True)
    released: Mapped[bool] = mapped_column()

    # Define the relationship to Item
    items: Mapped[list["MilestoneItem"]] = relationship(back_populates="milestone")

    def __repr__(self) -> str:
        return f"Milestone(id={self.id!r}, identifier={self.identifier!r}, name={self.name!r})"


class MilestoneItem(Base):
    __tablename__ = "milestone_item_association"
    milestone_id: Mapped[UUID] = mapped_column(
        ForeignKey("milestone.id"), primary_key=True
    )
    item_id: Mapped[UUID] = mapped_column(ForeignKey("item.id"), primary_key=True)
    add_time: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    remove_time: Mapped[datetime.datetime] = mapped_column(nullable=True)

    # Relationships
    item: Mapped["Item"] = relationship(back_populates="milestones")
    milestone: Mapped["Milestone"] = relationship(back_populates="items")

    def __repr__(self) -> str:
        return f"MilestoneItem(milestone_id={self.milestone_id!r}, item_id={self.item_id!r})"
