import importlib
import os
from typing import List, Type
import typing
from eventsourcing.system import System, SingleThreadedRunner
from eventsourcing_sqlalchemy.recorders import SQLAlchemyProcessRecorder
from sqlalchemy import Connection, Engine, delete, select

from rmp.application import (
    ItemDataSourceApplication,
    AnalyticsDbApplication,
    MilestoneDataSourceApplication,
    SprintDataSourceApplication,
)
from rmp.sql_model import (
    Base,
    Config,
    DataSource,
    Item,
    ItemStatusChangelog,
    Milestone,
    Option,
    Sprint,
)
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload
from abc import ABC, abstractmethod


class OptionStorage(ABC):
    @abstractmethod
    def set_option(self, key: str, value: str) -> None:
        pass

    @abstractmethod
    def get_option(self, key: str) -> str | None:
        pass


class DataSourceConnector(ABC):
    def __init__(
        self,
        name: str,
        **config: dict[str, str],
    ):
        self.name = name
        self.config = config

    @abstractmethod
    def load_milestones(self, app: MilestoneDataSourceApplication) -> None:
        pass

    @abstractmethod
    def load_sprints(self, app: SprintDataSourceApplication) -> None:
        pass

    @abstractmethod
    def load_items(self, app: ItemDataSourceApplication) -> None:
        pass

    def set_option_storage(self, option_storage: OptionStorage) -> None:
        self.option_storage = option_storage


class SqlAlchemyOptionStorage(OptionStorage):
    def __init__(self, engine: Engine | Connection, data_source: DataSource) -> None:
        self.engine = engine
        self.data_source = data_source

    def set_option(self, key: str, value: str) -> None:
        with Session(self.engine) as session:
            stmt = select(Option).where(
                Option.key == key, Option.data_source_id == self.data_source.id
            )
            existing = session.scalars(stmt).one_or_none()
            if existing:
                existing.value = value
            else:
                option = Option(
                    key=key,
                    value=value,
                )
                option.data_source = self.data_source
                session.add(option)
            session.commit()

    def get_option(self, key: str) -> str | None:
        with Session(self.engine) as session:
            stmt = select(Option).where(
                Option.key == key, Option.data_source_id == self.data_source.id
            )
            existing = session.scalars(stmt).one_or_none()
            if existing:
                return existing.value

            return None


class Backend:
    def __init__(self) -> None:
        os.environ["PERSISTENCE_MODULE"] = "eventsourcing_sqlalchemy"

        # Create event sourcing system
        system = System(
            pipes=[
                [ItemDataSourceApplication, AnalyticsDbApplication],
                [SprintDataSourceApplication, AnalyticsDbApplication],
                [MilestoneDataSourceApplication, AnalyticsDbApplication],
            ]
        )
        self._runner = SingleThreadedRunner(system)

        self._item_app = self._runner.get(ItemDataSourceApplication)
        self._sprint_app = self._runner.get(SprintDataSourceApplication)
        self._milestone_app = self._runner.get(MilestoneDataSourceApplication)

        # Get engine/bind, assuming the use of sqlalchemy persistence module
        recorder = typing.cast(SQLAlchemyProcessRecorder, self._item_app.recorder)
        if recorder.datastore.engine is None:
            raise RuntimeError("Database engine could not be initialized")

        self._engine = recorder.datastore.engine

        # Create other tables
        Base.metadata.create_all(self._engine)

        # Start the runner
        self._runner.start()

    def add_connector(
        self,
        connector_class: Type[DataSourceConnector],
        name: str,
        **kwargs: dict[str, str],
    ) -> None:
        connector = connector_class(name, **kwargs)
        with Session(self._engine) as session:
            data_source = DataSource(
                name=connector.name,
                connector_module=connector_class.__module__,
                connector_class=connector_class.__name__,
            )

            for key, value in kwargs.items():
                config = Config(key=key, value=value)
                data_source.configs.append(config)
            session.add(data_source)
            session.commit()
            print(f"Created and stored new data source: {data_source}")

    def load_data(self) -> None:
        with Session(self._engine) as session:
            stmt = select(DataSource).options(joinedload(DataSource.configs))
            data_sources = session.scalars(stmt).unique().all()

            connectors: List[DataSourceConnector] = []
            for data_source in data_sources:
                config_kv = dict()
                for config in data_source.configs:
                    config_kv[config.key] = config.value
                module = importlib.import_module(data_source.connector_module)
                class_ = getattr(module, data_source.connector_class)

                if not issubclass(class_, DataSourceConnector):
                    raise TypeError(
                        f"Class '{data_source.connector_class}' does not inherit from DataSourceConnector"
                    )

                option_storage = SqlAlchemyOptionStorage(self._engine, data_source)
                connector = class_(data_source.name, **config_kv)
                connector.set_option_storage(option_storage)
                connectors.append(connector)

        for connector in connectors:
            self._load_connector_data(connector)

    def _load_connector_data(self, connector: DataSourceConnector) -> None:
        connector.load_milestones(self._milestone_app)
        connector.load_sprints(self._sprint_app)
        connector.load_items(self._item_app)

    def replay(self) -> None:
        app: AnalyticsDbApplication = self._runner.get(AnalyticsDbApplication)
        recorder = typing.cast(SQLAlchemyProcessRecorder, app.recorder)
        table = recorder.tracking_table

        with Session(self._engine) as session:
            session.execute(table.delete())
            session.execute(delete(ItemStatusChangelog))
            session.execute(delete(Item))
            session.execute(delete(Sprint))
            session.execute(delete(Milestone))
            session.commit()

        app.pull_and_process(ItemDataSourceApplication.__name__)
