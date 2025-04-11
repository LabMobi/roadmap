# Roadmap
Roadmap is a Python package that helps to analyze and visualize team's agile software development process. It provides insights into delivery performance and aims at creating more realistic roadmaps by leveraging the team historical performance.

## Installation

### Install from PyPI
```sh
$ pip install [TODO: package name]
```

## Usage examples

### Configure backend and load data from Jira
```python
from rmp.backend import Backend
from rmp.jira import JiraCloudConnector
import os

# For data storage, configure SQLAlchemy compatible URL
os.environ['SQLALCHEMY_URL'] = 'sqlite:///my_db.sqlite'

# Create instance of backend
backend = Backend()

# Load data
backend.add_connector(
    JiraCloudConnector,
    name='Jira Loader',
    domain='example',
    username='john.doe@example.com',
    api_token='API_TOKEN',
    jql = 'project = SPACE',
    board_id = 42
)
backend.load_data()
```

### Analyze Flow Metrics

```python
from sqlalchemy import create_engine
from rmp.flow_metrics import FlowMetrics, Workflow
from datetime import datetime

# Create engine for data access
engine = create_engine(f"sqlite:///my_db.sqlite", echo=False)

# Configure workflow stages
workflow = Workflow(
    not_started=['To Do'],
    in_progress=['In Progress', 'Code review', 'Testing'],
    finished=['Done', 'Cancelled'],
)

# Create instance of FlowMetrics
fm = FlowMetrics(engine, workflow)

# Define time ranges to exclude from analytics
excludes = [
    DateTimeRange("2024-12-23", "2025-01-05"), # Christmas period, team offline
    DateTimeRange("2025-04-14", "2025-04-21"), # Holy Week, most of the team away
]

# Plot cycle time scatter chart
fm.plot_cycle_time_scatter(exclude_ranges=excludes)

# Plot cycle time histogram
fm.plot_cycle_time_histogram(exclude_ranges=excludes)

# Plot aging work in progress chart
fm.plot_aging_wip(exclude_ranges=excludes)

# Plot throughput run chart
fm.plot_throughput_run_chart(exclude_ranges=excludes)

# Plot cumulative flow diagram
fm.plot_cfd(highlight_ranges=excludes)

# Find dates and probabilities to deliver 90 items using Monte Carlo simulation
fm.plot_monte_carlo_when_hist(runs=10000, item_count=90, exclude_ranges=excludes)

# Find how many items can be delivered by date with their probabilities using Monte Carlo simulation
target_date = datetime.now() + pd.Timedelta(days=30)
fm.plot_monte_carlo_how_many_hist(runs=10000, target_date=target_date, exclude_ranges=excludes)

# Output prioritised backlog with 85% confidence forecast of delivery dates  
fm.df_backlog_items(mc_when=True, mc_when_runs=1000, mc_when_percentile=85, exclude_ranges=excludes)
```

## Development

Select Python version using pyenv
```sh
pyenv local 3.11.8
```

Install Poetry dependencies
```sh
poetry install
```

Activate virtual environment
```sh
eval $(poetry env activate)
```

Run tests
```sh
pytest
```

Check code style and format
```sh
ruff check
ruff format
```

Run static type checker
```sh
mypy
```
