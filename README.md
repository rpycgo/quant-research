# quant-research

> A comprehensive framework for quantitative trading and financial research

## 📋 Overview

**quant-research** is a comprehensive platform designed to collect and analyze financial market data. This project provides the essential data pipelines needed for developing quantitative trading strategies and conducting financial research.

### Current Status

The project is currently at the **data collection implementation stage**. Functionality for systematically collecting and preprocessing data from various financial data sources has been fully implemented.

## 🎯 Key Features

- **Data Collection**: Automated data collection from diverse financial data sources
- **Data Collector**: Real-time and historical data collection capabilities
- **Data Ingestion**: Validation, cleaning, and storage of collected data
- **Dashboards**: Visualization and monitoring of collected data
- **Docker Support**: Consistent development environment setup

## 📁 Project Structure

```
quant-research/
├── src/                      # Core source code
│   ├── collector/           # Data collection module
│   ├── ingester/            # Data ingestion and storage module
│   └── utils/               # Utility functions
├── dashboards/              # Dashboards and visualizations
├── run_collector.py         # Data collector execution script
├── run_ingestor.py          # Data ingestion execution script
├── pyproject.toml           # Python project configuration (Poetry)
├── poetry.lock              # Locked dependencies file
├── docker-compose.yml       # Docker composition file
└── README.md                # This file
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Poetry (dependency management)
- Docker & Docker Compose (optional)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/rpycgo/quant-research.git
cd quant-research
```

2. **Install dependencies using Poetry**
```bash
poetry install
```

3. **Activate the Python virtual environment**
```bash
poetry shell
```

### Installation with Docker (Recommended)

```bash
docker-compose up -d
```

## 💻 Usage

### Running the Data Collector

```bash
python run_collector.py
```

The data collector automatically gathers financial data from configured data sources.

### Running the Data Ingestion

```bash
python run_ingestor.py
```

Cleans the collected data and stores it in the database or repository.

## 📊 Module Overview

### Collector
- Collects data from various financial data APIs
- Supports automated scheduling
- Error handling and retry mechanisms

### Ingester
- Validation and cleaning of raw collected data
- Data normalization and formatting
- Repository management and optimization

### Dashboards
- Real-time visualization of collected data
- Market indicators and performance monitoring
- Data quality validation

## ⚙️ Configuration

To customize the project's behavior, check the following:

- `pyproject.toml`: Python package and script configuration
- `docker-compose.yml`: Container service configuration
- Configuration files in each module

## 🔧 Tech Stack

- **Language**: Python 3.8+
- **Package Management**: Poetry
- **Containerization**: Docker & Docker Compose
- **Data Processing**: Pandas, NumPy
- **Visualization**: (Dashboard components)

## 📦 Dependencies

Main dependencies are defined in `pyproject.toml`.

```bash
poetry show  # View all installed dependencies
```
