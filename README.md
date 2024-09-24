# Apache airflow aggregate app

Application for scheduled batch aggregation of user activity data for a week

## Technologies

- **Python**
- **Apache airflow**
- **Docker & Docker Compose**
- **Spark(not work)**

## Getting Started

Here's how to get the project up and running on your local machine for development and testing.

### Prerequisites

- Docker
- Docker Compose

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/DedMokus/VK_Data_Enj_Test.git
   cd VK_Data_Enj_Test
   ```

2. **Build and run with Docker Compose:**
    In Linux you need to run this command
    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

   ```bash
   mkdir -p ./input ./output ./days_aggregate ./logs ./config ./plugins
   docker-compose up airflow-init
   docker-compose up
   ```
   This spins up basic airflow services

### Usage

Airflow web is accessible at `http://localhost:8080` after startup.

### Usage scenario

Every day at 7:00 a.m., the script aggregates data on user activity for the week. Script aggregates data for each day into a separate directory for less resource consumption.

### Flaw

I was unable to launch airflow with spark. There are files in the spark_airflow directory, they should run an instance with spark, but they are unfinished 