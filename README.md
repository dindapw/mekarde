# Data Pipeline and Data Warehouse Repository

This repository contains a collection of DAGs (`dags/`), 
a mini Python module to replicate data (`mekarde/`), 
and a folder for storing analytical queries (`analytics/`).

## Repository Structure

```bash
├── analytics/
│   ├── dwh_accounts.sql                # SQL script for analytical query 1
│   └── ...                           # Additional analytical SQL scripts
├── dags/
│   ├── data_001_transactions.py         # Airflow DAG for replicating data from S3 to Redshift
│   └── ...                           # Additional DAGs for various data processes
│
├── mekarde/
│   ├── file
│   │   ├── copy_s3_to_table.sql
│   │   ├── dwh_transactions_data.sql
│   │   └── ...
│   ├── module             # Python module for data warehouse creation and management
│   │   ├── __init__.py
│   │   ├── etl_common.py
│   │   ├── etl_db.py
│   │   └── etl_file.py
│   └── __init__.py                   # Init file for the module package
│
│
└── README.md                         # This README file
```

## Getting Started

### Prerequisites

- **Python 3.9+**
- **Apache Airflow**
- **Amazon Redshift**
- **Amazon S3**

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your_username/your_repository.git
   cd your_repository
   ```

2. **Install the required Python packages:**
   Ensure you have a Python virtual environment activated, then install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Setting Up Airflow

1. **Configure Airflow:**
   Make sure you have an Airflow instance up and running. Add the necessary connections for AWS (S3) and Redshift in the Airflow UI.

2. **Add DAGs:**
   Copy the DAGs from the `dags/` folder into your Airflow DAGs directory:
   ```bash
   cp dags/* /path_to_your_airflow_dags_folder/
   ```

3. **Start the Airflow Scheduler:**
   ```bash
   airflow scheduler
   ```

4. **Monitor DAGs:**
   Access the Airflow web interface to monitor and trigger DAGs.

### Usage

#### Data Replication from S3 to Redshift

- The `s3_to_redshift_dag.py` DAG automates the process of replicating data from S3 to Redshift. It uses the `s3_to_redshift.py` module, which handles the extraction from S3, transformation if needed, and loading into Redshift.

#### Data Warehouse Creation and Management

- The `data_warehouse_creation_dag.py` DAG automates the creation and management of the Redshift data warehouse. The corresponding `data_warehouse.py` module contains scripts to define schema, create tables, and manage warehouse operations.

#### Analytical Queries

- The `queries/` folder contains SQL scripts for performing various analytical queries on the data stored in the Redshift data warehouse.

### Customization

- **DAGs:** Modify the DAGs in the `dags/` folder to suit your specific data pipeline needs.
- **Python Modules:** Extend or modify the `s3_to_redshift.py` and `data_warehouse.py` modules to implement custom logic for your ETL processes.
- **Queries:** Add, edit, or remove SQL scripts in the `queries/` folder to perform custom data analysis.

### Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -am 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Create a new Pull Request.

### Contact

For any questions, please contact [dindaparamitha.w@gmail.comm](mailto:dindaparamitha.w@gmail.comm).
```

This `README.md` provides a clear overview of the repository structure, setup instructions, usage, and other essential information, making it easy for others to understand and use your project.