# Data Pipeline and Data Warehouse Repository

This repository contains a collection of DAGs (`dags/`), 
a mini Python module to replicate data (`mekarde/`), 
and a folder for storing analytical queries (`analytics/`).

## Repository Structure

```bash
├── analytics/
│   ├── dwh_transaction_date.sql         # SQL script for analytical related to transaction date
│   └── ...                              # Additional analytical SQL scripts
│
├── dags/
│   ├── data_001_transactions.py         # Airflow DAG for replicating transactions related data
│   └── ...                              # Additional DAGs for various data processes
│
├── mekarde/
│   ├── file
│   │   ├── copy_s3_to_table.sql         # Query template to copy data from s3 to table
│   │   ├── dwh_transactions_data.sql    # Query to transform data
│   │   └── ...
│   ├── module                           
│   │   ├── __init__.py
│   │   ├── etl_common.py                # module containing common functions such as get_s3_client, get_redshift_connection, etc
│   │   ├── etl_db.py                    # module to replicate data between redshift tables (data transformation)
│   │   └── etl_file.py                  # module to replicate data from file to redshift table
│   └── __init__.py                      # Init file for the module package
│
├── requirement.txt                      # file containing python module to be installed
├── .gitignore                           # file containing list of folder / files to be ignored by git
├── .airflowignore                       # file containing list of folder / files to be ignored by airflow
└── README.md                            # This README file
```

## Getting Started

### Prerequisites

- **Python 3.9+**
- **Apache Airflow**
- **Amazon Redshift**
- **Amazon System Manager**
- **Amazon S3**

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/dindapw/mekarde.git
   cd mekarde
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
   
3. **Add Modules:**
   Copy the Python Module from the `mekarde/` folder into your Airflow DAGs directory:
   ```bash
   cp mekarde/* /path_to_your_airflow_dags_folder/mekarde
   ```
   then, add `mekarde/*` in your `.airflowignore` so that it's not picked up as DAG by airflow.

   OR add `mekarde/` into your PYTHONPATH

4**Start the Airflow Scheduler:**
   ```bash
   airflow scheduler
   ```

5**Monitor DAGs:**
   Access the Airflow web interface to monitor and trigger DAGs.

### Usage

This module is already up and running in http://52.76.59.22:8080/.
Credentials can be obtained separately.

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
