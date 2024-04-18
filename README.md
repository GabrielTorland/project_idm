# I am a good title  

In today's data-driven world, the ability to make informed decisions swiftly based on real-time data is crucial. However, ODB systems may struggle with this due to their sheer volume. As a solution, DW are employed for focused analytical tasks. A DW can be any form of database, typically selected based on specific data needs. Furthermore, data warehouses often include a front-end interface that provides visualizations for better data interpretation. In this project, we will implement three distinct DW for an E-Commerce dataset: one based on SQL and the other two on NoSQL. A unified front-end will serve all three DW. The objective is to evaluate the advantages and disadvantages of each system in handling specific analytical tasks.


## Directory Structure

The project is organized into several directories, each dedicated to a specific component of the data architecture:

- `grafana`: Contains Docker configurations for running Grafana.
- `mongodb`: Contains Docker configurations, scripts, and files for setting up MongoDB DW.
- `neo4j`: Contains Docker configurations, scripts, and files for setting up Neo4j DW.
- `sql`: Contains  Docker configurations, script, and files for setting up SQL ODB and DW.

## Setup

### ODB

Navigate to the SQL directory:

```sh
cd sql
```

Create a virtual environment and install the required dependencies:

```sh
pip install -r requirements.txt
```

Start MySQL server and phpMyAdmin:
```sh
docker-compose up -d
```

Initialize the database schema:

```sh
python3 manage.py --create
```

pulate the database with user, category, product, and event data from CSV files. Alternatively, if you have access to the database dump, use the `propagate_db_from_csv.sh` bash script for slightly faster execution:

```sh
python3 manage.py --insert_users_and_categories
python3 manage.py --insert_products
python3 manage.py --insert_events
```

This process may take several hours, but once completed, the ODB will be fully configured.

### SQL DW

Ensure that you are still in the correct directory and the environment is active. Create a new database named dw on your MySQL server, then execute the following script to synchronize it with the ODB:

```sh
python3 sync_dw_with_odb.py
```

Next, create or update the summary tables:

```sh
python3 sync_summary_table.py
```


### Neo4J DW

Navigate to the Neo4j Folder:

```sh
cd neo4j
```

Install the required dependencies:

```sh
pip install --no-cache-dir -r requirements.txt
```

Start the neo4j server:

```sh
docker compose up -d
```

Run the following script to migrate the DW to neo4j:

```sh
python3 sync_neo4j_with_dw.py
```

This will take some time. After its complete, add the neo4j dashboard to grafana using the JSON file in the Neo4j folder.


### MongoDB DW

Navigate to the MongoDB Folder:

```sh
cd mongodb
```

Install the required dependencies:

```sh
pip install -r requirements.txt
```

Start the MongoDB server:

```sh
docker compose up -d
```

Migrate the DW to MongoDB using this script:

```sh
python3 migrate_to_mongodb.py
```

After its done loading, add the MongoDB dashboard to grafana using the JSON file in the MongoDB folder.

### Grafana

Navigate to the Grafana folder:

```sh
cd grafana
```

Start the Grafana interface server:

```sh
docker compose up -d
```

Now, if you have followed the previous steps, you should be able to create the dashboards. Start by creating the data sources for each DW. Then, import the dashboard files from their respective dirs.

<img width="1128" alt="image" src="https://github.com/GabrielTorland/project_idm/assets/80287447/d8f55105-329e-4751-b29d-beaf77ab14c8">

