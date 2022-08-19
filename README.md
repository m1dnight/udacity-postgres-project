# Udacity Postgres Project 

This repository contains the solution to Christophe De Troyer's Postgres project for the course "Data Engineer". 

```
.
├── README.md
├── create_tables.py
├── data
├── docker-compose.yaml
├── etl.ipynb
├── etl.py
├── requirements.txt
├── sql_queries.py
└── test.ipynb
```

`data` contains the log files to import in this project. 
`create_tables.py` sets up the database.
`docker-compose.yaml` creates a local Postgres database.
`etl.ipynb` is a notebook to play with queries. 
`etl.py` process the data and stores in the database.
`sql_queries.py` defines the queries used by `etl.py`.
`test.ipynb` runs some simple tests on the database to check the data.

# Running The Code 

1. To run the project, first a database needs to be created. 
   A Docker Compose file is provided that will start a Postgres database. 

   ```
   docker-compose up -d
   ```
2. Create the default user and database to run the scripts. 
   This will create a user `student` with password `student` and a database `studentdb`.
   ```
   echo "create role student with password 'student'; alter user student createdb; alter user student login superuser; create database studentdb;" | PGPASSWORD=postgres psql -h localhost -U postgres
   ```
  
3. Create a Python virtual environment and install the dependencies. 
   ```
   python3 -m venv venv 
   source venv/bin/activate 
   pip install -r requirements.txt 
   ```
4. Run the `create_tables.py` script to set up the database. 
   ```
   python3 create_tables.py
   ```
5. Process the data by running `etl.py`.
   ```
   python etl.py
   ```

6. Optionally, you can check the database quickly using the `test.ipynb` file, to see if there is data in the database, and if it makes sense.