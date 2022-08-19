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
  
3. 