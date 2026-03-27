-- Create Hive Metastore schema (already specified via POSTGRES_DB=metastore in docker-compose)

-- Create Airflow DB and user
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Optional: Grant privileges if needed
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
