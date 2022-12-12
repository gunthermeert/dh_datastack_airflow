import os

# Get environment variables
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR')

print('test environment variables')
print(AIRFLOW_HOME)
print(DBT_PROFILES_DIR)