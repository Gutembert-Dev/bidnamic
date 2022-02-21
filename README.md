pip install psycopg2 pandas

Ensure your postgres user has createdb role (alter user <user_name> createdb;)
Ensure your postgres user is a superuser (ALTER USER <user_name> WITH SUPERUSER;)

Provide the absolute path of the CSVs folder

For the Database instanciation, use the dbname postgres.
After creating the targeted database, initializing the db table, loading the csv files into the newly created db,
instanciate the Search class using the newly created db in order to perform search.