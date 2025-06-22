This section implements an ETL process that retrieves the list of users via the API from ELMA and saves it to the POSTGRES table. 
The Python script and the POSTGRES database instance are located on the Yandex Cloud cloud server.


main.py:
The script downloads the list of users from ELMA in parts of 10,000 entries. 
Then the dataset is saved to the raw layer table in the database and the postgres function "f_authmobiles_update" is called


f_authmobiles_update.sql:
A postgres function that processes user data from the raw data layer and loads it into a table in the dds layer