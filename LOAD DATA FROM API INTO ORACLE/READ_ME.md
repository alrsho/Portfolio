A set of functions for uploading data via the API and saving it to the ORACLE database.

get_data.bat:
a bat file that invokes a python function and defines a policy for writing logs to a txt file.


get_data.py:
функция python которая осуществляет последовательную выгрузку данных из источника по API. 
Набор данных преобразовывается при помощи pandas и записывается в сырой слой данных в oracle. 
Далее  происходит вызов oracle процедуры, которая загружает данные в таблицу dds слоя

P_UPDATE.sql:
Converts data from the raw layer and saves it to the dds layer. 
Prepares the data for subsequent incremental upload to the target system.