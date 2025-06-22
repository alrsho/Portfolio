import requests
import json
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
import os
import cx_Oracle
import sys

ENV = sys.argv[1]

if ENV == "PRD":
    import ora_config_stg_msc_prod as config
elif ENV == "QA":
    import ora_config_stg_msc as config
elif ENV == "DEV":
    import ora_config_stg_msc as config
else:
    print("ERROR - Unknown shema")
    raise Exception("ERROR - Unknown shema")


# ----------------------------------------------------------------------------------------------------------
# --------------------------------------------PARAMETERS----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------

api_key = "<key>"
autosearch_list = [
    "233875",
    "233876",
    "233879",
    "233880",
    "233883",
    "233886",
    "233889",
    "233890",
    "233891",
    "233894",
]
batch_size = 1000
retry_count = 10
retry_daley = 60
report_id = "9825"
table = "STG_DATA"

# ----------------------------------------------------------------------------------------------------------
# --------------------------------------------FUNCTIONS-----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------


# request data function:
def send_request_for_data(autosearch_id, report_id, bath_size, api_key):
    try:
        print(f"{datetime.now()}     Start function send_request_for_data")
        url = f"https://tenderland.ru/Api/v1/Export/Create?autosearchId={autosearch_id}&exportViewId={report_id}&limit=5000&batchSize={bath_size}&format=json&apiKey={api_key}"
        retry_num = 0
        while True:
            if retry_num < retry_count:
                response = requests.get(url)
                print(
                    f"{datetime.now()}         Try to send request {retry_num+1}, URL status is {response.status_code}"
                )
                if response.status_code != 200:
                    print(
                        f"{datetime.now()}         URL status != 200. Wait {retry_daley} seconds"
                    )
                    time.sleep(retry_daley)
                    retry_num += 1
                else:
                    break
            else:
                raise Exception(f"After {retry_count} attempts, the data was not sent")
        data = response.json()
        print(
            f"{datetime.now()}         Data request succesfull create, id: {data['Id']}, count of rows: {data['TotalCount']}"
        )
        return data
    except requests.exceptions.HTTPError as errh:
        print(datetime.now(), "Http Error:" + errh)
        raise Exception(errh)
    except requests.exceptions.ConnectionError as errc:
        print(datetime.now(), "Error Connecting:" + errc)
        raise Exception(errc)
    except requests.exceptions.Timeout as errt:
        print(datetime.now(), "Timeout Error:" + errt)
        raise Exception(errt)
    except requests.exceptions.RequestException as err:
        print(datetime.now(), "OOps: Something Else" + err)
        raise Exception(err)


# function for parsing rows
def deep_get(data, path, default=""):
    keys = path.split(".")
    current = data
    for key in keys:
        try:
            if isinstance(current, dict):
                current = current.get(key, default)
            elif isinstance(current, list):
                if key.isdigit() and int(key) < len(current):
                    current = current[int(key)]
                else:
                    return default
            else:
                return default
        except (AttributeError, TypeError, ValueError):
            return default
    return current if current is not None else default


# load data and save into list of lists
def load_data_from_api(request, offset, autosearch_id, api_key):
    try:
        print(f"{datetime.now()}     Starting load data by function load_data_from_api")
        url = f"https://tenderland.ru/Api/v1/Export/Get?exportId={request}&offset={offset}&apiKey={api_key}"
        retry_num = 0
        while True:
            if retry_num < retry_count:
                response = requests.get(url)
                print(
                    f"{datetime.now()}         Try {retry_num+1} to download data from request {request_id}, offset {offset}, URL status is {response.status_code}"
                )
                if response.status_code != 200:
                    print(
                        f"{datetime.now()}         URL status != 200. Wait {retry_daley} seconds"
                    )
                    time.sleep(retry_daley)
                    retry_num += 1
                else:
                    break
            else:
                raise Exception(
                    f"After {retry_count} attempts, the data was not download"
                )
        print(
            f"{datetime.now()}         Import data from request {request_id}, offset {offset} was successful"
        )
        data = response.json()
        list_of_rows = []
        for row in data["items"]:
            list_of_rows.append(
                [
                    str(row["tender"]["id"]),
                    str(deep_get(row, "tender.regNumber")),
                    str(deep_get(row, "tender.name"))[:2000],
                    str(deep_get(row, "tender.beginPrice")),
                    str(deep_get(row, "tender.customers.0.lotCustomerShortName"))[
                        :2000
                    ],
                    str(deep_get(row, "tender.region"))[:500],
                    str(deep_get(row, "tender.publishDate")),
                    str(deep_get(row, "tender.beginDate")),
                    str(deep_get(row, "tender.endDate")),
                    str(deep_get(row, "tender.typeName"))[:500],
                    str(deep_get(row, "tender.sourceLink"))[:2000],
                    str(deep_get(row, "tender.lotCategories.0"))[:300],
                    str(deep_get(row, "tender.lotDeliveryPlacesText.0"))[:2000],
                    "\n".join(
                        [
                            i.get("lotKtruName", "")
                            for i in deep_get(row, "tender.products")
                        ]
                    ),
                    str(deep_get(row, "tender.customers.0.lotCustomerInn")),
                    str(deep_get(row, "tender.sysUpdateDate")),
                    str(autosearch_id),
                    str(datetime.now()),
                    "NEW_ROW",
                ]
            )

        return list_of_rows
    except requests.exceptions.HTTPError as errh:
        print(datetime.now(), "Http Error:" + errh)
        raise Exception(errh)
    except requests.exceptions.ConnectionError as errc:
        print(datetime.now(), "Error Connecting:" + errc)
        raise Exception(errc)
    except requests.exceptions.Timeout as errt:
        print(datetime.now(), "Timeout Error:" + errt)
        raise Exception(errt)
    except requests.exceptions.RequestException as err:
        print(datetime.now(), "OOps: Something Else" + err)
        raise Exception(err)


# Create connection:
def get_connect():
    try:
        conn = None
        conn = cx_Oracle.connect(
            config.username, config.password, config.dsn, encoding=config.encoding
        )
        print(datetime.now(), "Connection to Oracle DB successfully created")
    except Exception as error:
        print("Connection to Oracle DB not establish! Please check config!")
        print(error)
        raise Exception(error)
    finally:
        return conn


# Bulk insert data into Oracle db:
def insert_db_data(received_rows, table_name):
    conn = get_connect()
    cur = conn.cursor()
    try:
        sql = f"TRUNCATE TABLE {table_name}"
        cur.execute(sql)
        print(datetime.now(), "Truncate table coplit")

        sql = f"""
        INSERT INTO {table_name} (TENDER_ID, TENDER_NUMBER, TENDER_NAME, BEGIN_PRICE, CUSTOMER_NAME, REGION, PUBLISH_DATE, START_DATE, END_DATE, TENDER_TYPE, SOURSE_LINK, LOT_CATEGORY, LOT_DELIVERY_PLACE, LOT_KTRU_NAME, CUSTOMER_INN, UPDATE_DATE, AUTOSEARCH_ID, LOAD_DATE, ROW_TYPE)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19)
        """
        cur.executemany(sql, received_rows)
        print(datetime.now(), "insert into STG complit")
        conn.commit()

        print(datetime.now(), "Commit complited")
    except Exception as error:
        print(f"Error inserting data: {error}")
        conn.rollback()
        raise Exception(error)
    finally:
        cur.close()
        conn.close()


# Load data from STG into DM table
def call_procedure():
    conn = get_connect()
    cur = conn.cursor()
    try:
        print(datetime.now(), " Change NLC settings")
        nlc_settings = """
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_LANGUAGE = ''RUSSIAN''';
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TERRITORY = ''RUSSIA''';
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_LANGUAGE = ''RUSSIAN''';
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''DD.MM.RR''';
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''DD.MM.RR HH24:MI:SSXFF''';
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_SORT = ''RUSSIAN''';
            EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_COMP = ''BINARY''';
        END;
        """
        cur.execute(nlc_settings)

        print(datetime.now(), " Calling pricedure")
        cur.callproc("P_MKT_TENDERLAND_UPDATE")
        conn.commit()
        print(datetime.now(), " Call procedure coplit")
    except Exception as error:
        print(f"Error in def call_procedure: {error}")
        conn.rollback()
        raise Exception(error)
    finally:
        cur.close()
        conn.close()


# ----------------------------------------------------------------------------------------------------------
# --------------------------------------------MAIN SCRIPT---------------------------------------------------
# ----------------------------------------------------------------------------------------------------------
columns = [
    "TENDER_ID",
    "TENDER_NUMBER",
    "TENDER_NAME",
    "BEGIN_PRICE",
    "CUSTOMER_NAME",
    "REGION",
    "PUBLISH_DATE",
    "START_DATE",
    "END_DATE",
    "TENDER_TYPE",
    "SOURSE_LINK",
    "LOT_CATEGORY",
    "LOT_DELIVERY_PLACE",
    "LOT_KTRU_NAME",
    "CUSTOMER_INN",
    "UPDATE_DATE",
    "AUTOSEARCH_ID",
    "LOAD_DATE",
    "ROW_TYPE",
]


dataset = []


# loop which load data from all autosearches
for autosearch_id in autosearch_list:
    print(f"{datetime.now()} Start load data from autosearch {autosearch_id}")
    # Send request for data
    request = send_request_for_data(autosearch_id, report_id, batch_size, api_key)
    request_id = request["Id"]
    request_count_rows = request["TotalCount"]

    # Slip for 5 seconds
    print(f"{datetime.now()}     Slip for 5 seconds")
    time.sleep(5)

    # Load data from autosearch
    offset = 0
    while offset < request_count_rows:
        chank_of_data = load_data_from_api(request_id, offset, autosearch_id, api_key)
        dataset.extend(chank_of_data)
        print(
            f"{datetime.now()}         Insert batch with offset = {offset} into dataset was successful"
        )
        offset += batch_size
    df = pd.DataFrame(dataset, columns=columns)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, "data.xlsx")
    df.to_excel(output_path, index=False, engine="openpyxl")
    print(f"{datetime.now()}     Dataset saving into xlsx")


# insert into STG layer:
insert_db_data(dataset, table)

# Call procedure which insert data into DM table
call_procedure()
