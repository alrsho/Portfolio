import requests
import psycopg2
from psycopg2.extras import execute_values
import json
import time

####################################################################  FUNCTIONS  ##############################################################################


def get_list_of_users_from_api(size, offset):
    print("Start load list of users from ELMA API")
    retry_count = 5
    retry_daley = 10
    try:
        url = "https://elma/Vision/Contact/list"
        body = {
            "active": True,
            "fields": {
                "MobilePhone": True,
                "IsActive": True,
                "IsConsentToProcessPersonalData": True,
            },
            "filter": {"tf": {"CountryCode": "ru"}},
            "from": offset,
            "size": size,
        }
        headers = {
            "Authorization": "Bearer token",
            "Content-Type": "application/json",
        }

        retry_num = 0
        while True:
            if retry_num < retry_count:
                response = requests.post(url, json=body, headers=headers)
                if response.status_code != 200:
                    print(
                        "    Load data failed. Wait",
                        retry_daley,
                        "to the next try (try",
                        retry_num,
                        ")",
                    )
                    time.sleep(retry_daley)
                    retry_num += 1
                else:
                    break
            else:
                print("    After", retry_num + 1, "tryes load data is FAILD")
                raise Exception(f"After {retry_count} attempts, the data was not sent")
        print("    Load data sucsess. try:", retry_num + 1)
        data = response.json()
        return data
    except requests.exceptions.HTTPError as errh:
        raise Exception(errh)
    except requests.exceptions.ConnectionError as errc:
        raise Exception(errc)
    except requests.exceptions.Timeout as errt:
        raise Exception(errt)
    except requests.exceptions.RequestException as err:
        raise Exception(err)


def get_DB_connection(ENV, db_pass):
    print("   Get db connection. ENV =", ENV)
    if ENV == "QA":
        proxy_id = "id"
    # elif ENV == 'PRD':
    #     proxy_id = "id"

    proxy_endpoint = "<code>;<code>.mdb.yandexcloud.net"
    port_number = "<port>"
    user = "<user>"
    con_string = f"dbname={proxy_id} user={user} password={db_pass} host={proxy_endpoint} port={port_number} sslmode=require"
    connection = psycopg2.connect(con_string)

    return connection


def insert_data_into_postgres(ENV, db_pass, data):
    print("Start inserting data into Postgres table")
    conn = get_DB_connection(ENV, db_pass)
    cur = conn.cursor()
    insert_query = (
        f"INSERT INTO stg_authmobiles (id, is_active, phone, consent_to_ppd) VALUES %s"
    )
    try:
        cur.execute("TRUNCATE TABLE stg_authmobiles")
        execute_values(cur, insert_query, data)
        conn.commit()
        print("    Insert data into stg layer complete")
        cur.execute("SELECT COUNT(*) FROM stg_authmobiles")
        count_rows = cur.fetchone()[0]
        print("    Count of the rows in stg_authmobiles -", count_rows)
        if count_rows > 0:
            cur.execute("DELETE FROM authmobiles WHERE OriginalId != 'Testing'")
            cur.execute("SELECT f_authmobiles_update()")
            conn.commit()
            print("Table authmobiles succesfull update")
        else:
            print(
                "Table authmobiles dont update< because count of rows in stg_authmobile is",
                count_rows,
            )
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


####################################################################  MAIN SCRIPT  #############################################################################


def handler(event, context):
    print("START MAIN FUNCTION")
    size = 10000
    offset = 0

    if context.function_name == "name":
        ENV = "QA"
    # elif context.function_name=='not_creaed':
    #     ENV = 'PRD'
    else:
        ENV = "NA"

    print(f"ENV {ENV}")

    if ENV != "NA":

        secretId = "secretid"
        headers = {"Authorization": "Bearer " + context.token["access_token"]}
        response = requests.get(
            f"https://payload.lockbox.api.cloud.yandex.net/lockbox/v1/secrets/{secretId}/payload",
            headers=headers,
        )
        db_pass = json.loads(response.text)["entries"][0]["textValue"]

        list_of_rows = []
        data = get_list_of_users_from_api(size, offset)
        total = data["result"]["total"]
        for row in data["result"]["result"]:
            list_of_rows.append(
                [
                    str(row["__id"])[:50],
                    str(row["IsActive"]),
                    str(row["MobilePhone"])[:50],
                    str(row["IsConsentToProcessPersonalData"]),
                ]
            )
        if total > 10000:
            offset += 10000
            while offset < total:
                data = get_list_of_users_from_api(size, offset)
                for row in data["result"]["result"]:
                    list_of_rows.append(
                        [
                            str(row["__id"])[:50],
                            str(row["IsActive"]),
                            str(row["MobilePhone"])[:50],
                            str(row["IsConsentToProcessPersonalData"]),
                        ]
                    )
                offset += 10000

        print("    Count of upload from ELMA rows is:", total)

        insert_data_into_postgres(ENV, db_pass, list_of_rows)
        print("FINISH")
