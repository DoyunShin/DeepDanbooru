import os
import sqlite3
from json import loads

import deepdanbooru as dd

def create_database(
    project_path, json_path, import_size=10
):
    """
    Create new database with default parameters.
    """
    project_context_path = os.path.join(project_path)

    #get json_path dir list
    json_path_dir_list = dd.io.get_directory_list(json_path, "posts*")
    if json_path_dir_list == []:
        print("ERROR: No json file found.")
        return

    # Open Database
    conn = sqlite3.connect(os.path.join(project_path, "metadata.db"))
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        md5 TEXT,
        file_ext TEXT,
        tag_string TEXT,
        tag_count_general INTEGER,
        """)

    count = 0
    insert = []
    for path in json_path_dir_list:
        nowfile = path.split("\\")[-1]
        f = open(path, "r")
        for i in path:
            data = loads(i)

            insert.append((int(data["id"]), data["md5"], data["file_ext"], data["tag_string"], int(data["tag_count_general"])))
            if len(insert) == import_size:
                cursor.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?)", insert)
                conn.commit()
                insert = []
                count = 0
                print(f"{nowfile} :: {count} row imported.")
    
    if len(insert) > 0:
        cursor.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?)", insert)
        conn.commit()
        print(f"{nowfile} :: {count} row imported.")
    
    conn.close()

    