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
    try:
        cursor.execute("""
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    md5 TEXT,
    file_ext TEXT,
    tag_string TEXT,
    tag_count_general INTEGER
)
        """)
    except sqlite3.OperationalError:
        pass

    insert = []
    for path in json_path_dir_list:
        nowfile = path.split("\\")[-1]
        f = open(path, "r", encoding="utf8")
        count = 0
        for i in f:
            data = loads(i)
            try:
                insert.append((int(data["id"]), data["md5"], data["file_ext"], data["tag_string"], int(data["tag_count_general"])))
            except KeyError:
                pass
            if len(insert) == import_size:
                cursor.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?)", insert)
                conn.commit()
                print(f"{nowfile} :: {len(insert)} rows imported. {(len(insert) + import_size*count)}")
                insert = []
                count += 1
    
    if len(insert) > 0:
        cursor.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?)", insert)
        conn.commit()
        print(f"{nowfile} :: {len(insert)} rows imported. {(len(insert) + import_size*count)}")
    
    conn.close()

    