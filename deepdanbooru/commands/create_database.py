import os
import sqlite3
from json import loads
from gc import collect

import deepdanbooru as dd

def create_database(
    project_path, json_path, import_size=10, skip_unique = False, use_dbmem = False, create_new = False
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
    if use_dbmem:
        conn = sqlite3.connect(":memory:")
        if not create_new:    
            connsource = sqlite3.connect(os.path.join(project_path, "metadata.db"))

            print("DATABASE DISK --> MEMORY")
            connsource.backup(conn)
            print("DATABASE DISK --> MEMORY OK")
            connsource.close()
    else:
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
    
    for path in json_path_dir_list:
        nowfile = path.split("\\")[-1]
        f = open(path, "rb")

        
        count = 0
        insert = []
        
        for data in f:
            data = loads(data)

            try:
                insert.append((int(data["id"]), data["md5"], data["file_ext"], data["tag_string"], int(data["tag_count_general"])))
            except KeyError:
                pass
            if len(insert) == import_size:
                try:
                    cursor.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?)", insert)
                    conn.commit()
                    print(f"{nowfile} :: {len(insert)} rows imported. {(len(insert) + import_size*count)}")
                    count += 1
                except sqlite3.IntegrityError as e:
                    if skip_unique:
                        print(f"{nowfile} :: SKIPPING: {e}, {(import_size*count)}")
                    else:
                        print(f"{nowfile} :: ERROR: {e}, {(import_size*count)}")
                        exit()
                insert = []
        
        f.close()

        if len(insert) > 0:
            try:
                cursor.executemany("INSERT INTO posts VALUES (?, ?, ?, ?, ?)", insert)
                conn.commit()
                print(f"{nowfile} :: {len(insert)} rows imported. {(len(insert) + import_size*count)}")
            except sqlite3.IntegrityError as e:
                if skip_unique:
                    print(f"{nowfile} :: SKIPPING: {e}, {(import_size*count)}")
                else:
                    print(f"{nowfile} :: ERROR: {e}, {(import_size*count)}")
                    exit()

        try:
            del(insert)
            del(count)
            del(f)
        except:
            pass

        collect()
    

    if use_dbmem:
        connsource = sqlite3.connect(os.path.join(project_path, "metadata.db"))
        
        print("Database MEMORY --> DISK")
        conn.backup(connsource)
        print("Database MEMORY --> DISK OK")
        
        connsource.close()

    conn.close()


    