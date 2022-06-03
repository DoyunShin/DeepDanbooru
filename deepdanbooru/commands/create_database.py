import os
import sqlite3
from json import loads
from gc import collect
import mmap

import deepdanbooru as dd

def create_database(
    project_path, json_path, import_size=10, use_allmem = False, skip_unique = False, use_dbmem = False,
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

    if use_dbmem:
        #conn.enable_load_extension(True)
        #conn.load_extension(os.path.join(project_path, "lib", "sqlite_mmap"))
        conn.execute("PRAGMA journal_mode = MEMORY")

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
        f = open(path, "r", encoding="utf8")

        if use_allmem:
            data = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        else:
            data = f
        
        count = 0
        insert = []
        
        for i in data:
            data = loads(i)
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
        try:
            f.close()
        except:
            pass
        try:
            data.close()
        except:
            pass

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
            del(data)
            del(f)
        except:
            pass
        
        collect()
    

    
    conn.close()

    