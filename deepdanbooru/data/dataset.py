import os
import sqlite3
from gc import collect


def load_tags(tags_path):
    with open(tags_path, "r") as tags_stream:
        tags = [tag for tag in (tag.strip() for tag in tags_stream) if tag]
        return tags


def load_image_records(sqlite_path, minimum_tag_count, use_dbmem, load_as_md5, no_md5_folder):
    if not os.path.exists(sqlite_path):
        raise Exception(f"SQLite database is not exists : {sqlite_path}")
    if use_dbmem:
        connection = sqlite3.connect(":memory:")

        connection_disk = sqlite3.connect(sqlite_path)
        connection_disk.backup(connection)
        connection_disk.close()
    else:
        connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    image_folder_path = os.path.join(os.path.dirname(sqlite_path), "images")

    if load_as_md5:
        data = []
        for filename in os.listdir(image_folder_path):
            if filename.endswith(".jpg") or filename.endswith(".png") or filename.endswith(".jpeg"):
                data.append((filename.split(".")[0], minimum_tag_count))
        cursor.execute(
            "SELECT md5, file_ext, tag_string FROM posts WHERE (md5 = ?) AND (tag_count_general >= ?)",
            data
        )
    else:
        cursor.execute(
            "SELECT md5, file_ext, tag_string FROM posts WHERE (file_ext = 'png' OR file_ext = 'jpg' OR file_ext = 'jpeg') AND (tag_count_general >= ?) ORDER BY id",
            (minimum_tag_count,),
        )

    rows = cursor.fetchall()

    image_records = []

    for row in rows:
        md5 = row["md5"]
        extension = row["file_ext"]
        if no_md5_folder:
            image_path = os.path.join(image_folder_path, f"{md5}.{extension}")
        else:
            image_path = os.path.join(image_folder_path, md5[0:2], f"{md5}.{extension}")
        tag_string = row["tag_string"]

        image_records.append((image_path, tag_string))

    connection.close()
    del(connection)
    del(data)
    collect()

    return image_records
