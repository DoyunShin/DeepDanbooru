from pathlib import Path
from gc import collect
from hashlib import md5

import deepdanbooru as dd

def move_to_md5(source_path, destination_path):
    """
    Move image to md5 name.
    """
    image_dir = Path(source_path)
    destination_dir = Path(destination_path)
    if not image_dir.exists():
        raise FileNotFoundError(f"{image_dir} does not exist.")
    
    destination_dir.mkdir(parents=True, exist_ok=True)
    for image_file in image_dir.glob("**/*"):
        if image_file.is_file():
            image_file_md5 = str(md5(image_file.read_bytes()).hexdigest())+image_file.suffix
            image_file_md5_path = destination_dir / image_file_md5
            image_file.rename(image_file_md5_path)
            print(f"{image_file} -> {image_file_md5_path}")
            collect()

    pass