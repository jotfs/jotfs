import subprocess
import os
import hashlib
import random
import tempfile
import sqlite3
import time
import shutil

import boto3
import toml
import blake3


DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(DIR, "data")
TEST_DIR = os.path.join(DIR, f"test-{int(time.time() * 1000)}")
DOWNLOADS_DIR = os.path.join(TEST_DIR, "downloads")
MINIO_DIR = os.path.join(TEST_DIR, "minio")
CFG = toml.load("config.toml")
BUCKET = CFG["store"]["bucket"]
DBNAME = os.path.join(TEST_DIR, CFG["server"]["database"])

session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    aws_access_key_id=CFG["store"]["access_key"],
    aws_secret_access_key=CFG["store"]["secret_key"],
    endpoint_url=f"http://{CFG['store']['endpoint']}",
)


if not os.path.exists(TEST_DIR):
    os.mkdir(TEST_DIR)

if not os.path.exists(DOWNLOADS_DIR):
    os.mkdir(DOWNLOADS_DIR)

if not os.path.exists(MINIO_DIR):
    os.mkdir(MINIO_DIR)


def upload_file(name):
    """Uploads a file using the iota CLI tool."""
    res = subprocess.run(["iota", "cp", name, f"iota://{name}"], check=True)
    if res.stderr:
        print(res.stderr)


def download_file(src, dst):
    """Downloads a file using the iota CLI tool."""
    res = subprocess.run(["iota", "cp", f"iota://{src}", dst], check=True)
    if res.stderr:
        print(res.stderr)


def chunked_reader(name):
    """Reads a file in chunks."""
    with open(name, "rb") as src:
        for chunk in iter(lambda: src.read(4096), b""):
            yield chunk


def assemble_file(names):
    """
    Concatenates several files in the data dir into one file. Returns the path to the
    new file and its md5 hex-encoded checksum.
    """
    md5 = hashlib.md5()
    filename = ''.join([name.split('-')[-1] for name in names])
    fpath = os.path.join(tempfile.gettempdir(), filename)
    with open(fpath, "wb") as dst:
        for name in names:
            for chunk in chunked_reader(os.path.join(DATA_DIR, name)):
                md5.update(chunk)
                dst.write(chunk)

    return fpath, md5.digest().hex()


def check_pack_sizes():
    """
    Checks that the size of each packfile in the S3 store matches the corresponding size
    recorded in the database.
    """
    conn = sqlite3.connect(DBNAME)
    c = conn.cursor()
    for row in c.execute("SELECT lower(hex(sum)), size FROM packs"):
        checksum, size = row
        resp = s3.head_object(Bucket=BUCKET, Key=f"{checksum}.pack")
        length = resp["ContentLength"]
        if length != size:
            raise ValueError(f"pack {checksum}: expected size {size} but actual size is {length}")


def check_pack_checksums():
    """
    Checks that the checksum of each packfile in the S3 store matches the corresponging
    checksum stored in the database.
    """
    conn = sqlite3.connect(DBNAME)
    c = conn.cursor()
    for row in c.execute("SELECT lower(hex(sum)) FROM packs"):
        checksum = row[0]
        res = s3.get_object(Bucket=BUCKET, Key=f"{checksum}.pack")
        body = res["Body"]
        h = blake3.blake3()
        for chunk in iter(lambda: body.read(4096), b""):
            h.update(chunk)
        
        c = h.hexdigest()
        if c != checksum:
            raise ValueError("pack {checksum}: checksum {c} does not match")


def main():
    base_files = os.listdir(DATA_DIR)

    # Keep track of files we have uploaded
    uploaded = []

    # Upload files
    for i in range(5):
        files = random.choices(base_files, k=10)
        name, checksum = assemble_file(files)
        upload_file(name)
        os.remove(name)
        uploaded.append((name, checksum))
 
    # Download files
    for name, checksum in uploaded:
        dst = os.path.join(DOWNLOADS_DIR, os.path.basename(name))
        download_file(src=name, dst=dst)

        md5 = hashlib.md5()
        for chunk in chunked_reader(dst):
            md5.update(chunk)
        
        os.remove(dst)
    
    # Validation checks
    check_pack_sizes()
    check_pack_checksums()


def setup():
    """Starts the minio & iotafs servers."""
    minio_p = subprocess.Popen(["./bin/minio", "server", "--quiet", "--address", CFG["store"]["endpoint"], MINIO_DIR])
    s3.create_bucket(Bucket=BUCKET)
    iotafs_p = subprocess.Popen(["./bin/iotafs", "-config", "config.toml", "-db", DBNAME])
    return [minio_p, iotafs_p]


if __name__ == "__main__":
    try:
        processes = setup()
        main()
        shutil.rmtree(TEST_DIR)
    finally:
        for p in processes:
            p.kill()

