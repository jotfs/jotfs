import subprocess
import os
import hashlib
import random
import sqlite3
import time
import shutil
import re
import argparse

import boto3
import blake3


DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(DIR, "data")
TEST_DIR = os.path.join(DIR, f"test-{int(time.time() * 1000)}")
DOWNLOADS_DIR = os.path.join(TEST_DIR, "downloads")
MINIO_DIR = os.path.join(TEST_DIR, "minio")
FILES_DIR = os.path.join(TEST_DIR, "files")

STORE_SECRET_KEY = "minioadmin"
STORE_ACCESS_KEY = "minioadmin"
BUCKET = "jotfs-test"
STORE_ENDPOINT = "localhost:9004"
DBNAME = os.path.join(TEST_DIR, "jotfs.db")
PORT = 6776
TLS_CERT = "server.test.crt"
TLS_KEY = "server.test.key"

session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    aws_access_key_id=STORE_ACCESS_KEY,
    aws_secret_access_key=STORE_SECRET_KEY,
    endpoint_url=f"http://{STORE_ENDPOINT}",
)

if not os.path.exists(TEST_DIR):
    os.mkdir(TEST_DIR)

if not os.path.exists(DOWNLOADS_DIR):
    os.mkdir(DOWNLOADS_DIR)

if not os.path.exists(MINIO_DIR):
    os.mkdir(MINIO_DIR)

if not os.path.exists(FILES_DIR):
    os.mkdir(FILES_DIR)


cmd_preamble = ["./bin/jot", "--tls_skip_verify", "--endpoint", f"https://localhost:{PORT}"]


def upload_file(name):
    """Uploads a file using the jot CLI tool."""
    subprocess.check_output(cmd_preamble + ["cp", name, f"jot://{name}"])


def download_file(src, dst):
    """Downloads a file using the jot CLI tool."""
    subprocess.check_output(cmd_preamble + ["cp", f"jot://{src}", dst])

def delete_file(name):
    """Deletes a file using the jot CLI tool."""
    subprocess.check_output(cmd_preamble + ["rm", name])

def vacuum():
    """
    Runs a manual vacuum on the server using the jot CLI tool. Returns the vacuum ID.
    """
    out = subprocess.check_output(cmd_preamble + ["admin", "start-vacuum"])
    out = out.decode()
    m = re.match(r'^vacuum ([a-zA-Z0-9]+)', out)
    if m:
        return m.group(1)
    raise ValueError(f"unable to find vacuum ID in output: {out}")

def vacuum_status(vac_id):
    """Gets the status of a vacuum"""
    out = subprocess.check_output(cmd_preamble + ["admin", "vacuum-status", "--id", vac_id])
    out = out.decode()
    return out.split(' ')[0].strip()


def server_stats():
    """Gets the server stats."""
    out = subprocess.check_output(cmd_preamble + ["admin", "stats"])
    return out.decode()


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
    fpath = os.path.join(FILES_DIR, filename)
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


def download_and_validate_checksum(name, checksum):
    """Downloads a file and validates its MD5 checksum."""
    dst = os.path.join(DOWNLOADS_DIR, os.path.basename(name))
    download_file(src=name, dst=dst)
    md5 = hashlib.md5()
    for chunk in chunked_reader(dst):
        md5.update(chunk)
    dl_checksum = md5.digest().hex()
    if dl_checksum != checksum:
        raise ValueError(f"expected checksum {checksum} but received {dl_checksum}")
    os.remove(dst)


def run(n):
    """Run the tests with n files."""
    base_files = os.listdir(DATA_DIR)

    # Keep track of files we have uploaded
    uploaded = []

    # Upload files
    for _ in range(n):
        files = random.choices(base_files, k=random.randint(1, 15))
        name, checksum = assemble_file(files)
        upload_file(name)
        os.remove(name)
        uploaded.append((name, checksum))

    # Download files
    for name, checksum in uploaded:
        download_and_validate_checksum(name, checksum)

    # Validation checks
    check_pack_sizes()
    check_pack_checksums()

    print(server_stats())

    # Delete all files except for the last two. This should force the vacuum to rebalance
    # some packfiles.
    to_delete = uploaded[:-2]
    remaining = uploaded[-2:]
    for name, _ in to_delete:
        delete_file(name)

    # Run a vacuum and wait for it to complete
    vacuum_id = vacuum()
    status = None
    for _ in range(10):
        status = vacuum_status(vacuum_id)
        if status != "RUNNING":
            break
        time.sleep(1)
    if status != "SUCCEEDED":
        raise ValueError(f"vacuum failed {status}")

    # Check that the remaining files can still be downloaded after the vacuum
    for name, checksum in remaining:
        download_and_validate_checksum(name, checksum)


def setup():
    """Start the JotFS server."""
    processes = []
    try:
        s3.create_bucket(Bucket=BUCKET)
        jotfs_p = subprocess.Popen([
            "./bin/jotfs",
            "-db", DBNAME,
            "-port", str(PORT),
            "-store_bucket", BUCKET,
            "-store_access_key", STORE_ACCESS_KEY,
            "-store_secret_key", STORE_SECRET_KEY,
            "-store_endpoint", STORE_ENDPOINT,
            "-tls_cert", TLS_CERT,
            "-tls_key", TLS_KEY,
            "-store_region", "us-east-1",
            "-debug", "-store_path_style", "-store_disable_ssl"
        ])
        processes.append(jotfs_p)
        return processes
    except Exception as e:
        for p in processes:
            p.kill()
        raise e


def main():
    parser = argparse.ArgumentParser(description="execute JotFS integration tests")
    parser.add_argument("-n", type=int, help="number of files to generate", default=10)
    parser.add_argument("--seed", type=int, help="random number generator seed", default=1)
    args = parser.parse_args()

    random.seed(args.seed)
    print(f"Seed = {args.seed}")
    print(f"n = {args.n}")

    processes = []
    try:
        processes = setup()
        run(args.n)
        shutil.rmtree(TEST_DIR)
    finally:
        for p in processes:
            p.kill()


if __name__ == "__main__":
    main()
