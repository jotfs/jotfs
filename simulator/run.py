import subprocess
import os
import hashlib
import random
import tempfile


DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(DIR, "data")
DOWNLOADS_DIR = os.path.join(DIR, "downloads")

if not os.path.exists(DOWNLOADS_DIR):
    os.mkdir(DOWNLOADS_DIR)


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


if __name__ == "__main__":
    main()

