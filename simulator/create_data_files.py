import os
import random
import string


DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(DIR, "data")
RAND_SEED = os.environ.get("RAND_SEED", 9988)


def main():
    print(f"Generating base data files in {DATA_DIR} with random seed {RAND_SEED}:")

    kib = 1024
    mib = 1024 * kib
    sizes = {
        "data-a": 10 * kib,
        "data-b": 10 * kib,
        "data-c": 10 * kib,
        "data-d": 10 * kib,
        "data-e": 10 * mib,
        "data-f": 10 * mib,
        "data-g": 10 * mib,
        "data-h": 10 * mib,
        "data-i": 100 * mib,
    }

    random.seed(RAND_SEED)

    # chars = (string.ascii_lowercase + string.ascii_uppercase + string.digits).encode('ascii')
    chars = string.ascii_lowercase + string.ascii_uppercase + string.digits

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    for name, size in sizes.items():
        path = os.path.join(DATA_DIR, name)
        with open(path, "w") as f:
            for i in range(size):
                if i % 80 == 0:
                    f.write('\n')
                else:
                    f.write(random.choice(chars))

        print(f"  {name} ({size / kib} KiB)")



if __name__ == "__main__":
    main()