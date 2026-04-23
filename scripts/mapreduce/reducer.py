import sys


def main():
    current_key = None
    emitted = False

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        key = parts[0]

        if key != current_key:
            current_key = key
            emitted = False

        if not emitted:
            print(line)
            emitted = True


if __name__ == "__main__":
    main()
