import os
import sys
import csv
import hashlib
import multiprocessing as mp
import tempfile

APPID_COL = 1
PLAY_COL = 8
OUTPUT_FILE = "result.csv"

def split_file_ranges(path, num_chunks):
    size = os.path.getsize(path)
    if size == 0 or num_chunks <= 1:
        return [(0, size)]
    chunk = size // num_chunks
    out = []
    with open(path, "rb") as f:
        start = 0
        for i in range(num_chunks):
            if i == num_chunks - 1:
                out.append((start, size))
                break
            f.seek(start + chunk)
            f.readline()
            next_start = f.tell()
            out.append((start, next_start))
            start = next_start
    return out

def parse_csv_line(line):
    return next(csv.reader([line]))

def mapper_worker(path, byte_range, mapper_id, tmpdir):
    start, end = byte_range
    out_path = os.path.join(tmpdir, f"map_{mapper_id}.tsv")
    written = 0
    with open(path, "r", encoding="utf-8", errors="ignore") as f, open(out_path, "w", encoding="utf-8") as out:
        f.seek(start)
        if start == 0:
            f.readline()
        pos = f.tell()
        while pos < end:
            line = f.readline()
            if not line:
                break
            pos = f.tell()
            try:
                parts = parse_csv_line(line.rstrip("\n"))
            except Exception:
                continue

            if len(parts) <= PLAY_COL:
                continue

            game = parts[2].strip()
            play = parts[PLAY_COL].strip()
            if not game:
                continue
            try:
                play_f = float(play)
            except:
                continue
            if play_f < 0:
                continue

            out.write(f"{game}	{play_f}\n")
            written += 1
    return out_path

def shuffle_phase(map_outputs, reducers, tmpdir):
    import hashlib
    bucket_paths = [os.path.join(tmpdir, f"bucket_{i}.tsv") for i in range(reducers)]
    buckets = [open(p, "w", encoding="utf-8") for p in bucket_paths]

    try:
        for mp_out in map_outputs:
            with open(mp_out, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    parts = line.split("	", 1)
                    if len(parts) != 2:
                        continue
                    game, play = parts
                    h = int(hashlib.md5(game.encode("utf-8")).hexdigest(), 16) % reducers
                    buckets[h].write(f"{game}	{play}\n")
    finally:
        for b in buckets:
            b.close()

    return bucket_paths

def reducer_worker(bucket_path, rid, outdir):
    agg = {}
    with open(bucket_path, "r", encoding="utf-8") as f:
        for line in f:
            game, play = line.rstrip("\n").split("\t")
            play_f = float(play)
            s, c = agg.get(game, (0.0, 0))
            agg[game] = (s + play_f, c + 1)

    out_path = os.path.join(outdir, f"reduced_{rid}.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["game", "avg_playtime", "total_playtime", "count"])
        for game, (s, c) in agg.items():
            w.writerow([game, s / c if c else 0.0, s, c])
    return out_path

def merge_outputs(reducer_csvs, final_path):
    with open(final_path, "w", encoding="utf-8", newline="") as out:
        w = None
        for i, path in enumerate(reducer_csvs):
            with open(path, "r", encoding="utf-8") as f:
                header = f.readline()
                if i == 0:
                    out.write(header)
                for line in f:
                    out.write(line)

def main():


    input_path = sys.argv[1]
    mappers = 4
    reducers = 2

    tmpdir = tempfile.mkdtemp(prefix="mapred_")
    print("Temp dir:", tmpdir)

    ranges = split_file_ranges(input_path, mappers)

    with mp.Pool(mappers) as pool:
        jobs = [pool.apply_async(mapper_worker, (input_path, r, i, tmpdir)) for i, r in enumerate(ranges)]
        map_outputs = [j.get() for j in jobs]

    buckets = shuffle_phase(map_outputs, reducers, tmpdir)

    red_dir = os.path.join(tmpdir, "reducers")
    os.makedirs(red_dir, exist_ok=True)

    with mp.Pool(reducers) as pool:
        jobs = [pool.apply_async(reducer_worker, (buckets[i], i, red_dir)) for i in range(reducers)]
        reducer_csvs = [j.get() for j in jobs]

    merge_outputs(reducer_csvs, OUTPUT_FILE)

if __name__ == "__main__":
    main()
