#!/usr/bin/env python3

import sys, csv, signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

HRRP_MARK = "HRRP"
POS_MARK  = "POS"

out = csv.writer(sys.stdout, lineterminator="\n")

HEADER = [
    "Facility ID",
    "Measure Name",
    "Excess Readmission Ratio",
    "Number of Readmissions",
    "FIPS_STATE_CD",
    "FIPS_CNTY_CD",
    "FIPS",
    "CRTFD_BED_CNT",
    "BED_CNT",
    "RN_CNT",
    "ICU",
    "ICU_SRVC_CD",
]

header_written = False

current_key = None
hrrp_rows = []  # list[(measure, err, readm)]
pos_rows  = []  # list[dict]


"""Pick the first non-empty for each POS field across all rows of a provider as the data is sparse in places"""
def choose_best(pos_rows):
    fields = ["FIPS_STATE_CD","FIPS_CNTY_CD","CRTFD_BED_CNT","BED_CNT","RN_CNT","ICU","ICU_SRVC_CD"]
    best = {f: "" for f in fields}
    for r in pos_rows:
        for f in fields:
            if not best[f] and r.get(f):
                best[f] = r[f]
    return best

def flush():
    global header_written, hrrp_rows, pos_rows
    if not hrrp_rows or not pos_rows:
        hrrp_rows.clear()
        pos_rows.clear()
        return

    pos_best = choose_best(pos_rows)
    fs = (pos_best["FIPS_STATE_CD"] or "")
    fc = (pos_best["FIPS_CNTY_CD"]  or "")
    fips = fs + fc

    if not header_written:
        out.writerow(HEADER)
        header_written = True

    for meas, err, readm in hrrp_rows:
        out.writerow([
            current_key,
            meas,
            err,
            readm,
            fs,
            fc,
            fips,
            pos_best["CRTFD_BED_CNT"],
            pos_best["BED_CNT"],
            pos_best["RN_CNT"],
            pos_best["ICU"],
            pos_best["ICU_SRVC_CD"],
        ])

    hrrp_rows.clear()
    pos_rows.clear()

    # Schema:  POS | fs | fc | crtfd_bed | bed_cnt | rn_cnt | icu | icu_cd

def parse_pos(payload):
    parts = payload.split("|")
    # parts[0] == POS
    fs        = parts[1] if len(parts) > 1 else ""
    fc        = parts[2] if len(parts) > 2 else ""
    crtfd_bed = parts[3] if len(parts) > 3 else ""
    bed_cnt   = parts[4] if len(parts) > 4 else ""
    rn_cnt    = parts[5] if len(parts) > 5 else ""
    icu       = parts[6] if len(parts) > 6 else ""
    icu_cd    = parts[7] if len(parts) > 7 else ""
    return {
        "FIPS_STATE_CD": fs,
        "FIPS_CNTY_CD":  fc,
        "CRTFD_BED_CNT": crtfd_bed,
        "BED_CNT":       bed_cnt,
        "RN_CNT":        rn_cnt,
        "ICU":           icu,
        "ICU_SRVC_CD":   icu_cd,
    }

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue
    try:
        key, payload = line.split("\t", 1)
    except ValueError:
        continue

    if current_key is None:
        current_key = key

    if key != current_key:
        flush()
        current_key = key

        # Schema: HRRP | measure | err | readmit

    if payload.startswith(HRRP_MARK + "|"):
        parts = payload.split("|", 3)

        meas  = parts[1] if len(parts) > 1 else ""
        err   = parts[2] if len(parts) > 2 else ""
        readm = parts[3] if len(parts) > 3 else ""
        hrrp_rows.append((meas, err, readm))
    elif payload.startswith(POS_MARK + "|"):
        pos_rows.append(parse_pos(payload))

# flush last key
flush()
