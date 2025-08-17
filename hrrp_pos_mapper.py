#!/usr/bin/env python3

import sys, csv, signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

HRRP_MARK = "HRRP"
POS_MARK  = "POS"

def zpad(s, width):
    s = (s or "").strip()
    return s.zfill(width) if s else ""

def is_hrrp_header(cols):
    s = set(c.strip() for c in cols)
    return {"Facility ID","Measure Name","Excess Readmission Ratio","Number of Readmissions"}.issubset(s)

def is_pos_header(cols):
    s = set(c.strip() for c in cols)
    return {"PRVDR_NUM","PRVDR_CTGRY_CD","FIPS_STATE_CD","FIPS_CNTY_CD"}.issubset(s)

def index_map(cols):
    return {c.strip(): i for i, c in enumerate(cols)}

reader = csv.reader(sys.stdin)
current = None   # "HRRP" or "POS"
idx = {}

for row in reader:
    if not row:
        continue

    # Detect headers anywhere
    if is_hrrp_header(row):
        current = "HRRP"
        idx = index_map(row)
        continue
    if is_pos_header(row):
        current = "POS"
        idx = index_map(row)
        continue
    if current is None:
        continue

    try:
        if current == "HRRP":
            fid   = row[idx["Facility ID"]].strip()
            meas  = row[idx["Measure Name"]].strip()
            err   = row[idx["Excess Readmission Ratio"]].strip()
            readm = row[idx["Number of Readmissions"]].strip()
            if not fid:
                continue
            # key , tab,  HRRP | measure | err | readmits
            print(f"{fid}\t{HRRP_MARK}|{meas}|{err}|{readm}")

        elif current == "POS":
            prvdr_num = row[idx["PRVDR_NUM"]].strip()
            prv_cat   = row[idx["PRVDR_CTGRY_CD"]].strip()
            if prv_cat != "01":  # short-term acute care only
                continue
            fs = zpad(row[idx["FIPS_STATE_CD"]].strip() if "FIPS_STATE_CD" in idx else "", 2)
            fc = zpad(row[idx["FIPS_CNTY_CD"]].strip()  if "FIPS_CNTY_CD"  in idx else "", 3)

            bed_cnt = ""
            if "BED_CNT" in idx:
                bed_cnt = row[idx["BED_CNT"]].strip()
            elif "BEDS_CNT" in idx:
                bed_cnt = row[idx["BEDS_CNT"]].strip()

            crtfd_bed = row[idx["CRTFD_BED_CNT"]].strip() if "CRTFD_BED_CNT" in idx else ""
            rn_cnt    = row[idx["RN_CNT"]].strip()        if "RN_CNT"        in idx else ""
            icu       = row[idx["ICU"]].strip()           if "ICU"           in idx else ""
            icu_cd    = ""
            if "ICU_SRVC_CD" in idx:
                icu_cd = row[idx["ICU_SRVC_CD"]].strip()
            elif "ICU_SRVC_IND" in idx:
                icu_cd = row[idx["ICU_SRVC_IND"]].strip()

            if not prvdr_num:
                continue

            # key, tab, POS | fips_state | fips_cnty | crtfd_bed| bed_cnt | rn_cnt | icu | icu_cd
            print(f"{prvdr_num}\t{POS_MARK}|{fs}|{fc}|{crtfd_bed}|{bed_cnt}|{rn_cnt}|{icu}|{icu_cd}")

    except Exception:
        continue
