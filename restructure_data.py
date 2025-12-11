import os
import shutil

# ============================================================
# CONFIGURATION
# ============================================================
# SOURCE_DIR = "/home/caypro/Documents/real_estate_project/real_estate_project/server_data"
# DEST_PARENT_DIR = "/home/caypro/Documents/real_estate_project/real_estate_project/restructured_data"

SOURCE_DIR = "/home/caypro/Documents/real_estate_existing_data_handler/server_data"
DEST_PARENT_DIR = "/home/caypro/Documents/real_estate_existing_data_handler/restructured_data"
BATCH_SIZE = 2000

# constant english district value required by you
CHRDISTRICT_ENGLISH_CONST = "Mumbai District"

# ============================================================
# MAPPING DICTIONARIES (extend as needed)
# - Only mapped values are transformed, others remain unchanged
# ============================================================
DISTRICT_MAP = {
    "मुंबई_जिल्हा": "मुंबई जिल्हा",
}

SRO_MAP = {
    "Joint_S.R._Mumbai_2_(Mumbai_City_2_(Worli))": "Joint S.R. Mumbai 2 (Mumbai City 2 (Worli))",
    "Joint_S.R._Mumbai_1_(Mumbai_City_1_(Fort))": "Joint S.R. Mumbai 1 (Mumbai City 1 (Fort))",
    "Joint_S.R._Mumbai_3_(Joint_Sub-Registrar_Mumbai_City_3)":"Joint S.R. Mumbai 3 (Joint Sub-Registrar Mumbai City 3)"
    # add more mappings here...
}

# ============================================================
# UTILITIES
# ============================================================
def sql_escape(val):
    """Escape single quotes for SQL string literals and return 'NULL' for None."""
    if val is None:
        return "NULL"
    # ensure val is string
    s = str(val)
    s = s.replace("'", "''")
    return f"'{s}'"

def map_district(d):
    return DISTRICT_MAP.get(d, d)

def map_sro(s):
    return SRO_MAP.get(s, s)

# ============================================================
# PARSE FOLDER NAME
# ============================================================
def parse_folder_name(folder_name):
    parts = folder_name.split('_')

    # iSarita 2.0 special case
    if len(parts) > 2 and parts[0] == 'iSarita' and parts[1] == '2.0':
        reg_type = "iSarita_2.0"
        remaining_parts = parts[2:]
    else:
        reg_type = parts[0]
        remaining_parts = parts[1:]

    if len(remaining_parts) < 4:
        return None

    dist = f"{remaining_parts[0]}_{remaining_parts[1]}"
    year = remaining_parts[-2]

    try:
        doc_no = int(remaining_parts[-1])
    except:
        return None

    sro = "_".join(remaining_parts[2:-2])

    return {
        "original_name": folder_name,
        "reg_type": reg_type,
        "dist": dist,
        "sro": sro,
        "year": year,
        "doc_no": doc_no
    }

# ============================================================
# GENERATE SQL
# ============================================================
def generate_sql(processed_data):
    """
    Generates two lists of INSERT SQL statements:
    - summary_rows -> tblongoingdocno
    - record_rows  -> tblongoingdocnorecords
    """
    # 1. Organize data by batch (sub_dir_id)
    batches = {}
    for item in processed_data:
        bid = item['sub_dir_id']
        if bid not in batches:
            batches[bid] = {
                "items": {},  # doc_no -> item
                "meta": {     # Common metadata for the batch
                    "dist": item["dist"],
                    "sro": item["sro"],
                    "year": item["year"],
                    "range_min": item["range_min"],
                    "range_max": item["range_max"]
                }
            }
        batches[bid]["items"][item["doc_no"]] = item

    summary_rows = []
    record_rows = []

    # 2. Iterate through each batch and fill gaps
    for bid in sorted(batches.keys()):
        batch_data = batches[bid]
        meta = batch_data["meta"]
        items_map = batch_data["items"]
        
        range_min = meta["range_min"]
        range_max = meta["range_max"]

        # Prepare summary row (tblongoingdocno)
        # Note: bucket logic is already fixed by range_min/max from the batch logic
        chrdistrict_mapped = map_district(meta["dist"])
        chrsro_mapped = map_sro(meta["sro"])

        summary_sql = (
            "INSERT INTO tblongoingdocno "
            "(intongoingid, chrdistrict, chrsro, intstartrange, intendrange, chryear, chrip, enmstatus) VALUES ("
            f"{bid}, "
            f"{sql_escape(chrdistrict_mapped)}, "
            f"{sql_escape(chrsro_mapped)}, "
            f"{range_min}, {range_max}, "
            f"{sql_escape(meta['year'])}, "
            f"NULL, "
            f"{sql_escape('Completed Webhook Recieved')}"
            ");"
        )
        summary_rows.append(summary_sql)

        # Prepare record rows (tblongoingdocnorecords)
        # Iterate through the FULL range
        for doc_no in range(range_min, range_max + 1):
            
            if doc_no in items_map:
                # Case 1: Document Exists
                item = items_map[doc_no]
                
                # chrregistrationtype logic for EXISTING items
                if item['reg_type'] == "iSarita_2.0":
                    chrregistrationtype_val = "iSarita 2.0"
                    crawling_status_val = "Not Found" # Wait, original code said Not Found for iSarita 2.0? 
                    # Checking original code: 
                    # if item['reg_type'] == "iSarita_2.0":
                    #     chrregistrationtype_val = "iSarita 2.0"
                    #     crawling_status_val = "Not Found"
                    # else: ... crawling_status_val = "Found"
                    # Yes, that was the logic.
                else:
                    chrregistrationtype_val = item['reg_type']
                    crawling_status_val = "Found"
                
                dist_val = item['dist']
                sro_val = item['sro']
                year_val = item['year']
                
            else:
                # Case 2: Document Missing (Gap)
                # User Instruction: "additionally add insert query for the intdocno 4 and it will be 'Not Found'"
                # User Instruction: "chrregistrationtype= 'iSarita 2.0'"
                
                chrregistrationtype_val = "iSarita 2.0"
                crawling_status_val = "Not Found"
                
                # Use batch metadata
                dist_val = meta['dist']
                sro_val = meta['sro']
                year_val = meta['year']

            # Common fields
            chrdistrict_mapped = map_district(dist_val)
            chrsro_mapped = map_sro(sro_val)
            chrdistrictenglish_val = CHRDISTRICT_ENGLISH_CONST
            extraction_status_val = "Pending"
            text_htmlpath_val = None
            text_jsonpath_val = None
            text_error_val = None

            record_sql = (
                "INSERT INTO tblongoingdocnorecords "
                "(intongoingid, chrdistrict, chrdistrictenglish, chrregistrationtype, chrsro, intdocno, chryear, intstartrange, intendrange, crawling_status, extraction_status, text_htmlpath, text_jsonpath, text_error) VALUES ("
                f"{bid}, "
                f"{sql_escape(chrdistrict_mapped)}, "
                f"{sql_escape(chrdistrictenglish_val)}, "
                f"{sql_escape(chrregistrationtype_val)}, "
                f"{sql_escape(chrsro_mapped)}, "
                f"{doc_no}, "
                f"{sql_escape(year_val)}, "
                f"{range_min}, {range_max}, "
                f"{sql_escape(crawling_status_val)}, "
                f"{sql_escape(extraction_status_val)}, "
                f"{sql_escape(text_htmlpath_val)}, "
                f"{sql_escape(text_jsonpath_val)}, "
                f"{sql_escape(text_error_val)}"
                ");"
            )
            record_rows.append(record_sql)

    return summary_rows, record_rows

# ============================================================
# MAIN PROCESS
# ============================================================
def main():
    print("Starting restructure...")

    # sanity check source exists
    if not os.path.exists(SOURCE_DIR):
        print(f"Source directory not found: {SOURCE_DIR}")
        return

    # prompt and remove dest if present
    if os.path.exists(DEST_PARENT_DIR):
        ans = input("Destination exists, delete? (y/n): ")
        if ans.lower().strip() == 'y':
            shutil.rmtree(DEST_PARENT_DIR)
        else:
            print("Abort.")
            return

    os.makedirs(DEST_PARENT_DIR, exist_ok=True)

    items = os.listdir(SOURCE_DIR)

    parsed_items = []
    skipped = 0
    for folder in items:
        full = os.path.join(SOURCE_DIR, folder)
        if not os.path.isdir(full):
            continue
        meta = parse_folder_name(folder)
        if meta:
            parsed_items.append(meta)
        else:
            skipped += 1

    print(f"Found {len(parsed_items)} valid folders. Skipped {skipped} invalid items.")

    # GROUP KEY = (dist, sro, year)
    groups = {}
    for item in parsed_items:
        key = (item["dist"], item["sro"], item["year"])
        groups.setdefault(key, []).append(item)

    batch_map = {}
    processed = []
    global_batch_id = 0
    copy_count = 0

    for key, lst in groups.items():
        lst.sort(key=lambda x: x["doc_no"])

        for item in lst:
            doc_no = item["doc_no"]
            logical = (doc_no - 1) // BATCH_SIZE
            map_key = (key, logical)

            if map_key not in batch_map:
                global_batch_id += 1
                batch_map[map_key] = global_batch_id
                os.makedirs(os.path.join(DEST_PARENT_DIR, str(global_batch_id)), exist_ok=True)

            bid = batch_map[map_key]
            item['sub_dir_id'] = bid

            # compute 2000-range boundaries
            range_min = logical * BATCH_SIZE + 1
            range_max = (logical + 1) * BATCH_SIZE
            item['range_min'] = range_min
            item['range_max'] = range_max

            # copy folder safely (skip if already exists)
            sp = os.path.join(SOURCE_DIR, item['original_name'])
            dp = os.path.join(DEST_PARENT_DIR, str(bid), item['original_name'])
            try:
                if not os.path.exists(dp):
                    shutil.copytree(sp, dp)
                    copy_count += 1
            except Exception as e:
                print(f"Error copying {item['original_name']}: {e}")

            processed.append(item)

    # generate SQL
    summary_sql, detail_sql = generate_sql(processed)

    out_sql_path = os.path.abspath("migration.sql")
    with open(out_sql_path, "w", encoding="utf-8") as f:
        f.write("-- Summary Table Inserts (tblongoingdocno)\n")
        f.write("\n".join(summary_sql))
        f.write("\n\n-- Detail Table Inserts (tblongoingdocnorecords)\n")
        f.write("\n".join(detail_sql))

    # ============================================================
    # VERIFICATION BLOCK
    # ============================================================
    print("\nRunning Verification...\n")

    batch_to_types = {}
    for item in processed:
        bid = item['sub_dir_id']
        batch_to_types.setdefault(bid, set()).add(item['reg_type'])

    # FIND BATCHES WITH ONLY eRegistration
    only_ereg_batches = [bid for bid, types in batch_to_types.items()
                         if types == {"eRegistration"}]

    # FIND BATCHES CONTAINING iSarita_2.0
    isarita_batches = [bid for bid, types in batch_to_types.items()
                       if "iSarita_2.0" in types]

    print("----- Verification Report -----")

    print(f"\nTotal Batches with ONLY eRegistration (Missing Regular): {len(only_ereg_batches)}")
    if only_ereg_batches:
        print("Batch IDs:", sorted(only_ereg_batches))
    else:
        print("All batches containing eRegistration also contain Regular files.")

    print(f"\nTotal Batches containing iSarita_2.0: {len(isarita_batches)}")
    if isarita_batches:
        print("Batch IDs:", sorted(isarita_batches))
    else:
        print("No batches contain iSarita_2.0 files.")

    # final stats
    print("\nCompleted.")
    print(f"Total batches created: {global_batch_id}")
    print(f"Total folders copied: {copy_count}")
    print(f"SQL written to: {out_sql_path}")


if __name__ == "__main__":
    main()