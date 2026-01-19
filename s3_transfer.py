import os
import re
import subprocess
import time

import logging
import datetime
import traceback


DISTRICT_MAP = {
    "मुंबई_जिल्हा": "Mumbai_District",
}

def map_district(d):
    return DISTRICT_MAP.get(d, d)

# Import parse_folder_name from the existing script
try:
    from restructure_data import parse_folder_name
except ImportError:
    # Fallback if running from a different location or if import fails
    def parse_folder_name(folder_name):
        parts = folder_name.split('_')
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

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("s3_transfer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class S3Transfer:
    def __init__(self, bucket_name, dry_run=False):
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        # Only initialize client if not in dry_run mode to avoid credential errors during testing

    def generate_timestamped_html_name(self, cnr_no, local_file_path):
        """
        Generates a timestamped filename for HTML files.
        Format: {filename}_{timestamp}.html
        """
        filename = os.path.basename(local_file_path)
        name, ext = os.path.splitext(filename)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{name}_{timestamp}{ext}"

    def generate_timestamped_html_name(self, diary_no):
        """
        Generate timestamped HTML filename.

        Args:
            cnr_no (str): CNR number
            original_html_path (str): Path to original HTML file

        Returns:
            str: New filename with timestamp
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            new_filename = f"{diary_no}_{timestamp}.html"
            print("Generated timestamped HTML name: %s", new_filename)
            return new_filename

        except Exception as e:
            print("Error generating timestamped name for CNR %s: %s", diary_no, e)
            return f"{diary_no}.html"  # Fallback to original format

    def list_s3_files(self, s3_base_path):
        result = subprocess.run(
            ["s3cmd", "ls", s3_base_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            # Extract file names from s3cmd output
            return set(re.findall(r'\s+(\S+)$', result.stdout, re.MULTILINE))
        else:
            print("Failed to list S3 directory: %s", result.stderr)
            return set()


    def transfer_file_to_s3(self, local_file_path, s3_destination_path, max_retries=3):
        """
        Transfer a single file to S3 with retry mechanism.

        Args:
            local_file_path (str): Local file path
            s3_destination_path (str): S3 destination path
            max_retries (int): Maximum retry attempts

        Returns:
            bool: True if successful, False otherwise
        """
        for attempt in range(1, max_retries + 1):
            try:
                print("Attempt %d: Transferring %s to %s",
                                  attempt, local_file_path, s3_destination_path)

                result = subprocess.run([
                    "s3cmd", "put", local_file_path, s3_destination_path
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                if result.returncode == 0:
                    print("Successfully transferred file on attempt %d", attempt)
                    return True
                else:
                    print("Transfer failed on attempt %d: %s", attempt, result.stderr)

            except Exception as e:
                print("Transfer attempt %d failed with exception: %s", attempt, e)

            if attempt < max_retries:
                time.sleep(2)  # Wait before retry

        print("Failed to transfer file after %d attempts: %s", max_retries, local_file_path)
        return False


    def transfer_folder_to_s3(self, folder_path, diary_no, do_id):
        """
        Transfer entire folder to S3 with proper structure and verification.

        Args:
            folder_path (str): Local folder path to transfer
            cnr_no (str): CNR number
            do_id (str): Data Order ID

        Returns:
            tuple: (success: bool, s3_base_path: str, s3_html_file_path: str)
        """
        try:
            folder_name = os.path.basename(folder_path)

            # Parse components to get district, sro, year
            # We use the folder name itself to parse metadata
            meta = parse_folder_name(folder_name)
            if not meta:
                logger.error("Failed to parse folder name components for: %s", folder_name)
                return False, None, None, None

            # district = meta['dist']
            district = map_district(meta["dist"])
            sro = meta['sro']
            year = meta['year']


            s3_base_path = f"s3://calyso/marshaltestrealestate/{district}/{sro}/{year}/{do_id}/{folder_name}/"
            print("Starting S3 transfer for CNR %s to path: %s", diary_no, s3_base_path)

            # Get all files in the folder
            if not os.path.exists(folder_path):
                print("Source folder does not exist: %s", folder_path)
                return False, None, None

            files_to_transfer = []
            s3_html_file_path = None



            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, folder_path)

                    s3_file_path = s3_base_path + relative_path.replace(os.sep, '/')

                    files_to_transfer.append((local_file_path, s3_file_path))

            print("Found %d files to transfer for CNR %s", len(files_to_transfer), diary_no)

            # Transfer each file with retry mechanism
            successful_transfers = 0
            failed_files = []

            for local_file, s3_destination in files_to_transfer:
                if self.transfer_file_to_s3(local_file, s3_destination):
                    successful_transfers += 1
                else:
                    failed_files.append(local_file)

            # Get base S3 directory (remove file name from path)
            s3_base_path = os.path.dirname(files_to_transfer[0][1]) + "/"
            s3_files = self.list_s3_files(s3_base_path)

            verification_failures = []
            for local_file, s3_destination in files_to_transfer:
                print(f"local_file: {local_file}")
                print(f"s3_destination: {s3_destination}")
                if local_file not in failed_files:
                    s3_file_name = s3_destination
                    if s3_file_name not in s3_files:
                        verification_failures.append(s3_file_name)

            # Check if transfer was successful
            total_files = len(files_to_transfer)
            if successful_transfers == total_files and len(verification_failures) == 0:
                print("Successfully transferred and verified all %d files for CNR %s",
                                 total_files, diary_no)
                return True, s3_base_path, s3_html_file_path
            else:
                print("Transfer incomplete for CNR %s: %d/%d successful, %d verification failures",
                                  diary_no, successful_transfers, total_files, len(verification_failures))
                print("Failed files: %s", failed_files)
                print("Verification failures: %s", verification_failures)
                return False, s3_base_path, s3_html_file_path

        except Exception as e:
            print("Error in S3 transfer for CNR %s: %s", diary_no, e)
            print(traceback.format_exc())
            return False, None, None


def main():
    SOURCE_DIR = "/home/caypro/Documents/real_estate_existing_data_handler/ongoing"
    BUCKET_NAME = "calyso"
    
    if not os.path.exists(SOURCE_DIR):
        print(f"Source directory not found: {SOURCE_DIR}")
        return

    uploader = S3Transfer(BUCKET_NAME)
    
    # Iterate through do_id folders (1, 2, 3...)
    try:
        do_ids = sorted([d for d in os.listdir(SOURCE_DIR) if os.path.isdir(os.path.join(SOURCE_DIR, d))], key=lambda x: int(x) if x.isdigit() else 999999)
    except ValueError:
        do_ids = [d for d in os.listdir(SOURCE_DIR) if os.path.isdir(os.path.join(SOURCE_DIR, d))]

    print(f"Found {len(do_ids)} batches (do_ids) to process.")

    for do_id in do_ids:
        do_id_path = os.path.join(SOURCE_DIR, do_id)
        print(f"\nProcessing Batch (do_id): {do_id}")

        # Iterate through case folders inside the do_id folder
        case_folders = os.listdir(do_id_path)

        for case_folder in case_folders:
            case_path = os.path.join(do_id_path, case_folder)
            if not os.path.isdir(case_path):
                continue

            # Use the folder name as cnr_no/case identifier for logging
            cnr_no = case_folder

            success, s3_path, html_path = uploader.transfer_folder_to_s3(case_path, cnr_no, do_id)

            if success:
                print(f"[SUCCESS] {case_folder} -> {s3_path}")
            else:
                print(f"[FAILED] {case_folder}")

if __name__ == "__main__":
    main()