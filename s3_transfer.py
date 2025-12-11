import os
import boto3
import logging
import datetime
import traceback
from botocore.exceptions import ClientError

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
        if not self.dry_run:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = None
            logger.info("S3Transfer initialized in DRY RUN mode. No files will be uploaded.")

    def generate_timestamped_html_name(self, cnr_no, local_file_path):
        """
        Generates a timestamped filename for HTML files.
        Format: {filename}_{timestamp}.html
        """
        filename = os.path.basename(local_file_path)
        name, ext = os.path.splitext(filename)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{name}_{timestamp}{ext}"

    def transfer_file_to_s3(self, local_path, s3_path):
        """
        Uploads a single file to S3.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would upload {local_path} to {s3_path}")
            return True

        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_path)
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to {s3_path}: {e}")
            return False

    def list_s3_files(self, prefix):
        """
        Lists all files in the S3 bucket with the given prefix.
        Returns a set of S3 keys.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Would list objects with prefix {prefix}")
            # In dry run, we can't verify against S3, so we return an empty set 
            # or we could mock it. For now, returning empty set but we will skip verification failure in dry run.
            return set()

        s3_files = set()
        paginator = self.s3_client.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_files.add(obj['Key'])
        except ClientError as e:
            logger.error(f"Failed to list objects with prefix {prefix}: {e}")
        return s3_files

    def transfer_folder_to_s3(self, folder_path, cnr_no, do_id):
        """
        Transfer entire folder to S3 with proper structure and verification.
        
        Args:
            folder_path (str): Local folder path to transfer (the case folder)
            cnr_no (str): CNR number (used for logging/logic, here it maps to the case identifier)
            do_id (str): Data Order ID (the batch ID)
            
        Returns:
            tuple: (success: bool, s3_cnr_folder_path: str, s3_html_file_path: str, s3_files: list)
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
            
            # Build S3 paths
            # Structure: s3://{bucket}/marshaldc/{district}/{sro}/{year}/{do_id}/{original_folder_name}/
            # Note: Added {original_folder_name} at the end to avoid collisions within the same do_id batch
            
            s3_prefix_base = f"marshaldc/{district}/{sro}/{year}/{do_id}/{folder_name}/"
            s3_cnr_folder_path = f"s3://{self.bucket_name}/{s3_prefix_base}"
            
            logger.info("Starting S3 transfer for Case %s (do_id: %s) to path: %s", cnr_no, do_id, s3_cnr_folder_path)
            
            # Get all files in the folder
            if not os.path.exists(folder_path):
                logger.error("Source folder does not exist: %s", folder_path)
                return False, None, None, None
            
            files_to_transfer = []
            s3_html_file_path = None
            
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, folder_path)
                    
                    # Handle HTML file with timestamp
                    if file.lower().endswith('.html'):
                        timestamped_name = self.generate_timestamped_html_name(cnr_no, local_file_path)
                        # HTML goes to the base of the case folder in S3
                        s3_file_key = s3_prefix_base + timestamped_name
                        s3_html_file_path = f"s3://{self.bucket_name}/{s3_file_key}"
                    else:
                        # Other files maintain relative structure
                        s3_file_key = s3_prefix_base + relative_path.replace(os.sep, '/')
                    
                    files_to_transfer.append((local_file_path, s3_file_key))
            
            logger.debug("Found %d files to transfer for Case %s", len(files_to_transfer), cnr_no)
            
            # Transfer each file
            successful_transfers = 0
            failed_files = []
            
            for local_file, s3_key in files_to_transfer:
                if self.transfer_file_to_s3(local_file, s3_key):
                    successful_transfers += 1
                else:
                    failed_files.append(local_file)

            # Verification
            if self.dry_run:
                logger.info("[DRY RUN] Skipping verification step.")
                # In dry run, we assume success if all "transfers" succeeded
                if successful_transfers == len(files_to_transfer):
                    return True, s3_cnr_folder_path, s3_html_file_path, []
                else:
                    return False, s3_cnr_folder_path, s3_html_file_path, []

            # List files in the S3 directory we just uploaded to
            uploaded_s3_keys = self.list_s3_files(s3_prefix_base)

            verification_failures = []
            for local_file, s3_key in files_to_transfer:
                if local_file not in failed_files:
                    if s3_key not in uploaded_s3_keys:
                        verification_failures.append(s3_key)

            # Check if transfer was successful
            total_files = len(files_to_transfer)
            if successful_transfers == total_files and len(verification_failures) == 0:
                logger.info("Successfully transferred and verified all %d files for Case %s", 
                        total_files, cnr_no)
                return True, s3_cnr_folder_path, s3_html_file_path, list(uploaded_s3_keys)
            else:
                logger.error("Transfer incomplete for Case %s: %d/%d successful, %d verification failures", 
                            cnr_no, successful_transfers, total_files, len(verification_failures))
                if failed_files:
                    logger.error("Failed files: %s", failed_files)
                if verification_failures:
                    logger.error("Verification failures: %s", verification_failures)
                return False, s3_cnr_folder_path, s3_html_file_path, list(uploaded_s3_keys)
                
        except Exception as e:
            logger.error("Error in S3 transfer for Case %s: %s", cnr_no, e)
            logger.error(traceback.format_exc())
            return False, None, None, None

def main():
    SOURCE_DIR = "/home/caypro/Documents/real_estate_existing_data_handler/restructured_data"
    BUCKET_NAME = "re_bucket"
    
    if not os.path.exists(SOURCE_DIR):
        print(f"Source directory not found: {SOURCE_DIR}")
        return

    uploader = S3Transfer(BUCKET_NAME, dry_run=True)
    
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
            
            success, s3_path, html_path, s3_files = uploader.transfer_folder_to_s3(case_path, cnr_no, do_id)
            
            if success:
                print(f"[SUCCESS] {case_folder} -> {s3_path}")
            else:
                print(f"[FAILED] {case_folder}")

if __name__ == "__main__":
    main()