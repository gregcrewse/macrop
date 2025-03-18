import os
import imaplib
import email
from email.header import decode_header
import tempfile
import pandas as pd
import numpy as np
import logging
import yaml
import time
import json
import re
from datetime import datetime
from pathlib import Path
import shutil
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("workday_email_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('workday_email_processor')

class WorkdayEmailProcessor:
    """Process Workday export emails and append data to Google Sheets for Risk tracking."""
    
    def __init__(self, config_path):
        """Initialize with configuration."""
        self.config = self._load_config(config_path)
        
        # Create temporary directory for downloaded files
        self.temp_dir = Path(tempfile.mkdtemp())
        logger.info(f"Created temporary directory: {self.temp_dir}")
        
        # Initialize Google Sheets connection
        self._init_gsheets()
        
        # Initialize archive directory
        self._init_archive()
    
    def _load_config(self, config_path):
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info("Successfully loaded configuration")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def _init_gsheets(self):
        """Initialize Google Sheets API connection."""
        try:
            # Define the scope
            scope = ['https://spreadsheets.google.com/feeds',
                     'https://www.googleapis.com/auth/drive']
            
            # Get credentials from service account file
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.config['google_sheets']['credentials_file'], 
                scope
            )
            
            # Authorize with Google
            self.gc = gspread.authorize(credentials)
            self.google_creds = credentials
            logger.info("Successfully authenticated with Google Sheets API")
        except Exception as e:
            logger.error(f"Error connecting to Google Sheets: {e}")
            raise
    
    def _init_archive(self):
        """Initialize the archive directory or Google Drive folder."""
        try:
            # Check which archive method to use
            archive_method = self.config['archive']['method']
            
            if archive_method == 'local':
                # Create local archive directory if it doesn't exist
                archive_dir = Path(self.config['archive']['local_path'])
                archive_dir.mkdir(parents=True, exist_ok=True)
                
                # Create subdirectories for LOA and TO files
                (archive_dir / 'loa').mkdir(exist_ok=True)
                (archive_dir / 'to').mkdir(exist_ok=True)
                
                self.archive_dir = archive_dir
                logger.info(f"Initialized local archive directory: {archive_dir}")
                
            elif archive_method == 'google_drive':
                # Drive will be initialized when needed using the same credentials
                # as Google Sheets (already set up in _init_gsheets)
                
                # Get or create the parent folder in Google Drive
                from googleapiclient.discovery import build
                from googleapiclient.http import MediaFileUpload
                
                drive_service = build('drive', 'v3', credentials=self.google_creds)
                
                # Check if the parent folder exists
                parent_folder_name = self.config['archive']['drive_folder_name']
                response = drive_service.files().list(
                    q=f"name='{parent_folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
                    spaces='drive'
                ).execute()
                
                if not response['files']:
                    # Create the parent folder
                    folder_metadata = {
                        'name': parent_folder_name,
                        'mimeType': 'application/vnd.google-apps.folder'
                    }
                    parent_folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
                    parent_folder_id = parent_folder.get('id')
                    logger.info(f"Created Google Drive parent folder: {parent_folder_name}")
                else:
                    parent_folder_id = response['files'][0]['id']
                    logger.info(f"Found existing Google Drive parent folder: {parent_folder_name}")
                
                # Check or create LOA subfolder
                loa_folder_name = 'LOA Files'
                response = drive_service.files().list(
                    q=f"name='{loa_folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false",
                    spaces='drive'
                ).execute()
                
                if not response['files']:
                    folder_metadata = {
                        'name': loa_folder_name,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [parent_folder_id]
                    }
                    loa_folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
                    loa_folder_id = loa_folder.get('id')
                    logger.info(f"Created Google Drive LOA subfolder")
                else:
                    loa_folder_id = response['files'][0]['id']
                
                # Check or create TO subfolder
                to_folder_name = 'TO Files'
                response = drive_service.files().list(
                    q=f"name='{to_folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false",
                    spaces='drive'
                ).execute()
                
                if not response['files']:
                    folder_metadata = {
                        'name': to_folder_name,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [parent_folder_id]
                    }
                    to_folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
                    to_folder_id = to_folder.get('id')
                    logger.info(f"Created Google Drive TO subfolder")
                else:
                    to_folder_id = response['files'][0]['id']
                
                # Store folder IDs for later use
                self.drive_service = drive_service
                self.drive_folder_ids = {
                    'parent': parent_folder_id,
                    'loa': loa_folder_id,
                    'to': to_folder_id
                }
                logger.info(f"Initialized Google Drive archive folders")
            
            else:
                raise ValueError(f"Unsupported archive method: {archive_method}")
            
        except Exception as e:
            logger.error(f"Error initializing archive: {e}")
            raise
    
    def archive_file(self, file_path, file_type):
        """Archive the file to the configured location."""
        try:
            filename = os.path.basename(file_path)
            # Keep the original filename which includes the date
            archived_filename = filename
            
            # Create metadata for the archive record
            metadata = {
                'original_filename': filename,
                'archived_filename': archived_filename,
                'file_type': file_type,
                'archived_timestamp': datetime.now().isoformat(),
                'archived_by': 'workday_email_processor'
            }
            
            archive_method = self.config['archive']['method']
            
            if archive_method == 'local':
                # Determine target directory based on file type
                if file_type == 'loa':
                    target_dir = self.archive_dir / 'loa'
                else:  # 'to'
                    target_dir = self.archive_dir / 'to'
                
                # Copy file to archive
                target_path = target_dir / archived_filename
                
                # If file already exists, append a unique identifier
                if target_path.exists():
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    archived_filename = f"{timestamp}_{filename}"
                    target_path = target_dir / archived_filename
                    metadata['archived_filename'] = archived_filename
                
                shutil.copy2(file_path, target_path)
                
                # Write metadata file
                metadata_path = target_path.with_suffix('.json')
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                logger.info(f"Archived file to local storage: {target_path}")
                return str(target_path)
                
            elif archive_method == 'google_drive':
                # Determine target folder based on file type
                if file_type == 'loa':
                    folder_id = self.drive_folder_ids['loa']
                else:  # 'to'
                    folder_id = self.drive_folder_ids['to']
                
                # Check if file with same name already exists
                response = self.drive_service.files().list(
                    q=f"name='{archived_filename}' and '{folder_id}' in parents and trashed=false",
                    spaces='drive'
                ).execute()
                
                if response['files']:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    archived_filename = f"{timestamp}_{filename}"
                    metadata['archived_filename'] = archived_filename
                
                # Upload file to Google Drive
                file_metadata = {
                    'name': archived_filename,
                    'parents': [folder_id],
                    'description': json.dumps(metadata)  # Store metadata in file description
                }
                
                media = MediaFileUpload(
                    file_path, 
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                file = self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id,webViewLink'
                ).execute()
                
                logger.info(f"Archived file to Google Drive: {file.get('webViewLink')}")
                return file.get('webViewLink')
            
            else:
                raise ValueError(f"Unsupported archive method: {archive_method}")
                
        except Exception as e:
            logger.error(f"Error archiving file: {e}")
            return None
    
    def fetch_email_attachments(self):
        """Fetch Workday export attachments from configured email account."""
        logger.info("Starting email attachment fetch process")
        
        try:
            # Connect to the IMAP server
            mail = imaplib.IMAP4_SSL(self.config['email']['imap_server'])
            mail.login(self.config['email']['username'], self.config['email']['password'])
            mail.select('inbox')
            
            # Search for emails with specific criteria
            search_criteria = self.config['email']['search_criteria']
            status, messages = mail.search(None, search_criteria)
            
            if status != 'OK':
                logger.error(f"Error searching for emails: {status}")
                return []
            
            message_ids = messages[0].split()
            logger.info(f"Found {len(message_ids)} matching emails")
            
            downloaded_files = []
            
            for message_id in message_ids:
                # Fetch the email
                status, msg_data = mail.fetch(message_id, '(RFC822)')
                
                if status != 'OK':
                    logger.error(f"Error fetching email {message_id}: {status}")
                    continue
                
                # Parse the email
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)
                
                # Get email subject for logging
                subject = decode_header(msg["Subject"])[0][0]
                if isinstance(subject, bytes):
                    subject = subject.decode()
                
                logger.info(f"Processing email: {subject}")
                
                # Process attachments
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    
                    filename = part.get_filename()
                    if not filename:
                        continue
                    
                    # Check if it's an Excel file
                    if not (filename.endswith('.xlsx') or filename.endswith('.xls')):
                        continue
                    
                    # Determine file type based on Workday export patterns
                    file_type = None
                    
                    # Match patterns like "SILL_CXO  Leave of Absence 20250312 07_00 PDT.xlsx"
                    if re.search(r'leave\s+of\s+absence', filename.lower()):
                        file_type = 'loa'
                    # Match patterns like "SILL_CXO  Time Off 20250312 07_02 PDT.xlsx"
                    elif re.search(r'time\s+off', filename.lower()):
                        file_type = 'to'
                    else:
                        logger.warning(f"Unrecognized Workday file type: {filename}")
                        continue
                    
                    # Save attachment to temp directory
                    file_path = self.temp_dir / filename
                    with open(file_path, 'wb') as f:
                        f.write(part.get_payload(decode=True))
                    
                    logger.info(f"Saved attachment: {filename} as {file_type} type")
                    downloaded_files.append({
                        'path': file_path,
                        'type': file_type,
                        'message_id': message_id,
                        'subject': subject,
                        'filename': filename
                    })
                
                # Mark as processed
                if self.config['email'].get('mark_as_processed', False):
                    mail.store(message_id, '+FLAGS', '\\Seen')
                    
                    # Move to processed folder if configured
                    if 'processed_folder' in self.config['email']:
                        mail.copy(message_id, self.config['email']['processed_folder'])
                        mail.store(message_id, '+FLAGS', '\\Deleted')
            
            mail.close()
            mail.logout()
            
            logger.info(f"Downloaded {len(downloaded_files)} attachments")
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error in email processing: {e}")
            return []
    
    def process_loa_file(self, file_path):
        """Process Leave of Absence Workday export file for appending to Google Sheets."""
        logger.info(f"Processing LOA file: {file_path}")
        
        try:
            # Read the Excel file
            df = pd.read_excel(file_path)
            
            # Rename columns if needed to match expected format
            column_mapping = self.config.get('column_mapping', {}).get('loa', {})
            if column_mapping:
                df = df.rename(columns=column_mapping)
            
            # Validate basic structure - check if essential columns exist
            required_columns = ['Employee ID', 'Effective Date', 'First Day of Leave']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Generate a timestamp_unix column if it doesn't exist
            if 'timestamp_unix' not in df.columns:
                # Use current time as timestamp for all rows
                current_timestamp = int(time.time())
                df['timestamp_unix'] = current_timestamp
                logger.info("Added timestamp_unix column")
            
            # Additional data cleaning for LOA data
            # Handle missing dates - set to empty string instead of NaN for consistency
            date_columns = [col for col in df.columns if 'Date' in col or 'Day' in col]
            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].fillna('')
            
            logger.info(f"Successfully processed LOA file with {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error processing LOA file: {e}")
            raise
    
    def process_to_file(self, file_path):
        """Process Time Off Workday export file for appending to Google Sheets."""
        logger.info(f"Processing Time Off file: {file_path}")
        
        try:
            # Read the Excel file
            df = pd.read_excel(file_path)
            
            # Rename columns if needed to match expected format
            column_mapping = self.config.get('column_mapping', {}).get('to', {})
            if column_mapping:
                df = df.rename(columns=column_mapping)
            
            # Validate basic structure
            required_columns = ['Employee ID', 'Time Off Date', 'Duration']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Generate a timestamp_unix column if it doesn't exist
            if 'timestamp_unix' not in df.columns:
                # Use current time as timestamp for all rows
                current_timestamp = int(time.time())
                df['timestamp_unix'] = current_timestamp
                logger.info("Added timestamp_unix column")
            
            # Ensure Status column exists
            if 'Status' not in df.columns:
                df['Status'] = 'Approved'  # Default status if missing
            
            # Ensure Unit of Time column exists
            if 'Unit of Time' not in df.columns and 'Duration' in df.columns:
                df['Unit of Time'] = 'Hours'  # Default unit if missing
            
            logger.info(f"Successfully processed Time Off file with {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error processing Time Off file: {e}")
            raise
    
    def extract_date_from_filename(self, filename):
        """Extract date from Workday export filename pattern."""
        # Pattern like: "SILL_CXO  Leave of Absence 20250312 07_00 PDT.xlsx"
        date_pattern = r'(\d{8})'
        match = re.search(date_pattern, filename)
        if match:
            date_str = match.group(1)
            try:
                return datetime.strptime(date_str, '%Y%m%d')
            except ValueError:
                return None
        return None
    
    def append_to_gsheet(self, df, sheet_type):
        """Append dataframe to the appropriate Google Sheet."""
        logger.info(f"Appending {len(df)} rows to {sheet_type.upper()} Google Sheet")
        
        try:
            # Open the appropriate Google Sheet
            if sheet_type == 'loa':
                sheet_id = self.config['google_sheets']['loa_sheet_id']
                sheet_name = self.config['google_sheets'].get('loa_sheet_name', 'Sheet1')
            else:  # 'to'
                sheet_id = self.config['google_sheets']['to_sheet_id']
                sheet_name = self.config['google_sheets'].get('to_sheet_name', 'Sheet1')
            
            # Open the Google Sheet
            spreadsheet = self.gc.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # Get existing data to determine where to append
            existing_data = worksheet.get_all_values()
            
            if not existing_data:
                # If sheet is empty, include headers
                append_headers = True
                start_row = 1
            else:
                # Check if headers match
                headers = existing_data[0]
                missing_columns = [col for col in df.columns if col not in headers]
                
                if missing_columns:
                    logger.warning(f"New columns not in existing sheet: {missing_columns}")
                    # Add new columns to existing headers if needed
                    new_headers = headers + missing_columns
                    # Update the first row with new headers
                    worksheet.update('A1', [new_headers])
                    headers = new_headers
                
                # Start appending after the last row
                start_row = len(existing_data) + 1
                append_headers = False
            
            # Prepare the data for appending
            if append_headers:
                # Include header row if sheet is empty
                values = [df.columns.tolist()] + df.values.tolist()
                range_name = f"A1:{chr(64 + len(df.columns))}{len(df) + 1}"
            else:
                # Reorder columns to match existing sheet
                aligned_df = pd.DataFrame(columns=headers)
                for col in headers:
                    if col in df.columns:
                        aligned_df[col] = df[col]
                    else:
                        aligned_df[col] = np.nan
                
                values = aligned_df.values.tolist()
                range_name = f"A{start_row}:{chr(64 + len(headers))}{start_row + len(df) - 1}"
            
            # Update the sheet
            worksheet.update(range_name, values)
            
            logger.info(f"Successfully appended data to {sheet_type.upper()} Google Sheet")
            return True
            
        except Exception as e:
            logger.error(f"Error appending to Google Sheet: {e}")
            return False
    
    def run(self):
        """Main process to fetch emails and update Google Sheets."""
        logger.info("Starting Workday email to Google Sheets process")
        
        try:
            # Step 1: Fetch email attachments
            downloaded_files = self.fetch_email_attachments()
            
            if not downloaded_files:
                logger.info("No new files found in emails")
                return
            
            results = []
            
            # Step 2: Process each file, append to Google Sheets, and archive
            for file_info in downloaded_files:
                file_path = file_info['path']
                file_type = file_info['type']
                filename = file_info['filename']
                
                try:
                    # Extract date from filename for logging
                    file_date = self.extract_date_from_filename(filename)
                    date_str = file_date.strftime('%Y-%m-%d') if file_date else "unknown date"
                    
                    # Process the file
                    if file_type == 'loa':
                        df = self.process_loa_file(file_path)
                        success = self.append_to_gsheet(df, 'loa')
                    elif file_type == 'to':
                        df = self.process_to_file(file_path)
                        success = self.append_to_gsheet(df, 'to')
                    else:
                        logger.warning(f"Unknown file type for: {file_path}")
                        continue
                    
                    # Archive the file if processing was successful
                    if success:
                        archive_location = self.archive_file(file_path, file_type)
                    else:
                        archive_location = None
                    
                    results.append({
                        'file': filename,
                        'date': date_str,
                        'type': file_type,
                        'records': len(df),
                        'success': success,
                        'archived': archive_location is not None,
                        'archive_location': archive_location
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                    results.append({
                        'file': filename,
                        'type': file_type,
                        'records': 0,
                        'success': False,
                        'archived': False,
                        'error': str(e)
                    })
            
            # Step 3: Log summary
            logger.info("Process completed. Summary:")
            for result in results:
                status = "SUCCESS" if result['success'] else "FAILED"
                archived = "ARCHIVED" if result.get('archived', False) else "NOT ARCHIVED"
                logger.info(f"{result['file']} ({result['type']}, {result.get('date', 'unknown date')}): {status} - {result['records']} records - {archived}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in main process: {e}")
            raise
        finally:
            # Clean up temp directory
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
                logger.info(f"Removed temporary directory: {self.temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory: {e}")


if __name__ == "__main__":
    config_path = os.environ.get('WORKDAY_CONFIG_PATH', 'workday_config.yml')
    processor = WorkdayEmailProcessor(config_path)
    processor.run()
