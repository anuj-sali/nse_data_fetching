import requests
import csv
import json
import os
import logging
import argparse
from datetime import datetime
from typing import Dict, Optional

def setup_logging() -> None:
    """
    Setup logging configuration
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('nse_data_fetch.log'),
            logging.StreamHandler()
        ]
    )

def download_csv(url: str = "https://images.dhan.co/api-data/api-scrip-master.csv", 
                filename: str = 'api-scrip-master.csv') -> bool:
    """
    Download the api-scrip-master.csv file from the given URL
    
    Args:
        url: The URL to download from
        filename: The local filename to save as
    
    Returns:
        bool: True if successful, False otherwise
    """
    logging.info(f"Downloading {filename} from {url}...")
    
    try:
        # Add headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        # Save the CSV file
        with open(filename, 'wb') as f:
            f.write(response.content)
        
        file_size = len(response.content)
        logging.info(f"Successfully downloaded {filename} ({file_size:,} bytes)")
        return True
        
    except requests.exceptions.Timeout:
        logging.error("Download timed out after 30 seconds")
        return False
    except requests.exceptions.ConnectionError:
        logging.error("Connection error occurred while downloading")
        return False
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {e}")
        return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading CSV file: {e}")
        return False
    except IOError as e:
        logging.error(f"Error saving file: {e}")
        return False

def create_futstk_mapping(csv_filename: str = 'api-scrip-master.csv', 
                          instrument_type: str = 'FUTSTK') -> Dict[str, str]:
    """
    Create a dictionary mapping SEM_TRADING_SYMBOL to SEM_SMST_SECURITY_ID
    for all rows where SEM_INSTRUMENT_NAME matches the specified type
    
    Args:
        csv_filename: Path to the CSV file
        instrument_type: Type of instrument to filter (default: 'FUTSTK')
    
    Returns:
        Dict[str, str]: Mapping of trading symbols to security IDs
    """
    logging.info(f"Extracting {instrument_type} mappings from {csv_filename}...")
    mapping = {}
    
    if not os.path.exists(csv_filename):
        logging.error(f"CSV file {csv_filename} not found")
        return {}
    
    try:
        with open(csv_filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)
            
            # Find the indices of required columns
            required_columns = ['SEM_INSTRUMENT_NAME', 'SEM_TRADING_SYMBOL', 'SEM_SMST_SECURITY_ID']
            column_indices = {}
            
            for col in required_columns:
                try:
                    column_indices[col] = header.index(col)
                except ValueError:
                    logging.error(f"Required column '{col}' not found in CSV header")
                    return {}
            
            logging.info(f"Column indices - {', '.join([f'{col}: {idx}' for col, idx in column_indices.items()])}")
            
            # Process each row
            row_count = 0
            instrument_count = 0
            duplicates = 0
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 since header is row 1
                row_count += 1
                
                # Check if row has enough columns
                min_required_idx = max(column_indices.values())
                if len(row) <= min_required_idx:
                    continue
                
                # Check if this is the instrument type we want
                if row[column_indices['SEM_INSTRUMENT_NAME']] == instrument_type:
                    trading_symbol = row[column_indices['SEM_TRADING_SYMBOL']].strip()
                    security_id = row[column_indices['SEM_SMST_SECURITY_ID']].strip()
                    
                    # Skip empty values
                    if not trading_symbol or not security_id:
                        continue
                    
                    # Check for duplicates
                    if trading_symbol in mapping:
                        if mapping[trading_symbol] != security_id:
                            logging.warning(f"Duplicate trading symbol '{trading_symbol}' with different security IDs: {mapping[trading_symbol]} vs {security_id} (row {row_num})")
                        duplicates += 1
                    else:
                        mapping[trading_symbol] = security_id
                        instrument_count += 1
            
            logging.info(f"Processed {row_count:,} rows, found {instrument_count} unique {instrument_type} entries")
            if duplicates > 0:
                logging.warning(f"Found {duplicates} duplicate trading symbols")
        
        return mapping
        
    except FileNotFoundError:
        logging.error(f"CSV file {csv_filename} not found")
        return {}
    except UnicodeDecodeError as e:
        logging.error(f"Error reading CSV file - encoding issue: {e}")
        return {}
    except csv.Error as e:
        logging.error(f"Error parsing CSV file: {e}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error processing CSV file: {e}")
        return {}

def save_mapping_to_json(mapping: Dict[str, str], 
                        output_filename: str = 'futstk_mapping.json',
                        add_metadata: bool = True) -> bool:
    """
    Save the mapping to a JSON file with optional metadata
    
    Args:
        mapping: Dictionary of trading symbols to security IDs
        output_filename: Output JSON filename
        add_metadata: Whether to include metadata in the JSON
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not mapping:
        logging.warning("No mappings to save")
        return False
    
    try:
        # Prepare data to save
        data_to_save = mapping.copy()
        
        if add_metadata:
            # Add metadata
            metadata = {
                "_metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "total_mappings": len(mapping),
                    "source": "https://images.dhan.co/api-data/api-scrip-master.csv",
                    "instrument_type": "FUTSTK"
                }
            }
            data_to_save = {**metadata, **mapping}
        
        # Save to JSON file
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(output_filename)
        logging.info(f"Found {len(mapping):,} mappings")
        logging.info(f"Mapping saved to {output_filename} ({file_size:,} bytes)")
        
        # Show sample entries
        sample_count = min(10, len(mapping))
        logging.info(f"Sample entries ({sample_count} of {len(mapping)}):")
        for i, (symbol, security_id) in enumerate(mapping.items()):
            if i >= sample_count:
                break
            logging.info(f"  '{symbol}': '{security_id}'")
        
        if len(mapping) > sample_count:
            logging.info(f"  ... and {len(mapping) - sample_count:,} more entries")
        
        return True
        
    except IOError as e:
        logging.error(f"Error writing to file {output_filename}: {e}")
        return False
    except json.JSONEncodeError as e:
        logging.error(f"Error encoding data to JSON: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error saving mapping to JSON: {e}")
        return False

def validate_mapping(mapping: Dict[str, str]) -> bool:
    """
    Validate the extracted mapping for basic sanity checks
    
    Args:
        mapping: Dictionary to validate
    
    Returns:
        bool: True if validation passes
    """
    if not mapping:
        logging.error("Mapping is empty")
        return False
    
    # Check for reasonable number of entries
    if len(mapping) < 100:
        logging.warning(f"Mapping has only {len(mapping)} entries, which seems low")
    
    # Check for any obviously invalid entries
    invalid_count = 0
    for symbol, security_id in mapping.items():
        if not symbol or not security_id:
            invalid_count += 1
        elif not security_id.isdigit():
            logging.warning(f"Non-numeric security ID found: {symbol} -> {security_id}")
    
    if invalid_count > 0:
        logging.warning(f"Found {invalid_count} entries with empty values")
    
    logging.info(f"Validation completed: {len(mapping)} valid mappings")
    return True

def main(skip_download: bool = False, 
         csv_url: str = "https://images.dhan.co/api-data/api-scrip-master.csv",
         output_file: str = 'futstk_mapping.json') -> None:
    """
    Main function to download CSV and extract FUTSTK mappings
    
    Args:
        skip_download: Skip downloading if CSV already exists
        csv_url: URL to download CSV from
        output_file: Output JSON filename
    """
    setup_logging()
    
    start_time = datetime.now()
    logging.info("=== NSE Data Fetching and FUTSTK Mapping Extraction Started ===")
    
    try:
        # Step 1: Download the CSV file (unless skipping)
        csv_filename = 'api-scrip-master.csv'
        
        if skip_download and os.path.exists(csv_filename):
            logging.info(f"Skipping download, using existing {csv_filename}")
        else:
            if not download_csv(csv_url, csv_filename):
                logging.error("Failed to download CSV file. Exiting.")
                return
        
        # Step 2: Extract FUTSTK mappings
        mapping = create_futstk_mapping(csv_filename)
        if not mapping:
            logging.error("Failed to extract FUTSTK mappings. Exiting.")
            return
        
        # Step 3: Validate mappings
        if not validate_mapping(mapping):
            logging.error("Mapping validation failed. Exiting.")
            return
        
        # Step 4: Save mappings to JSON
        if not save_mapping_to_json(mapping, output_file):
            logging.error("Failed to save FUTSTK mappings. Exiting.")
            return
        
        # Success summary
        duration = datetime.now() - start_time
        logging.info("=== Process completed successfully! ===")
        logging.info(f"Execution time: {duration.total_seconds():.2f} seconds")
        logging.info("Files created/updated:")
        logging.info(f"- {csv_filename} (downloaded)")
        logging.info(f"- {output_file} (extracted mappings)")
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error in main process: {e}")
        raise

def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Download NSE data and extract FUTSTK mappings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_and_extract.py                    # Download and extract with defaults
  python fetch_and_extract.py --skip-download    # Use existing CSV file
  python fetch_and_extract.py -o custom.json     # Custom output filename
  python fetch_and_extract.py --url "custom_url" # Custom CSV URL
        """
    )
    
    parser.add_argument(
        '--skip-download', '-s',
        action='store_true',
        help='Skip downloading CSV if it already exists locally'
    )
    
    parser.add_argument(
        '--url', '-u',
        default="https://images.dhan.co/api-data/api-scrip-master.csv",
        help='URL to download CSV from (default: %(default)s)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='futstk_mapping.json',
        help='Output JSON filename (default: %(default)s)'
    )
    
    parser.add_argument(
        '--instrument-type', '-i',
        default='FUTSTK',
        help='Instrument type to filter (default: %(default)s)'
    )
    
    parser.add_argument(
        '--no-metadata',
        action='store_true',
        help='Do not include metadata in output JSON'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: %(default)s)'
    )
    
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='NSE Data Fetcher v1.0.0'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    # Update logging level based on argument
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Call main with parsed arguments
    main(
        skip_download=args.skip_download,
        csv_url=args.url,
        output_file=args.output
    )