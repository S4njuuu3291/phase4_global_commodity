#!/usr/bin/env python3
"""
Adhoc Script: Migrate S3 data dari format lama ke format Hive partitioned baru

Old Format: bronze/2026-04-08 08:00:00/metal_prices-2026-04-08 08:00:00.json
New Format: bronze/metal/ingested_at=2026-04-08/data.json

Tahapan:
1. List semua objects di S3 dengan pattern lama
2. Read data dari old path
3. Write ke new path dengan format baru
4. Verifikasi (optional)
5. Delete old path jika verifikasi sukses
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Initialize S3 client
s3_client = boto3.client('s3', region_name='ap-southeast-1')

# Config
BUCKET_NAME = "global-commodity-data-lake-commo-dev"
DATASOURCES = {
    'metal': ['metal_prices', 'metal'],
    'currency': ['currency_rates', 'currency'],
    'fred': ['fred_data', 'fred'],
    'news': ['news_data', 'news']
}

def extract_date_from_path(path: str) -> str:
    """Extract date dari timestamp di path lama
    
    Contoh: bronze/2026-04-08 08:00:00/metal_prices-2026-04-08 08:00:00.json
    Return: 2026-04-08
    """
    try:
        # Split path dan ambil timestamp folder
        parts = path.split('/')
        timestamp = parts[1]  # "2026-04-08 08:00:00"
        date_only = timestamp.split(' ')[0]  # "2026-04-08"
        return date_only
    except Exception as e:
        logging.warning(f"Could not extract date from path {path}: {e}")
        return None

def migrate_bronze_data():
    """Migrate bronze layer data dari format lama ke format baru"""
    logging.info("=" * 80)
    logging.info("MIGRATING BRONZE LAYER DATA")
    logging.info("=" * 80)
    
    try:
        # List semua objects di bronze layer (lama)
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix='bronze/')
        
        migrated_count = 0
        failed_count = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                old_key = obj['Key']
                
                # Skip jika sudah format baru (contains "ingested_at=")
                if 'ingested_at=' in old_key:
                    logging.debug(f"Skipping (already new format): {old_key}")
                    continue
                
                # Skip jika bukan file (folder)
                if old_key.endswith('/'):
                    continue
                
                # Tentukan datasource dari nama file
                datasource = None
                for ds, patterns in DATASOURCES.items():
                    if any(pattern in old_key for pattern in patterns):
                        datasource = ds
                        break
                
                if not datasource:
                    logging.warning(f"Could not determine datasource for: {old_key}")
                    continue
                
                # Extract date
                date_str = extract_date_from_path(old_key)
                if not date_str:
                    logging.error(f"Could not extract date from: {old_key}")
                    failed_count += 1
                    continue
                
                # Build new key
                new_key = f"bronze/{datasource}/ingested_at={date_str}/data.json"
                
                try:
                    logging.info(f"Migrating: {old_key}")
                    logging.info(f"       To: {new_key}")
                    
                    # 1. Read dari old path
                    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=old_key)
                    data = json.loads(response['Body'].read().decode('utf-8'))
                    
                    # 2. Write ke new path
                    s3_client.put_object(
                        Bucket=BUCKET_NAME,
                        Key=new_key,
                        Body=json.dumps(data, ensure_ascii=False) + '\n',
                        ContentType='application/json'
                    )
                    
                    logging.info(f"✅ MIGRATED: {old_key} → {new_key}")
                    migrated_count += 1
                    
                except Exception as e:
                    logging.error(f"❌ FAILED to migrate {old_key}: {e}")
                    failed_count += 1
        
        logging.info(f"Bronze migration complete: {migrated_count} succeeded, {failed_count} failed")
        return migrated_count, failed_count
        
    except Exception as e:
        logging.error(f"Error during bronze migration: {e}", exc_info=True)
        return 0, 0

def migrate_silver_data():
    """Migrate silver layer data dari format lama ke format baru"""
    logging.info("=" * 80)
    logging.info("MIGRATING SILVER LAYER DATA")
    logging.info("=" * 80)
    
    try:
        # List semua objects di silver layer (lama)
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix='silver/')
        
        migrated_count = 0
        failed_count = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                old_key = obj['Key']
                
                # Skip jika sudah format baru (contains "ingested_at=")
                if 'ingested_at=' in old_key:
                    logging.debug(f"Skipping (already new format): {old_key}")
                    continue
                
                # Skip jika bukan file (folder) atau bukan parquet
                if old_key.endswith('/') or not old_key.endswith('.parquet'):
                    continue
                
                # Tentukan datasource dari nama file
                datasource = None
                for ds, patterns in DATASOURCES.items():
                    if any(pattern in old_key for pattern in patterns):
                        datasource = ds
                        break
                
                if not datasource:
                    logging.warning(f"Could not determine datasource for: {old_key}")
                    continue
                
                # Extract date
                date_str = extract_date_from_path(old_key)
                if not date_str:
                    logging.error(f"Could not extract date from: {old_key}")
                    failed_count += 1
                    continue
                
                # Build new key (.parquet format)
                new_key = f"silver/{datasource}/ingested_at={date_str}/data.parquet"
                
                try:
                    logging.info(f"Migrating: {old_key}")
                    logging.info(f"       To: {new_key}")
                    
                    # 1. Copy dari old path ke new path (binary, jangan parse)
                    copy_source = {'Bucket': BUCKET_NAME, 'Key': old_key}
                    s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=BUCKET_NAME,
                        Key=new_key
                    )
                    
                    logging.info(f"✅ MIGRATED: {old_key} → {new_key}")
                    migrated_count += 1
                    
                except Exception as e:
                    logging.error(f"❌ FAILED to migrate {old_key}: {e}")
                    failed_count += 1
        
        logging.info(f"Silver migration complete: {migrated_count} succeeded, {failed_count} failed")
        return migrated_count, failed_count
        
    except Exception as e:
        logging.error(f"Error during silver migration: {e}", exc_info=True)
        return 0, 0

def delete_old_data(layer: str, dry_run: bool = True):
    """Delete old format data setelah verifikasi migration sukses
    
    Args:
        layer: 'bronze' atau 'silver'
        dry_run: Jika True, hanya list tanpa hapus
    """
    logging.info("=" * 80)
    logging.info(f"DELETING OLD {layer.upper()} DATA")
    if dry_run:
        logging.info("(DRY RUN MODE - tidak akan benar-benar dihapus)")
    logging.info("=" * 80)
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=f'{layer}/')
        
        deleted_count = 0
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Skip jika sudah format baru
                if 'ingested_at=' in key:
                    continue
                
                # Skip folders
                if key.endswith('/'):
                    continue
                
                try:
                    if dry_run:
                        logging.info(f"[DRY RUN] Would delete: {key}")
                    else:
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
                        logging.info(f"✅ DELETED: {key}")
                    deleted_count += 1
                    
                except Exception as e:
                    logging.error(f"❌ FAILED to delete {key}: {e}")
        
        logging.info(f"Deletion complete: {deleted_count} objects {'would be deleted' if dry_run else 'deleted'}")
        return deleted_count
        
    except Exception as e:
        logging.error(f"Error during deletion: {e}", exc_info=True)
        return 0

def main():
    """Main migration workflow"""
    logging.info("\n" + "=" * 80)
    logging.info("S3 DATA MIGRATION: Old Path Format → Hive Partitioned Format")
    logging.info("=" * 80 + "\n")
    
    # Step 1: Migrate Bronze
    bronze_migrated, bronze_failed = migrate_bronze_data()
    
    # Step 2: Migrate Silver
    silver_migrated, silver_failed = migrate_silver_data()
    
    # Step 3: Summary
    logging.info("\n" + "=" * 80)
    logging.info("MIGRATION SUMMARY")
    logging.info("=" * 80)
    logging.info(f"Bronze: {bronze_migrated} migrated, {bronze_failed} failed")
    logging.info(f"Silver: {silver_migrated} migrated, {silver_failed} failed")
    
    total_success = bronze_migrated + silver_migrated
    total_failed = bronze_failed + silver_failed
    
    if total_failed == 0 and total_success > 0:
        logging.info("\n✅ All migrations successful!")
        
        # Step 4: Delete old data (prompt user)
        response = input("\nDelete old format data? (y/n): ").strip().lower()
        
        if response == 'y':
            # First dry-run
            logging.info("\n--- Dry Run: Files that WOULD be deleted ---")
            delete_old_data('bronze', dry_run=True)
            delete_old_data('silver', dry_run=True)
            
            # Confirm
            response2 = input("\nProceed with deletion? (y/n): ").strip().lower()
            if response2 == 'y':
                logging.info("\n--- DELETING OLD DATA ---")
                delete_old_data('bronze', dry_run=False)
                delete_old_data('silver', dry_run=False)
                logging.info("\n✅ Old data deletion complete!")
            else:
                logging.info("\nDeletion cancelled.")
        else:
            logging.info("\nSkipping deletion. Old data retained.")
    else:
        logging.error(f"\n❌ Migration had failures. Please review before deleting old data.")
        sys.exit(1)
    
    logging.info("\n" + "=" * 80)
    logging.info("Migration workflow complete!")
    logging.info("=" * 80 + "\n")

if __name__ == "__main__":
    main()
