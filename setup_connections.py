#!/usr/bin/env python3
"""
Setup Airflow connections for Google Cloud.
This script creates the necessary connections for GCP services.
"""

import os
import sys

try:
    from airflow.models import Connection
    from airflow import settings
    from sqlalchemy.orm import sessionmaker
    
    Session = sessionmaker(bind=settings.engine)
    session = Session()
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(
        Connection.conn_id == "google_cloud_default"
    ).first()
    
    if existing_conn:
        print("✓ Connection 'google_cloud_default' already exists")
        session.close()
        sys.exit(0)
    
    # Create Google Cloud connection using application default credentials
    conn = Connection(
        conn_id="google_cloud_default",
        conn_type="google_cloud_platform",
        extra={
            "extra__google_cloud_platform__project": os.getenv("GCP_PROJECT_ID"),
            "extra__google_cloud_platform__key_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        }
    )
    
    session.add(conn)
    session.commit()
    print("✓ Successfully created 'google_cloud_default' connection")
    session.close()
    
except Exception as e:
    print(f"✗ Error setting up connection: {e}")
    sys.exit(1)
