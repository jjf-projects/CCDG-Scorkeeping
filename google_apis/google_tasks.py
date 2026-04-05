# python & 3rd party
import csv
import io
import os
from datetime import datetime

import pandas as pd
from io import BytesIO
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import gspread

from logger.logger import logger_gen as logger


'''
google_tasks.py

Thin wrappers around the gspread and PyDrive2 libraries for interacting
with Google Sheets and Google Drive using a service account.

Setup:
  - Create a service account in Google Cloud Console
  - Download the JSON credentials file and point G_SVC_CREDS_FILE at it
  - Share any Sheets/Drive folders with the service account email address
  - See: https://docs.google.com/document/d/1obuwpJykyDmwbKyDOIOFzF-EHP-qpMCDVK81Q01vock
'''


# ---------------------------------------------------------------------------
# GOOGLE SHEETS  (via gspread)
# ---------------------------------------------------------------------------

def read_gsheet_range(creds_file: str, sheet_info: dict) -> list:
    """Read a range from a Google Sheet and return it as a list of rows.

    Args:
        creds_file: Path to the service account JSON credentials file.
        sheet_info: Dict with keys: file_id (str), sheet_id (int), range (str).

    Returns:
        List of lists — each inner list is one row of cell values.
    """
    client = _auth_sheets(creds_file)
    try:
        sheet = client.open_by_key(sheet_info['file_id'])
    except PermissionError:
        msg = (
            "Permission denied reading Google Sheet. "
            "Did you share it with the service account? "
            "ccdg-google-service-account@ccdg-csv-utility.iam.gserviceaccount.com"
        )
        logger.error(msg)
        raise

    worksheet = sheet.get_worksheet_by_id(sheet_info['sheet_id'])
    return worksheet.get(sheet_info['range'])


def write_gsheet_range(creds_file: str, sheet_info: dict, rows: list) -> None:
    """Clear a Google Sheet tab and write new data starting at A1.

    Args:
        creds_file: Path to the service account JSON credentials file.
        sheet_info: Dict with keys: file_id (str), sheet_id (int).
        rows:       List of lists — each inner list is one row of cell values.
    """
    client = _auth_sheets(creds_file)
    sheet = client.open_by_key(sheet_info['file_id'])
    worksheet = sheet.get_worksheet_by_id(sheet_info['sheet_id'])
    worksheet.clear()
    worksheet.update('A1', rows)


def list_to_dict(data: list) -> list:
    """Convert a list-of-lists (with a header row) to a list of dicts.

    The first row is used as dict keys.  Useful for turning raw gsheet
    data into structured registration rows.

    Example:
        [['Name', 'Email'], ['Alice', 'a@b.com']]
        → [{'Name': 'Alice', 'Email': 'a@b.com'}]
    """
    if not data or len(data) < 2:
        return []
    headers = data[0]
    return [dict(zip(headers, row)) for row in data[1:]]


def dicts_to_list(data: list) -> list:
    """Convert a list of dicts to a list-of-lists with a header row.

    Inverse of list_to_dict.  Useful for preparing data to write back
    to a Google Sheet.
    """
    if not data:
        return []
    headers = list(data[0].keys())
    return [headers] + [[row[k] for k in headers] for row in data]


# ---------------------------------------------------------------------------
# GOOGLE DRIVE  (via PyDrive2)
# ---------------------------------------------------------------------------

def add_file_to_gdrive(creds_file: str, local_file: str, folder_id: str) -> None:
    """Upload a local file to a Google Drive folder, replacing any existing file with the same name.

    Args:
        creds_file: Path to the service account JSON credentials file.
        local_file: Absolute path to the file to upload.
        folder_id:  Google Drive folder ID to upload into.
    """
    drive = _auth_drive(creds_file)
    filename = os.path.basename(local_file)

    # Trash any existing file with the same name in the target folder
    existing = drive.ListFile(
        {'q': f"title='{filename}' and trashed=false and '{folder_id}' in parents"}
    ).GetList()
    for f in existing:
        try:
            f.Trash()
        except Exception as e:
            logger.error(f"Could not trash existing '{filename}' in Drive folder {folder_id}: {e}")

    try:
        new_file = drive.CreateFile({
            'title': filename,
            'parents': [{'id': folder_id}],
        })
        new_file.SetContentFile(local_file)
        new_file.Upload()
        logger.info(f"Uploaded '{local_file}' to Drive folder {folder_id}.")
    except Exception as e:
        logger.error(f"Failed to upload '{filename}' to Drive folder {folder_id}: {e}")


def read_csv_from_gdrive(creds_file: str, file_id: str) -> list:
    """Download a CSV file from Google Drive and return its rows as a list of dicts.

    Args:
        creds_file: Path to the service account JSON credentials file.
        file_id:    Google Drive file ID.
    """
    drive = _auth_drive(creds_file)
    f = drive.CreateFile({'id': file_id})
    raw = f.GetContentString(mimetype='text/csv', encoding='utf-8-sig')
    return list(csv.DictReader(io.StringIO(raw)))


def read_xlsx_from_gdrive(creds_file: str, file_id: str) -> list:
    """Download an xlsx file from Google Drive and return its rows as a list of dicts.

    Args:
        creds_file: Path to the service account JSON credentials file.
        file_id:    Google Drive file ID.
    """
    drive = _auth_drive(creds_file)
    f = drive.CreateFile({'id': file_id})

    buffer = BytesIO()
    for chunk in f.GetContentIOBuffer():
        buffer.write(chunk)
    buffer.seek(0)

    return pd.read_excel(buffer).to_dict(orient='records')


def get_gdrive_file_metadata(creds_file: str, file_id: str, field: str):
    """Return a single metadata field for a Drive file (e.g. 'modifiedDate').

    Args:
        creds_file: Path to the service account JSON credentials file.
        file_id:    Google Drive file ID.
        field:      The metadata field name to retrieve.
    """
    drive = _auth_drive(creds_file)
    f = drive.CreateFile({'id': file_id})
    return f[field]


# ---------------------------------------------------------------------------
# PRIVATE AUTH HELPERS
# ---------------------------------------------------------------------------

def _auth_sheets(creds_file: str) -> gspread.Client:
    """Return an authenticated gspread Client using a service account."""
    try:
        return gspread.service_account(filename=creds_file)
    except Exception as e:
        msg = f"gspread authentication failed: {e}. Check that G_SVC_CREDS_FILE is correct."
        logger.error(msg)
        raise


def _auth_drive(creds_file: str) -> GoogleDrive:
    """Return an authenticated PyDrive2 GoogleDrive instance using a service account."""
    settings = {
        "client_config_backend": "service",
        "service_config": {"client_json_file_path": creds_file},
    }
    try:
        gauth = GoogleAuth(settings=settings)
        gauth.ServiceAuth()
        return GoogleDrive(auth=gauth)
    except Exception as e:
        msg = f"PyDrive2 authentication failed: {e}. Check that G_SVC_CREDS_FILE is correct."
        logger.error(msg)
        raise
