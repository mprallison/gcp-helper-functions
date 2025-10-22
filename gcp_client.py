import os
import pandas as pd
import numpy as np
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from gspread_dataframe import set_with_dataframe

# --- Authentication ---
class GCPAuth:

    def __init__(self, relative_path: str):

        self.relative_path = relative_path
        self.creds = self._load_credentials()
        self.oauth = self._gspread_oauth()

    def _load_credentials(self):

        token_path = os.path.join(self.relative_path, "token.json")
        cred_path = os.path.join(self.relative_path, "credentials.json")
        creds = None

        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(cred_path):
                    raise FileNotFoundError(f"{cred_path} not found")
                
                flow = InstalledAppFlow.from_client_secrets_file(cred_path, ["https://www.googleapis.com/auth/drive"])
                creds = flow.run_local_server(port=0)
            with open(token_path, "w") as token_file:
                token_file.write(creds.to_json())

        return creds

    def _gspread_oauth(self):

        token_path = os.path.join(self.relative_path, "token.json")
        
        return gspread.oauth(credentials_filename=token_path, authorized_user_filename=token_path)


# --- Google Docs Client ---
class GDocsClient:

    def __init__(self, creds: Credentials):

        self.doc_service = build("docs", "v1", credentials=creds)

    def create_document(self, title, folder_id=None):
        
        """create a new gdoc in MyDrive. return document id

        args:
            title: gdoc file name
        """

        doc = self.doc_service.documents().create(body={"title": title}).execute()

        return doc.get("documentId")

    def execute_request(self, doc_id, requests):
        
        """run batch update to gdoc

        args:
            doc_id: target gdoc
            requests: list opf json requests
        """
        
        result = self.doc_service.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()

        return result

    def read_doc(self, document_id):

        """return document object

        args:
            document_id: id string found in url when document is open in browser
                         https://docs.google.com/document/d/LONG STIRNG HERE/edit?tab=t.0
        """

        try:
            doc = self.doc_service.documents().get(documentId=document_id).execute()
            print("Serving document:", doc.get("title"))
            return doc
        
        except HttpError as error:
            print("Error:", error)
            return None

# --- Google Sheets Client ---
class GSheetsClient:

    def __init__(self, gspread_client):
        self.gc = gspread_client

    def read_sheet(self, document_id, sheet_name):

        """if tab does not exist, create tab
        return sheet content as dataframe and worksheet

         args:
            document_id: id string found in url when document is open in browser
                         https://docs.google.com/spreadsheets/d/LONG STIRNG HERE/edit?gid=0#gid=0
            sheet_name: sheet (or tab) in gsheet document
             
        """

        sh = self.gc.open_by_key(document_id)
        try:
            ws = sh.worksheet(sheet_name)
        except:
            sh.add_worksheet(title=sheet_name, rows=1, cols=1)
            ws = sh.worksheet(sheet_name)
        
        print(f"Serving sheet: {sh.title}, {sheet_name}")
        df = pd.DataFrame(ws.get_all_records())
        
        return df.astype(str), ws

    def write_sheet(self, df, worksheet):

        """write df to gsheet
        """

        df = df.astype(str)
        set_with_dataframe(worksheet, df)

        return

# --- Google Drive Client ---
class GDriveClient:
    def __init__(self, creds: Credentials):
        self.drive_service = build("drive", "v3", credentials=creds)

    def gdrive_ls(self, dir_id: str, parent_drive_id: str):

        """return contents of gdrive directory
        """

        query = f"trashed=false and parents in '{dir_id}'"
        results = self.drive_service.files().list(
            q=query,
            corpora="drive",
            driveId=parent_drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1000
        ).execute()
        
        return {f["name"]: f["id"] for f in results["files"]}

    def create_folder(self, folder_name: str, parent_folder_id: str):

        """create new directory in gdrive
        """

        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }

        folder = self.drive_service.files().create(body=folder_metadata, fields="*", supportsAllDrives=True).execute()
        
        return folder["id"]

    def copy_file(self, file_id, copy_name=None, new_directory_id=None, mime_type=None):

        """create copy of file. return file id of file copy

        args:
            file_id: target file id
            copy_name: declare copy filename else default to "Copy of target filename"
            new_directory_id: declare location of copy else efault to same directory as target file
            mime_type: declare if target file type to differ from copy e.g. xlsx to gsheet
        """

        body = {}

        if copy_name:
            body["name"] = copy_name
        if new_directory_id:
            body["parents"] = [new_directory_id]
        if mime_type:
            body["mimeType"] = mime_type

        result = self.drive_service.files().copy(fileId=file_id, body=body, supportsAllDrives=True).execute()
        
        return result["id"]