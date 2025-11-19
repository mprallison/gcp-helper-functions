import os
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from gspread_dataframe import set_with_dataframe

# --- Authentication ---
class GCPAuth:

    """create auth credentials from secrets
    if token.json doesn't exist then prompt user
    """

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

    def execute_request(self, doc_id, requests):
        
        """run batch update to gdoc

        args:
            doc_id: target gdoc
            requests: list opf json requests
        """
        
        result = self.doc_service.documents().batchUpdate(documentId=doc_id, body={"requests": requests}).execute()

        return result

# --- Google Sheets Client ---
class GSheetsClient:

    def __init__(self, gspread_client):
        self.gc = gspread_client

    def read_sheet(self, document_id, sheet_name, headers=None, header_row=0):

        """if tab does not exist, create tab
        return sheet content as dataframe and worksheet

         args:
            document_id: id string found in url when document is open in browser
                         https://docs.google.com/spreadsheets/d/LONG STIRNG HERE/edit?gid=0#gid=0
            sheet_name: sheet (or tab) in gsheet document
            headers: df will reject empty or repeated headers in gsheet. so assign ordered headers
            header_row: assign if data does not start on line one of gsheet
        """

        sh = self.gc.open_by_key(document_id)
        try:
            ws = sh.worksheet(sheet_name)
        except:
            sh.add_worksheet(title=sheet_name, rows=1, cols=1)
            ws = sh.worksheet(sheet_name)
        
        print(f"Serving sheet: {sh.title}, {sheet_name}")
        
        if headers is None:
            records = ws.get_all_records(head=header_row)
            
        else:
            all_data = ws.get_all_values()
            data_rows = all_data[header_row+1:]

            records = []
            for row in data_rows:
                record = dict(zip(headers, row))
                records.append(record)
            
        df = pd.DataFrame(records)
        
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

    def create_gdrive_object(self, name, mime_type, folder_id):
        
        """
        create new empty object in a specific google drive folder.
        return object id

        args:
            name (str): name of the new object.
            mime_type (str): mimetype shortcut: "gdoc", "gsheet", "folder"
            folder_id (str): id of the parent folder.
            file_content (bytes, optional): The content to upload. If None,
                                             creates a metadata-only file.
        """
        
        full_mimetypes = {
                        "gdoc": "application/vnd.google-apps.document",
                        "gsheet": "application/vnd.google-apps.sheet",
                        "folder": "application/vnd.google-apps.folder"
                        }

        full_mime_type = full_mimetypes[mime_type]
        
        file_metadata = {
            'name': name,
            'mimeType': full_mime_type,
            'parents': [folder_id]
        }

        try:
            file = self.drive_service.files().create(body=file_metadata, supportsAllDrives=True, fields="id").execute()
            
            return file["id"]

        except Exception as e:
            print(f"An error occurred: {e}")
            return None

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

        result = self.drive_service.files().copy(fileId=file_id, body=body, fields="id", supportsAllDrives=True).execute()
        
        return result["id"]
    
    def delete_object(self, object_id):

        """delete object 
        """
        
        try:
            self.drive_service.files().delete(fileId=object_id, supportsAllDrives=True).execute()
        
        except Exception as e:
            return f"An error occurred: {e}"

# --- Google Form Client ---
class GFormClient():
    
    def __init__(self, creds: Credentials):
        self.form_service = build("forms", "v1", credentials=creds)

    def get_data(self, form_id):
            
        """retrieve all submission data and form metadata

        args:
            form_id: file id
        """

        try:
            all_responses = []
            page_token = None

            #for paginator
            while True:
                request = self.form_service.forms().responses().list(formId=form_id, pageToken=page_token)
                result = request.execute()
                
                #get response data
                responses = result.get("responses", [])
                if responses:
                    all_responses.extend(responses)
                
                page_token = result.get("nextPageToken")
                if not page_token:
                    break

            #get form questions and name meta
            form_meta = self.form_service.forms().get(formId=form_id).execute()
            form_meta.get("items", [])
       
            return all_responses, form_meta

        except HttpError as err:
            print(f"An error occurred: {err}")
            return None