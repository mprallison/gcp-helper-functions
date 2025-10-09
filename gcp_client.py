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

    def read_doc_text(self, doc):

        """return all text elements within entire gdoc

        args:
            doc: doc object returned by read_doc()
        """

        def read_paragraph_element(element):
            text_run = element.get("textRun")
            if not text_run or "suggestedDeletionIds" in text_run:
                return ""
            return text_run.get("content")

        def read_structural_elements(elements):

            text = ""
            for value in elements:
                if "paragraph" in value:
                    for elem in value["paragraph"]["elements"]:
                        text += read_paragraph_element(elem)
                elif "table" in value:
                    for row in value["table"]["tableRows"]:
                        for cell in row["tableCells"]:
                            text += read_structural_elements(cell["content"])
                elif "tableOfContents" in value:
                    text += read_structural_elements(value["tableOfContents"]["content"])
            return text

        return read_structural_elements(doc["body"]["content"])

    def read_table_section(self, doc, table_id):

        """return table element containing table_id
        """

        sections = doc['body']['content']
        for section in sections:
            if table_id in str(section):
                found_section=True
                return section
        
        print(f'table_id "{table_id}" not found in a gdoc table')
        
        return None

    def read_table_textruns(self, doc, table_id, header_row_index, preserve_format=False):

        """iter through table rows >> cells >>paras >> textruns to retreive table text as list.
        also return count of columns to organize table
        """

        def read_textrun_element(elem):

            #drop suggested deletions as though accepted
            #preserve format allows formatting to be written to document
            if "suggestedDeletionIds" in elem["textRun"]:
                return (elem["startIndex"], " " * len(elem["textRun"]["content"]) if preserve_format else "")
            return (elem["startIndex"], elem["textRun"]["content"])

        def read_para(para):

            para_elems = [read_textrun_element(e) for e in para["elements"]]
            return para_elems[0][0], "".join([e[1] for e in para_elems])

        def read_cell(cell):

            cell_elems = [read_para(p["paragraph"]) for p in cell["content"]]
            return cell_elems[0][0], "".join([e[1] for e in cell_elems])

        def read_row(row):

            return [read_cell(c) for c in row["tableCells"]]

        section = self.read_table_section(doc, table_id)
        
        #check table section found
        if section is None:
            
            return None, None

        rows = section["table"]["tableRows"][header_row_index:]
        table_cells = []
        
        for row in rows:
            table_cells.extend(read_row(row))

        #count columns
        column_n = section["table"]["columns"]
        
        return table_cells, column_n

    def read_doc_table(self, doc, table_id, header_row_index, preserve_format=False):

        """return table in gdoc as list of cell textruns and count of columns
        args:
            doc: object returned by read_doc()
            table: unique table tag found somewhere in table content
            header_row_index: row index of header row (1 if single title row above headers)
            preserve_format: True if doc contains suggested changes and changes are to be written back to doc
        """

        cells, column_n = self.read_table_textruns(doc, table_id, header_row_index, preserve_format)

        #check table data is returned
        if cells is None:
            return 

        #get text from index, textrun tuples
        cell_text = [c[1] for c in cells]

        #organize cell text runs into rows using number of columns
        rows = [cell_text[i:i + column_n] for i in range(0, len(cell_text), column_n)]

        headers = rows[0]
        data = np.array(rows[1:]).T.tolist()
        df = pd.DataFrame(dict(zip(headers, data)))
        
        return df.astype(str)

    def clean_table(self, table: pd.DataFrame):

        """clean any doc formatting chars
        """

        def strip_chars(text):

            return text.replace("\n", "").replace("\x0b", "").replace("\xa0", "").replace("\ufeff", "").replace("\u200b", "").replace("  ", " ").strip()

        table.columns = list(map(lambda x: strip_chars(x), table.columns))
        table = table.map(lambda x: (strip_chars(x)))

        return table

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