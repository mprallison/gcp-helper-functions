import pandas as pd
import numpy as np

def read_doc_text(doc):

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

def read_table_section(doc, table_id):

    """return table element containing table_id
    """

    sections = doc['body']['content']
    for section in sections:
        if table_id in str(section):
            found_section=True
            return section
    
    print(f'table_id "{table_id}" not found in a gdoc table')
    
    return None

def read_table_textruns(doc, table_id, header_row_index, preserve_format=False):

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

    section = read_table_section(doc, table_id)
    
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

def read_doc_table(doc, table_id, header_row_index, preserve_format=False):

    """return table in gdoc as list of cell textruns and count of columns
    args:
        doc: object returned by read_doc()
        table: unique table tag found somewhere in table content
        header_row_index: row index of header row (1 if single title row above headers)
        preserve_format: True if doc contains suggested changes and changes are to be written back to doc
    """

    from gdoc_helper_functions import read_table_textruns

    cells, column_n = read_table_textruns(doc, table_id, header_row_index, preserve_format)

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

def clean_table(table):

    """clean any doc formatting chars
    """

    def strip_chars(text):

        return text.replace("\n", "").replace("\x0b", "").replace("\xa0", "").replace("\ufeff", "").replace("\u200b", "").replace("  ", " ").strip()

    table.columns = list(map(lambda x: strip_chars(x), table.columns))
    table = table.map(lambda x: (strip_chars(x)))

    return table