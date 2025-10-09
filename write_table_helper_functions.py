def insert_rows(table_body, df, insert_index):

    """return request to insert len(df) rows to gdoc table

    args:
        table_body: table object returned by gdoc_client.read_table_body()
        df: pd.dataframe
        insert_index: first row for populated data (1 if single title row)
    """
    
    table_index = table_body["startIndex"]

    return [{
            'insertTableRow': {
                'tableCellLocation': {
                    'tableStartLocation': {'index': table_index},
                    'rowIndex': insert_index,
                },
                'insertBelow': True
            }
        }] * len(df)

def insert_text(table_body, df, first_row_index):

    """return request to insert text to inserted rows

    args:
        table_body: table object returned by gdoc_client.read_table_body()
        df: pd.dataframe
        first_row_index: first row for populated data (1 if single title row)
    """

    #reverse order so populating last to first row else subsequest location indexes are broken
    #write backwards rule described at https://developers.google.com/workspace/docs/api/how-tos/move-text#write_backwards
    
    #get insert index for each table cell
    cell_index = []
    rows = table_body["table"]["tableRows"][first_row_index:]
    rows.reverse()

    for row in rows:
        cells = row["tableCells"]
        cells.reverse()
        for cell in cells:
            cell_index.append(cell["startIndex"] + 1)

    #get cell text to be inserted
    cell_text = [item for row in [df.columns.tolist()]+df.values.tolist() for item in row]
    cell_text.reverse()

    request = [
            {'insertText': {'location': {'index': idx}, 'text': text}}
            for idx, text in zip(cell_index, cell_text)
            ]
                             
    return request

def replace_title_placeholder(placeholder: str, new_name: str):

    """return request to replace text in gdoc 
    """

    return [{"replaceAllText": {
                "containsText": {
                        "text": placeholder,
                        "matchCase": True
                        },
                "replaceText": new_name
                }
                }]
