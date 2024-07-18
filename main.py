import pandas as pd
import os
import json

def load_excel(file_path, sheet_name):
    """Load the Excel file and return a DataFrame."""
    return pd.read_excel(file_path, sheet_name=sheet_name, header=None)

def extract_tables(df, start=None, end=None):
    """Extract tables from a DataFrame by identifying null rows or using start and end rows."""
    if start is not None and end is not None:
        return [df.iloc[start-1:end]]
    else:
        nul_rows = list(df[df.isnull().all(axis=1)].index)
        list_of_dataframes = [] if len(nul_rows) > 0 else [df]

        for i in range(len(nul_rows) - 1):
            list_of_dataframes.append(df.iloc[nul_rows[i]+1:nul_rows[i+1], :])

        return list_of_dataframes

def clean_table(table, keywords):
    """Clean a single table by removing null columns and setting headers."""
    table = table.dropna(axis=1, how='all')

    if table.empty:
        return None
    
    if all(map(lambda x: isinstance(x, int), table.columns)):
        for index, row in table.iterrows():
            if not row.isnull().any():
                table.columns = row
                table = table.drop(index)
                break
            table = table.drop(index)

    table = table.dropna(how='all')

    if table.empty or not table.columns.isin(keywords).any():
        return None

    table = table.loc[:, table.columns.isin(keywords)]
    table = table.dropna(how='all')

    if table.empty:
        return None

    return table

def process_tables(df, keywords, start=None, end=None):
    """Process and clean all extracted tables from a DataFrame."""
    list_of_dataframes = extract_tables(df, start, end)
    cleaned_tables = [clean_table(table, keywords) for table in list_of_dataframes]
    return [table for table in cleaned_tables if table is not None]

def read_excel_with_multiple_tables(file_path, sheet_name='Master', keywords=['Part Number', 'UPC', 'Description', 'Cost'], start=None, end=None):
    """Read an Excel file and return cleaned tables based on keywords."""
    df = load_excel(file_path, sheet_name)
    cleaned_tables = process_tables(df, keywords, start, end)
    return cleaned_tables

def read_json(json_path):
    """Read JSON file and return the data."""
    with open(json_path, 'r') as file:
        return json.load(file)

def main():
    # Set the file path
    current_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(current_dir, 'data_20250715', 'Drexan Catalog.xlsx')
    json_path = os.path.join(current_dir, 'data_20250715', '10363-Drexan.json')

    # Read the JSON file
    data = read_json(json_path)

    # Process each tab in the JSON data
    tabs = data['excel_worksheet_tabs']
    for tab in tabs:
        sheet = tab['Worksheet Tab']
        keywords = [key for key, value in tab.items() if isinstance(value, str) and 'Column' in value]
        start = tab.get('Start', None)
        end = tab.get('End', None)

        # Read the Excel file and process the tables
        cleaned_tables = read_excel_with_multiple_tables(file_path, sheet_name=sheet, keywords=keywords, start=start, end=end)
        data = []
        # Display the cleaned tables
        for idx, table in enumerate(cleaned_tables):
            if idx == 0:
                data.append(table.columns.tolist())
            print(f"Table {idx+1}:")
            # list of list
            data.extend(table.values.tolist())
        print(data)
if __name__ == "__main__":
    main()
