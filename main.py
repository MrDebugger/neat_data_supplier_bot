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
    
    nul_rows = df[df.isnull().all(axis=1)].index.tolist()
    if not nul_rows:
        return [df]

    list_of_dataframes = [df.iloc[nul_rows[i]+1:nul_rows[i+1]] for i in range(len(nul_rows) - 1)]
    list_of_dataframes.insert(0, df.iloc[:nul_rows[0]])
    list_of_dataframes.append(df.iloc[nul_rows[-1]+1:])

    return list_of_dataframes

def clean_table(table, column_indexes):
    """Clean a single table by removing null columns and setting headers."""
    table = table.dropna(axis=1, how='all')
    if table.empty or not set(column_indexes.values()).intersection(set(table.columns)):
        return None

    valid_columns = {key: idx for key, idx in column_indexes.items() if idx in table.columns}
    table = table.loc[:, valid_columns.values()]
    table = table.dropna(how='all')

    new_indexes = {}
    for key, value in valid_columns.items():
        new_indexes[key] = table.columns.get_loc(value)

    for index, row in table.iterrows():
        if not row.isnull().any():
            table.columns = row
            table = table.drop(index)
            break
        table = table.drop(index)

    # remove all rows with the same values as the header
    table = table[~table.apply(lambda row: all(row == table.columns), axis=1)]
    if not table.empty:
        new_columns = {table.columns[idx]: key for key, idx in new_indexes.items()}
        table = table.rename(columns=new_columns)

    table = table.dropna(how='all')
    return table if not table.empty else None

def process_tables(df, column_indexes, start=None, end=None):
    """Process and clean all extracted tables from a DataFrame."""
    list_of_dataframes = extract_tables(df, start, end)
    cleaned_tables = [clean_table(table, column_indexes) for table in list_of_dataframes]
    return [table for table in cleaned_tables if table is not None]

def read_excel_with_multiple_tables(file_path, sheet_name='Master', columns={}, start=None, end=None):
    """Read an Excel file and return cleaned tables based on columns indexes."""
    df = load_excel(file_path, sheet_name)
    return process_tables(df, columns, start, end)

def read_json(json_path):
    """Read JSON file and return the data."""
    with open(json_path, 'r') as file:
        return json.load(file)

def columns_to_index(obj):
    """Convert column letters to zero-based column indexes."""
    columns = {}
    for key, value in obj.items():
        if isinstance(value, str) and 'Column' in value:
            column = value.split()[-1].upper().strip()
            index = sum((ord(char) - ord('A') + 1) * (26 ** exp) for exp, char in enumerate(reversed(column))) - 1
            columns[key] = index
    return columns

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
        column_indexes = columns_to_index(tab)
        start = tab.get('Start', None)
        end = tab.get('End', None)

        # Read the Excel file and process the tables
        cleaned_tables = read_excel_with_multiple_tables(file_path, sheet_name=sheet, columns=column_indexes, start=start, end=end)
        data = []

        # Display the cleaned tables
        for idx, table in enumerate(cleaned_tables):
            if idx == 0:
                data.append(table.columns.tolist())
            data.extend(table.values.tolist())
        print(f"Sheet: {sheet}")
        print(f"{'-' * 50}")
        print('Total tables:', idx + 1)
        print(data)

if __name__ == "__main__":
    main()
