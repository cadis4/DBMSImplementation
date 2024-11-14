# db_operations.py
from db_catalog import load_catalog, save_catalog
import xml.etree.ElementTree as ET
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json

# Insert a new record into a specified table in the MongoDB database

# client = MongoClient("mongodb://localhost:27017/")

# def convert_type(value):
#     # Try to convert to integer, then to float, or keep as string if neither works
#     try:
#         return int(value)
#     except ValueError:
#         try:
#             return float(value)
#         except ValueError:
#             return value.strip("'\"")  # Remove any extra quotes around strings


client = MongoClient("mongodb://localhost:27017/")

def write_to_json(collection, json_file_path):
    
    documents = collection.find({}, {"_id": 0})  
    records = list(documents)  
    
    with open(json_file_path, "w") as json_file:
        json.dump(records, json_file, indent=4)


import xml.etree.ElementTree as ET

def load_table_schema(db_name, table_name):
    """Load the allowed schema and primary key for a specific table in the given database from the XML catalog."""
    tree = ET.parse("DataBase.xml")  
    root = tree.getroot()
    
    # Navigate to the correct database and table
    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            for table in db.find("Tables").findall("Table"):
                if table.get("tableName") == table_name:
                    # Extract attribute names
                    attributes = [attr.get("attributeName") for attr in table.find("Structure").findall("Attribute")]
                    
                    # Extract primary key fields
                    primary_keys = [pk.text for pk in table.find("primaryKey").findall("pkAttribute")]
                    
                    return attributes, primary_keys  # Return list of attribute names and primary key fields
    return None, None  # Return None if table or database is not found

def insert_record(table_name, values, db_name):
    if not db_name:
        return "No database selected. Use 'USE <database_name>' to select a database."
    
    # Load table from XML 
    allowed_fields, primary_keys = load_table_schema(db_name, table_name)
    if allowed_fields is None or primary_keys is None:
        return f"Table '{table_name}' or database '{db_name}' does not exist in the catalog."
    
    # Validation
    input_fields = set(values.keys())
    if not input_fields.issubset(set(allowed_fields)):
        invalid_fields = input_fields - set(allowed_fields)
        return f"Error: Invalid field(s) {', '.join(invalid_fields)} for table '{table_name}'."
    
    # Access the MongoDB 
    db = client[db_name]
    collection = db[table_name]
    
    # Separate primary key 
    primary_key_values = [str(values[pk]) for pk in primary_keys if pk in values]
    if len(primary_key_values) != len(primary_keys):
        return f"Error: Missing primary key value(s) for table '{table_name}'."

    # composite key
    composite_key = '#'.join(primary_key_values)

    # Concatenate the other attr (no pk)
    non_primary_values = {k: v for k, v in values.items() if k not in primary_keys}
    concatenated_values = '#'.join(str(v) for v in non_primary_values.values())
    
    document = {
        "_id": composite_key,  
        "key": composite_key,
        "value": concatenated_values
    }
    
    # Insert the record into MongoDB, handling duplicates
    try:
        # validation for duplicates
        if collection.find_one({"_id": composite_key}):
            return "Error: Record with this primary key already exists."
        
        # Insert 
        collection.insert_one(document)
        json_file_path = f"{table_name}.json"  # for JSON file 
        write_to_json(collection, json_file_path)
        
        return f"Record inserted successfully into table {table_name}."
    except Exception as e:
        return f"Error inserting record: {str(e)}"





def delete_record(table_name, primary_key_value, db_name):
    if not db_name:
        return "No database selected. Use 'USE <database_name>' to select a database."

    # Access the MongoDB 
    db = client[db_name]
    collection = db[table_name]

    # Delete record based on pk 
    try:
        result = collection.delete_one({"key": primary_key_value})
        if result.deleted_count == 0:
            return "No record found with the given primary key."

        # update the JSON file
        json_file_path = f"{table_name}.json"
        write_to_json(collection, json_file_path)  
        return f"Record with key {primary_key_value} deleted successfully from table {table_name}."
    except Exception as e:
        return f"Error deleting record: {str(e)}"


            
def create_database(db_name):
    # Load the existing catalog
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()
    # Check if the database already exists in the XML catalog
    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            return f"Database {db_name} already exists."

    # Create new database entry in the XML catalog
    new_db = ET.Element("DataBase", {"dataBaseName": db_name})
    ET.SubElement(new_db, "Tables")
    root.append(new_db)
    save_catalog(tree)

    # Create a corresponding MongoDB database
    client[db_name]  # This line effectively creates the database in MongoDB if it doesn't exist.

    return f"Database {db_name} created successfully."

def drop_database(db_name):
    # Load and modify XML catalog
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()
    db_found = False

    # Check if the database exists in the XML catalog and remove it
    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            root.remove(db)
            db_found = True
            save_catalog(tree)
            break

    if not db_found:
        return f"Database {db_name} does not exist in catalog."

    # Drop the database from MongoDB
    try:
        client.drop_database(db_name)
        return f"Database {db_name} dropped successfully from catalog"
    except Exception as e:
        return f"Error dropping database from MongoDB: {str(e)}"

# List all databases
def list_databases():
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."
    
    databases = []
    for db in tree.findall("DataBase"):
        databases.append(db.get("dataBaseName"))
    
    return "Databases: " + ", ".join(databases) if databases else "No databases found."

# List all tables in a specified database
def list_tables(db_name):
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()
    database = None

    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            database = db
            break

    if not database:
        return f"Database {db_name} does not exist."

    tables = []
    for tbl in database.find("Tables").findall("Table"):
        tables.append(tbl.get("tableName"))
    
    return "Tables in " + db_name + ": " + ", ".join(tables) if tables else f"No tables found in {db_name}."

def create_table(db_name, table_name, columns, primary_key, foreign_keys=[]):
    print("Starting create_table...")  # comm
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()

    # Find the db
    database = None
    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            database = db
            break

    if database is None:
        return f"Database {db_name} does not exist."

    # Check if the table already exists
    for tbl in database.find("Tables").findall("Table"):
        if tbl.get("tableName") == table_name:
            return f"Table {table_name} already exists in database {db_name}."

    # Debug: Print columns before calculating row_length
    print("Columns before row length calculation:", columns)

    # Calculate row length by summing the lengths of all attributes
    try:
        # Only sum positive lengths and ensure they're integers
        row_length = sum(c['length'] for c in columns if isinstance(c['length'], int) and c['length'] > 0)  
        # Debug: Print calculated row_length
        print("Calculated Row Length:", row_length)
    except Exception as e:
        return f"Error calculating row length: {e}"
    
    # Validate foreign keys
    for fk in foreign_keys:
        ref_table = fk['ref_table']
        ref_col = fk['ref_col']
        
        # Locate the referenced table
        ref_table_value = None
        ref_table_element = None
        for tbl in database.find("Tables").findall("Table"):
            table_name_finder = tbl.get("tableName")
            ref_table_value = ref_table.split('(')[0]
            if table_name_finder == ref_table_value:
                ref_table_element = tbl
                print(f"Found referenced table: {ref_table_value}")  # Debug print
                break

        if ref_table_element is None:
            return f"Referenced table {ref_table_value} does not exist for foreign key."

        # Verify the referenced column is part of the primary key
        ref_primary_key = ref_table_element.find("primaryKey")
        ref_key_columns = [pk.text for pk in ref_primary_key.findall("pkAttribute")]

        print(f"Referenced table {ref_table} primary keys: {ref_key_columns}")  # Debug print

        if ref_col not in ref_key_columns:
            return f"Referenced column {ref_col} is not a primary key in table {ref_table}."

        
    # Create new table element
    new_table = ET.Element("Table", {
        "tableName": table_name, 
        "fileName": f"{table_name}.bin", 
        "rowLength": str(row_length)
    })
    
    structure = ET.SubElement(new_table, "Structure")

    # Add columns to the table structure
    for col in columns:
        # Ensure that base_type is set correctly and length is handled
        base_type = col['type']
        length_value = col['length'] if base_type.lower() == "varchar" else ''  # Only include length for varchar

        # Create the Attribute element with the correct type and length
        ET.SubElement(structure, "Attribute", {
            "attributeName": col['name'],
            "type": base_type,
            "length": str(length_value) if isinstance(length_value, int) and length_value > 0 else '',
            "isnull": str(col['isnull'])
        })

        print(f"Added Attribute: {col['name']}, Type: {base_type}, Length: {length_value}, IsNull: {col['isnull']}")  # COMMsho

    # Add primary key
    primary_key_el = ET.SubElement(new_table, "primaryKey")
    for pk in primary_key:
        ET.SubElement(primary_key_el, "pkAttribute").text = pk

    # Check if primary key was added
    print(f"Primary key added: {primary_key}")  # COMM

    # Add foreign keys if there are any
    if foreign_keys:
        foreign_keys_el = ET.SubElement(new_table, "foreignKeys")
        for fk in foreign_keys:
            fk_el = ET.SubElement(foreign_keys_el, "foreignKey")
            ET.SubElement(fk_el, "fkAttribute").text = fk['fk_col']
            references = ET.SubElement(fk_el, "references")
            ET.SubElement(references, "refTable").text = fk['ref_table'].split('(')[0]
            ET.SubElement(references, "refAttribute").text = fk['ref_col']

    # Append the new table to the database's table list
    database.find("Tables").append(new_table)
    save_catalog(tree)

    print(f"Table {table_name} created successfully in database {db_name}.")  # comm
    return f"Table {table_name} created successfully in database {db_name}."

def drop_table(db_name, table_name):
    # Load the catalog and find the specified database
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()
    database = None
    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            database = db
            break

    if not database:
        return f"Database '{db_name}' does not exist."

    # Find the table to be dropped
    table = None
    for tbl in database.find("Tables").findall("Table"):
        if tbl.get("tableName") == table_name:
            table = tbl
            break

    if not table:
        return f"Table '{table_name}' does not exist in database '{db_name}'."

    # Gather primary keys of the table to be dropped
    primary_keys = set()
    primary_key_element = table.find("primaryKey")
    if primary_key_element is not None:
        primary_keys = {pk.text for pk in primary_key_element.findall("pkAttribute")}
    else:
        return f"Error: Table '{table_name}' has no primary key defined."

    # Check for any foreign key references in other tables
    for tbl in database.find("Tables").findall("Table"):
        if tbl.get("tableName") != table_name:  # Skip the table being dropped
            foreign_keys_element = tbl.find("foreignKeys")
            if foreign_keys_element is not None:
                for fk in foreign_keys_element.findall("foreignKey"):
                    ref_table_elem = fk.find("references/refTable")
                    ref_attribute_elem = fk.find("references/refAttribute")

                    # Ensure both refTable and refAttribute are present
                    if ref_table_elem is not None and ref_attribute_elem is not None:
                        ref_table = ref_table_elem.text
                        ref_column = ref_attribute_elem.text

                        # Check if this foreign key references the table to be dropped
                        if ref_table == table_name and ref_column in primary_keys:
                            return (
                                f"Cannot drop table '{table_name}'; it is referenced by a foreign key "
                                f"in table '{tbl.get('tableName')}', column '{ref_column}'."
                            )

    # No foreign key references found, safe to drop the table from XML catalog
    database.find("Tables").remove(table)
    save_catalog(tree)

    # Also drop the table (collection) from MongoDB
    try:
        db = client[db_name]  # Access the database in MongoDB
        if table_name in db.list_collection_names():
            db.drop_collection(table_name)  # Drop the collection for the table
            return f"Table '{table_name}' dropped successfully from database '{db_name}'."
        else:
            return f"Table '{table_name}' does not exist in MongoDB database '{db_name}'."
    except Exception as e:
        return f"Error dropping table '{table_name}' from MongoDB: {str(e)}"



# Create an index for a table in the specified database
def create_index(db_name, table_name, index_name, column_name, is_unique, index_type="BTree"):
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()

    database = None
    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            database = db
            break

    if not database:
        return f"Database {db_name} does not exist."

    table = None
    for tbl in database.find("Tables").findall("Table"):
        if tbl.get("tableName") == table_name:
            table = tbl
            break

    if not table:
        return f"Table {table_name} does not exist in database {db_name}."

    # Check if the index already exists or create one now
    index_files = table.find("IndexFiles")
    if index_files is None:
        index_files = ET.SubElement(table, "IndexFiles")

    for index in index_files.findall("IndexFile"):
        if index.get("indexName") == f"{index_name}.ind":
            return f"Index {index_name} already exists on table {table_name}."

    new_index = ET.SubElement(index_files, "IndexFile", {
        "indexName": f"{index_name}.ind",
        "keyLength": "30",  
        "isUnique": str(int(is_unique)),
        "indexType": index_type
    })
    index_attributes = ET.SubElement(new_index, "IndexAttributes")
    ET.SubElement(index_attributes, "IAttribute").text = column_name

    save_catalog(tree)

    return f"Index {index_name} on {column_name} created successfully for table {table_name}."

def parse_column_definitions_manually(column_definitions):
    columns = []
    primary_key = []
    foreign_keys = []  # List to store foreign keys
    print("Enter parse function", column_definitions)
    
    for col in column_definitions:
        col = col.strip()
        
        # Check for composite PRIMARY KEY declaration
        if "PRIMARY KEY" in col:
            if "(" in col and ")" in col:
                pk_cols_part = col[col.index("(")+1:col.index(")")]  # Get the columns in parentheses
                pk_columns = pk_cols_part.split(',')
                primary_key.extend([pk_col.strip() for pk_col in pk_columns])  # Add all columns to PK list
                print(f"Found composite primary key: {primary_key}")  # Debug output
            continue  
        
        # Check for individual column PRIMARY KEY declaration
        if "PRIMARY" in col and "KEY" not in col:  
            pk_part = col.split('PRIMARY')[0].strip()
            pk_name = pk_part.split()[0]  # column name
            primary_key.append(pk_name)  # Add PK to the list
            print(f"Found primary key: {pk_name}")  # COMM
            col = pk_part.strip()  # Remove PK part for further processing

        # Check for FOREIGN KEY declaration
        if "FOREIGN KEY" in col:
            # Extract the column name inside the parentheses
            fk_col_part = col[col.index("(")+1:col.index(")")].strip()
            fk_column = fk_col_part.split()[0]
            print(f"Found foreign key: {fk_column}")  # COMM

            # Find the REFERENCES part to get the referenced table and column
            if "REFERENCES" in col:
                ref_part = col.split("REFERENCES")[1].strip()
                ref_table = ref_part.split()[0]  # The first part is the table name
                ref_column = ref_part[ref_part.index("(")+1:ref_part.index(")")].strip()  # Extract referenced column

                # Add foreign key information to the list
                foreign_keys.append({
                    'fk_col': fk_column,
                    'ref_table': ref_table,
                    'ref_col': ref_column
                })

                print(f"Added foreign key: {fk_column}, References: {ref_table}({ref_column})")  # comm
            continue 

        # Split on spaces and parentheses
        parts = col.split()
        column_name = parts[0]  # The first part is always the column name
        
        # Initialize data_type and length
        data_type = ""
        length = 0  # Default length
        
        if len(parts) > 1:
            data_type = parts[1]
            print(f"Data type found: {data_type}")  # comm

            if '(' in data_type:
                try:
                    # Split to get the base type and the length
                    data_type, len_part = data_type.split('(')
                    length_str = len_part.strip(')')  # Clean up to remove the closing parenthesis

                    if length_str:  # Ensure length_str is not empty
                        length = int(length_str)  # Convert to int
                    else:
                        print(f"Warning: Length for {column_name} is empty. Defaulting to 0.")
                except ValueError as e:
                    print(f"Error parsing length for {column_name}: {e}. Setting length to 0.")
                    length = 0  # Default to 0 in case of parsing error
            else:
                # If data_type is varchar but without length specified
                if data_type.lower() == 'varchar':
                    print(f"Warning: Length for {column_name} is not specified. Defaulting to 0.")
                    length = 0  

        # Append to columns list
        columns.append({
            'name': column_name,
            'type': data_type,
            'length': length,  # Ensure length is stored as an integer
            'isnull': 0  # Assuming not null for simplicity; adjust as necessary
        })

        print(f"Added column: {column_name}, Type: {data_type}, Length: {length}")  # Debug output

    print("From parse:", columns)  # Final columns output for debugging
    print("Foreign keys found:", foreign_keys)  # Debug output for foreign keys
    return columns, primary_key, foreign_keys  # Return columns, primary keys, and foreign keys

def extract_column_definitions(command):
    # Extract everything between the first opening and the last closing parenthesis
    start = command.index("(") + 1
    end = command.rindex(")")
    columns_string = command[start:end].strip()

    # Split the column definitions by commas, but handle cases like "varchar(5)"
    col_defs = []
    current_col = ""
    inside_parentheses = False

    for char in columns_string:
        if char == "(":
            inside_parentheses = True
        elif char == ")":
            inside_parentheses = False
        
        if char == "," and not inside_parentheses:
            col_defs.append(current_col.strip())  
            current_col = ""  # Reset for the next column
        else:
            current_col += char  # Continue building the current column
    
    # Append the last column 
    if current_col:
        col_defs.append(current_col.strip())

    return col_defs

# List all tables in a specified database
def list_tables(db_name):
    tree = load_catalog()
    if tree is None:
        return "Failed to load catalog."

    root = tree.getroot()
    database = None

    for db in root.findall("DataBase"):
        if db.get("dataBaseName") == db_name:
            database = db
            break

    if not database:
        return f"Database {db_name} does not exist."

    tables = []
    for tbl in database.find("Tables").findall("Table"):
        tables.append(tbl.get("tableName"))
    
    return "Tables in " + db_name + ": " + ", ".join(tables) if tables else f"No tables found in {db_name}."
