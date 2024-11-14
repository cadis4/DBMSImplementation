# server_commands.py

from db_operations import (
    create_database, insert_record, delete_record, list_tables, drop_database, 
    create_table, drop_table, extract_column_definitions, create_index, 
    list_databases, parse_column_definitions_manually
    #, convert_type
)

current_database = None

def process_command(command):
    global current_database
    tokens = command.strip().split()
    
    if not tokens:
        return "Invalid command."

    cmd = tokens[0].upper()

    # Show databases
    if cmd == "SHOW" and len(tokens) > 1 and tokens[1].upper() == "DATABASES":
        return list_databases()

    # Use database
    elif cmd == "USE" and len(tokens) > 1:
        current_database = tokens[1]  # Set the current active database
        return f"Switched to database {tokens[1]}."

    # Show tables
    elif cmd == "SHOW" and len(tokens) > 1 and tokens[1].upper() == "TABLES":
        if current_database:
            return list_tables(current_database)
        else:
            return "No database selected. Use 'USE <database_name>' to select a database."

    # Create database
    elif cmd == "CREATE" and len(tokens) > 2 and tokens[1].upper() == "DATABASE":
        return create_database(tokens[2])

    # Drop database
    elif cmd == "DROP" and len(tokens) > 2 and tokens[1].upper() == "DATABASE":
        return drop_database(tokens[2])

    # Create table
    elif cmd == "CREATE" and len(tokens) > 3 and tokens[1].upper() == "TABLE":
        if current_database:
            table_name = tokens[2]  # Expecting table name
            # Get the column definitions from the command
            if "(" in command and ")" in command:
                col_defs = extract_column_definitions(command) 
                columns, primary_key, foreign_key = parse_column_definitions_manually(col_defs)

                # Print columns just before table creation
                print("Final Columns for Creation:", columns)

            # Verify if foreign keys exist in the defined columns
                defined_columns = {col['name'] for col in columns}  # Create a set of column names
                for fk in foreign_key:
                    if fk['fk_col'] not in defined_columns:
                        return f"Can't assign nonexistent field '{fk['fk_col']}' as foreign key."
                    
           # Check if all primary keys exist in the columns
            for pk in primary_key:
                if not any(col['name'] == pk for col in columns):
                    return f"Error: Primary key '{pk}' does not exist in column definitions."
                
             # For composite keys, we need to check if all parts exist
            if isinstance(primary_key, list):
                for pk in primary_key:
                    if not any(col['name'] == pk for col in columns):
                        return f"Error: Primary key '{pk}' does not exist in column definitions."
                       
                return create_table(current_database, table_name, columns, primary_key, foreign_key)
            else:
                return "Invalid syntax for column definitions."
        else:
            return "No database selected. Use 'USE <database_name>' to select a database."

    # Drop table
    elif cmd == "DROP" and len(tokens) > 2 and tokens[1].upper() == "TABLE":
        if current_database:
            return drop_table(current_database, tokens[2])
        else:
            return "No database selected. Use 'USE <database_name>' to select a database."

    # Create index
    elif cmd == "CREATE" and len(tokens) > 5 and tokens[1].upper() == "INDEX" and tokens[3].upper() == "ON":
        if current_database:
            index_name = tokens[2]  # Index name
            table_name = tokens[4]  # Table name

            try:
                column_part = command[command.index("(")+1:command.index(")")]
                column_name = column_part.strip() 

            except ValueError:
                return "Invalid syntax. Make sure the column name is inside parentheses."

            if not column_name:
                return "Invalid column name syntax. Make sure it's in parentheses."

            # Check for UNIQUE keyword
            is_unique = 1 if "UNIQUE" in tokens else 0

            # Call the function to create the index
            return create_index(current_database, table_name, index_name, column_name, is_unique)
        else:
            return "No database selected. Use 'USE <database_name>' to select a database."

    if cmd == "INSERT" and len(tokens) > 2 and tokens[1].upper() == "INTO":
        table_name = tokens[2]
        
        # Extract values after "VALUES" keyword
        if "VALUES" in command.upper():
            values_part = command.upper().split("VALUES")[1].strip()
            values_part = values_part[1:-1]  # Remove surrounding parentheses
            value_items = values_part.split(",")
            columns, values = {}, {}

            # Extract column names from the table
            table_columns = command[command.index("(") + 1:command.index(")")].split(",")
            for i, col in enumerate(table_columns):
                col_name = col.strip()
                values[col_name] = value_items[i].strip().strip("'\"")  # Remove quotes

            return insert_record(table_name, values, current_database)


    # Inside the `process_command` function:
    elif cmd == "DELETE" and len(tokens) > 2 and tokens[1].upper() == "FROM":
        collection_name = tokens[2]
        primary_key_value = None

        # Check if a WHERE clause exists
        if "WHERE" in command.upper():
            where_part = command.split("WHERE")[1].strip()
            condition_items = where_part.split("AND")

            # Assuming only primary key condition is allowed
            if len(condition_items) == 1:
                col, val = condition_items[0].split("=")
                col = col.strip()
                primary_key_value = val.strip().strip("'\"")  # Strip quotes from the value

                # Check if column is the primary key
                if col.lower() != "key":
                    return "Error: Only primary key deletion is allowed."
            else:
                return "Error: Multiple conditions not allowed. Use only the primary key in WHERE."

        # Execute the delete command
        if primary_key_value is not None:
            return delete_record(collection_name, primary_key_value, current_database)
        else:
            return "Error: Invalid DELETE syntax. Specify a primary key in WHERE clause."
