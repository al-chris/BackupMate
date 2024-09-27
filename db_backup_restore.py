import json
import argparse
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, DateTime, Date, Integer, String, Float, Boolean, Text, Numeric, LargeBinary, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from datetime import datetime
from collections import defaultdict, deque
import time

# Mapping of SQLAlchemy types to string representations for serialization
SQLALCHEMY_TYPE_MAPPING = {
    Integer: 'Integer',
    String: 'String',
    Float: 'Float',
    Boolean: 'Boolean',
    DateTime: 'DateTime',
    Date: 'Date',
    Text: 'Text',
    Numeric: 'Numeric',
    LargeBinary: 'LargeBinary',
    # Add more types as needed
}

# Reverse mapping for deserialization
STRING_TO_SQLALCHEMY_TYPE = {
    'Integer': Integer,
    'String': String,
    'Float': Float,
    'Boolean': Boolean,
    'DateTime': DateTime,
    'Date': Date,
    'Text': Text,
    'Numeric': Numeric,
    'LargeBinary': LargeBinary,
    # Add more types as needed
}

def serialize_column(column):
    """
    Serializes a SQLAlchemy Column object into a dictionary.
    """
    col_dict = {
        'name': column.name,
        'type': SQLALCHEMY_TYPE_MAPPING.get(type(column.type), 'String'),  # Default to String if type not mapped
        'nullable': column.nullable,
        'primary_key': column.primary_key,
        'foreign_key': None
    }
    if column.foreign_keys:
        # Assuming single foreign key for simplicity
        fk = list(column.foreign_keys)[0]
        col_dict['foreign_key'] = {
            'column': fk.column.name,
            'table': fk.column.table.name
        }
    return col_dict

def deserialize_column(col_dict):
    """
    Deserializes a column dictionary back into a SQLAlchemy Column object.
    """
    col_type_str = col_dict['type']
    col_type = STRING_TO_SQLALCHEMY_TYPE.get(col_type_str, String)
    
    # Handle foreign keys
    if col_dict['foreign_key']:
        fk = ForeignKey(f"{col_dict['foreign_key']['table']}.{col_dict['foreign_key']['column']}")
        return Column(
            col_dict['name'], 
            col_type, 
            fk,  # Pass ForeignKey as a positional argument
            nullable=col_dict['nullable'],
            primary_key=col_dict['primary_key']
        )
    else:
        return Column(
            col_dict['name'], 
            col_type, 
            nullable=col_dict['nullable'],
            primary_key=col_dict['primary_key']
        )

def topological_sort(dependency_graph):
    """
    Performs a topological sort on the dependency graph.
    
    :param dependency_graph: Dict where key is table and value is set of tables it depends on.
    :return: List of tables sorted in dependency order or None if a cycle is detected.
    """
    in_degree = defaultdict(int)
    for deps in dependency_graph.values():
        for dep in deps:
            in_degree[dep] += 1

    queue = deque([table for table in dependency_graph if in_degree[table] == 0])

    sorted_list = []
    while queue:
        table = queue.popleft()
        sorted_list.append(table)
        for dependent in dependency_graph:
            if table in dependency_graph[dependent]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

    if len(sorted_list) != len(dependency_graph):
        return None  # Cycle detected
    return sorted_list

def backup_database(db_url, output_file):
    """
    Backs up the entire database schema and data to a JSON file.

    :param db_url: Database connection URL.
    :param output_file: Path to the output JSON file.
    """
    try:
        engine = create_engine(db_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)
        backup_data = {}

        with engine.connect() as connection:
            for table_name, table in metadata.tables.items():
                print(f"Backing up table: {table_name}")
                
                # Serialize schema
                columns = [serialize_column(col) for col in table.columns]
                
                # Serialize data
                select_stmt = table.select()
                result = connection.execute(select_stmt)
                rows = [dict(row) for row in result.mappings()]
                
                backup_data[table_name] = {
                    'schema': {
                        'columns': columns
                    },
                    'data': rows
                }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, indent=4, default=str)
        print(f"Backup successful! Data and schema saved to {output_file}")

    except SQLAlchemyError as e:
        print(f"Database error during backup: {e}")
    except Exception as e:
        print(f"Error during backup: {e}")

def restore_database(db_url, input_file, max_retries=5, retry_delay=5):
    """
    Restores the database schema and data from a JSON backup file by:
    1. Disabling foreign key constraints.
    2. Defining and creating all tables.
    3. Inserting data into all tables.
    4. Re-enabling foreign key constraints.
    
    :param db_url: Database connection URL.
    :param input_file: Path to the input JSON file.
    :param max_retries: Maximum number of retry attempts for locked database.
    :param retry_delay: Delay in seconds between retries.
    """
    try:
        engine = create_engine(db_url, connect_args={'timeout': 30})  # Increased timeout for SQLite
        metadata = MetaData()
        Session = sessionmaker(bind=engine)
        session = Session()

        with engine.connect() as connection:
            # Begin a transaction
            with connection.begin():
                # Disable foreign key constraints using text()
                connection.execute(text("PRAGMA foreign_keys = OFF;"))
                print("Foreign key constraints disabled.")

                # Load backup data
                with open(input_file, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)

                # Define all tables first
                for table_name, table_info in backup_data.items():
                    if table_name not in metadata.tables:
                        print(f"Defining table: {table_name}")
                        # Deserialize columns
                        columns = [deserialize_column(col) for col in table_info['schema']['columns']]
                        # Define new table
                        new_table = Table(table_name, metadata, *columns)
                    else:
                        print(f"Table {table_name} already defined in metadata.")

                # Create all tables at once
                metadata.create_all(engine)
                print("All tables created successfully.")

                # Insert data into all tables
                for table_name, table_info in backup_data.items():
                    table = Table(table_name, metadata, autoload_with=engine)
                    print(f"Restoring data for table: {table_name}")

                    # Optional: Clear existing data
                    connection.execute(table.delete())

                    if table_info['data']:
                        # Identify datetime columns
                        datetime_columns = [
                            column.name for column in table.columns
                            if isinstance(column.type, (DateTime, Date))
                        ]

                        # Process each row to convert datetime strings to datetime objects
                        processed_rows = []
                        for row in table_info['data']:
                            for col in datetime_columns:
                                if row[col] is not None:
                                    try:
                                        # Attempt to parse the datetime string
                                        row[col] = datetime.fromisoformat(row[col])
                                    except ValueError:
                                        # Handle other datetime formats if necessary
                                        row[col] = datetime.strptime(row[col], '%Y-%m-%d %H:%M:%S.%f')
                            processed_rows.append(row)

                        # Insert processed rows
                        connection.execute(table.insert(), processed_rows)

                # Re-enable foreign key constraints using text()
                connection.execute(text("PRAGMA foreign_keys = ON;"))
                print("Foreign key constraints enabled.")

        session.commit()
        print(f"Restore successful! Data and schema loaded from {input_file}")

    except OperationalError as e:
        if 'database is locked' in str(e):
            print("Database is locked. Please ensure no other processes are accessing the database and try again.")
        else:
            print(f"Operational error during restore: {e}")
    except SQLAlchemyError as e:
        print(f"Database error during restore: {e}")
    except Exception as e:
        print(f"Error during restore: {e}")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Backup and Restore Database to/from JSON")
    subparsers = parser.add_subparsers(dest='command', help='Commands: backup, restore')

    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Backup database to JSON')
    backup_parser.add_argument('--db-url', required=True, help='Database connection URL')
    backup_parser.add_argument('--output', required=True, help='Output JSON file path')

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore database from JSON')
    restore_parser.add_argument('--db-url', required=True, help='Database connection URL')
    restore_parser.add_argument('--input', required=True, help='Input JSON file path')

    return parser.parse_args()

def main():
    args = parse_arguments()
    if args.command == 'backup':
        backup_database(args.db_url, args.output)
    elif args.command == 'restore':
        restore_database(args.db_url, args.input)
    else:
        print("Please specify a command: backup or restore")
        print("Use -h for help.")

if __name__ == '__main__':
    main()
