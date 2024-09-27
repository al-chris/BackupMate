import json
import argparse
from sqlalchemy import (
    create_engine, MetaData, Table, Column, ForeignKey, DateTime, Date,
    Integer, String, Float, Boolean, Text, Numeric, LargeBinary, text,
    Enum, ARRAY, PrimaryKeyConstraint, UniqueConstraint, Index
)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from datetime import datetime
from collections import defaultdict, deque
import time
import logging

# Configure logging for better error tracking and debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mapping of SQLAlchemy types to detailed serialization information
SQLALCHEMY_TYPE_MAPPING = {
    Integer: {'name': 'Integer'},
    String: {'name': 'String', 'args': ['length']},
    Float: {'name': 'Float'},
    Boolean: {'name': 'Boolean'},
    DateTime: {'name': 'DateTime'},
    Date: {'name': 'Date'},
    Text: {'name': 'Text'},
    Numeric: {'name': 'Numeric'},
    LargeBinary: {'name': 'LargeBinary'},
    Enum: {'name': 'Enum', 'args': ['enum_values']},
    ARRAY: {'name': 'ARRAY', 'args': ['item_type']},
    # Add more types as needed
}

# Reverse mapping for deserialization with type-specific handling
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
    'Enum': Enum,
    'ARRAY': ARRAY,
    # Add more types as needed
}

def serialize_column(column):
    """
    Serializes a SQLAlchemy Column object into a dictionary, including type-specific options,
    multiple foreign keys, and additional attributes like default values and uniqueness.
    """
    try:
        col_type_info = SQLALCHEMY_TYPE_MAPPING.get(type(column.type))
        if not col_type_info:
            raise ValueError(f"Unsupported column type: {type(column.type)} for column {column.name}")

        # Handle type-specific arguments
        col_type = {'type': col_type_info['name']}
        if 'args' in col_type_info:
            for arg in col_type_info['args']:
                if arg == 'length' and hasattr(column.type, 'length'):
                    col_type['length'] = column.type.length
                elif arg == 'enum_values' and isinstance(column.type, Enum):
                    col_type['enum_values'] = list(column.type.enums)
                elif arg == 'item_type' and isinstance(column.type, ARRAY):
                    # Assuming item_type is a simple type
                    col_type['item_type'] = column.type.item_type.__class__.__name__
                # Add other type-specific arguments as needed

        # Serialize column attributes
        col_dict = {
            'name': column.name,
            'type': col_type,
            'nullable': column.nullable,
            'primary_key': column.primary_key,
            'unique': column.unique,
            'default': str(column.default.arg) if column.default else None,
            'foreign_keys': [],
            'index': any(index.columns for index in column.indexes)
        }

        # Serialize multiple foreign keys if present
        if column.foreign_keys:
            for fk in column.foreign_keys:
                col_dict['foreign_keys'].append({
                    'column': fk.column.name,
                    'table': fk.column.table.name,
                    'schema': fk.column.table.schema  # Include schema for robustness
                })

        return col_dict
    except Exception as e:
        logger.error(f"Error serializing column {column.name}: {e}")
        raise

def deserialize_column(col_dict, metadata):
    """
    Deserializes a column dictionary back into a SQLAlchemy Column object, handling type-specific
    options and multiple foreign keys.
    """
    try:
        col_type_info = col_dict['type']
        col_type_name = col_type_info['type']
        col_type_class = STRING_TO_SQLALCHEMY_TYPE.get(col_type_name)
        if not col_type_class:
            raise ValueError(f"Unsupported column type: {col_type_name} for column {col_dict['name']}")

        # Handle type-specific arguments
        type_args = {}
        if col_type_name == 'String' and 'length' in col_type_info:
            type_args['length'] = col_type_info['length']
        elif col_type_name == 'Enum' and 'enum_values' in col_type_info:
            type_args['enum_values'] = col_type_info['enum_values']
        elif col_type_name == 'ARRAY' and 'item_type' in col_type_info:
            item_type_class = STRING_TO_SQLALCHEMY_TYPE.get(col_type_info['item_type'], String)
            type_args['item_type'] = item_type_class
        # Add other type-specific arguments as needed

        # Reconstruct the column type with specific arguments
        if col_type_name == 'Enum':
            col_type = Enum(*col_type_info.get('enum_values', []))
        elif col_type_name == 'ARRAY':
            col_type = ARRAY(type_args['item_type'])
        else:
            col_type = col_type_class(**type_args) if type_args else col_type_class()

        # Handle multiple foreign keys
        foreign_keys = []
        for fk in col_dict.get('foreign_keys', []):
            fk_str = f"{fk['schema'] + '.' if fk.get('schema') else ''}{fk['table']}.{fk['column']}"
            foreign_keys.append(ForeignKey(fk_str))

        # Construct the Column with all attributes
        column_args = [
            col_dict['name'],
            col_type,
            *foreign_keys  # Supports multiple foreign keys
        ]

        column_kwargs = {
            'nullable': col_dict.get('nullable', True),
            'primary_key': col_dict.get('primary_key', False),
            'unique': col_dict.get('unique', False),
            'default': col_dict['default']
        }

        return Column(*column_args, **column_kwargs)
    except Exception as e:
        logger.error(f"Error deserializing column {col_dict['name']}: {e}")
        raise

def topological_sort(dependency_graph):
    """
    Performs a topological sort on the dependency graph to determine the order of table creation.
    Returns a sorted list of tables or None if a cycle is detected.
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
        logger.error("Cycle detected in dependency graph.")
        return None  # Cycle detected
    return sorted_list

def backup_database(db_url, output_file, include_relationships=False, version="1.0"):
    """
    Backs up the entire database schema and data to a JSON file, including expanded type support,
    multiple foreign keys, additional column attributes, composite keys, and optionally relationships.

    :param db_url: Database connection URL.
    :param output_file: Path to the output JSON file.
    :param include_relationships: Whether to include relationships in the backup.
    :param version: Backup schema version.
    """
    try:
        engine = create_engine(db_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)
        backup_data = {
            'version': version,
            'tables': {}
        }

        with engine.connect() as connection:
            for table_name, table in metadata.tables.items():
                logger.info(f"Backing up table: {table_name}")

                # Serialize schema
                columns = [serialize_column(col) for col in table.columns]

                # Serialize constraints
                primary_keys = [col.name for col in table.primary_key.columns]
                unique_constraints = [
                    [col.name for col in constraint.columns]
                    for constraint in table.constraints
                    if isinstance(constraint, UniqueConstraint)
                ]
                indexes = [index.name for index in table.indexes]

                # Serialize data
                select_stmt = table.select()
                result = connection.execute(select_stmt)
                rows = [dict(row) for row in result.mappings()]

                backup_data['tables'][table_name] = {
                    'schema': {
                        'columns': columns,
                        'primary_keys': primary_keys,
                        'unique_constraints': unique_constraints,
                        'indexes': indexes
                    },
                    'data': rows
                }

            # Optionally serialize relationships
            if include_relationships:
                relationships = {}
                for table in metadata.tables.values():
                    rels = []
                    for fk in table.foreign_keys:
                        rels.append({
                            'column': fk.parent.name,
                            'referenced_table': fk.column.table.name,
                            'referenced_column': fk.column.name,
                            'schema': fk.column.table.schema
                        })
                    relationships[table.name] = rels
                backup_data['relationships'] = relationships

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, indent=4, default=str)
        logger.info(f"Backup successful! Data and schema saved to {output_file}")

    except SQLAlchemyError as e:
        logger.error(f"Database error during backup: {e}")
    except Exception as e:
        logger.error(f"Error during backup: {e}")

def restore_database(db_url, input_file, max_retries=5, retry_delay=5):
    """
    Restores the database schema and data from a JSON backup file by:
    1. Disabling foreign key constraints.
    2. Defining and creating all tables.
    3. Inserting data into all tables.
    4. Re-enabling foreign key constraints.

    Handles multiple foreign keys, composite keys, and includes retry logic for locked databases.

    :param db_url: Database connection URL.
    :param input_file: Path to the input JSON file.
    :param max_retries: Maximum number of retry attempts for locked database.
    :param retry_delay: Delay in seconds between retries.
    """
    retries = 0
    while retries < max_retries:
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
                    logger.info("Foreign key constraints disabled.")

                    # Load backup data
                    with open(input_file, 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)

                    version = backup_data.get('version', '1.0')
                    logger.info(f"Restoring from backup version: {version}")

                    # Define all tables first
                    for table_name, table_info in backup_data['tables'].items():
                        if table_name not in metadata.tables:
                            logger.info(f"Defining table: {table_name}")
                            # Deserialize columns
                            columns = [deserialize_column(col, metadata) for col in table_info['schema']['columns']]
                            # Define new table
                            new_table = Table(table_name, metadata, *columns)

                            # Add primary key constraints
                            primary_keys = table_info['schema'].get('primary_keys', [])
                            if primary_keys:
                                new_table.append_constraint(PrimaryKeyConstraint(*primary_keys))

                            # Add unique constraints
                            unique_constraints = table_info['schema'].get('unique_constraints', [])
                            for uc in unique_constraints:
                                new_table.append_constraint(UniqueConstraint(*uc))

                            # Add indexes
                            indexes = table_info['schema'].get('indexes', [])
                            for idx in indexes:
                                index_columns = [col for col in new_table.c if col.name in idx]
                                new_table.append_constraint(Index(idx, *index_columns))
                        else:
                            logger.info(f"Table {table_name} already defined in metadata.")

                    # Create all tables at once
                    metadata.create_all(engine)
                    logger.info("All tables created successfully.")

                    # Insert data into all tables
                    # To respect dependencies, perform topological sort
                    dependency_graph = defaultdict(set)
                    for table_name, table_info in backup_data['tables'].items():
                        for fk in table_info['schema']['columns']:
                            for foreign_key in fk.get('foreign_keys', []):
                                dependency_graph[table_name].add(foreign_key['table'])

                    sorted_tables = topological_sort(dependency_graph)
                    if not sorted_tables:
                        raise ValueError("Cannot perform restore due to cyclic dependencies.")

                    for table_name in sorted_tables:
                        table_info = backup_data['tables'][table_name]
                        table = Table(table_name, metadata, autoload_with=engine)
                        logger.info(f"Restoring data for table: {table_name}")

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
                    logger.info("Foreign key constraints enabled.")

            session.commit()
            logger.info(f"Restore successful! Data and schema loaded from {input_file}")
            return

        except OperationalError as e:
            if 'database is locked' in str(e):
                logger.warning("Database is locked. Retrying...")
                retries += 1
                time.sleep(retry_delay)
            else:
                logger.error(f"Operational error during restore: {e}")
                break
        except SQLAlchemyError as e:
            logger.error(f"Database error during restore: {e}")
            break
        except Exception as e:
            logger.error(f"Error during restore: {e}")
            break

    logger.error("Restore failed after maximum retries.")

def parse_arguments():
    """
    Parses command-line arguments for backup and restore operations.
    """
    parser = argparse.ArgumentParser(description="Backup and Restore Database to/from JSON")
    subparsers = parser.add_subparsers(dest='command', help='Commands: backup, restore')

    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Backup database to JSON')
    backup_parser.add_argument('--db-url', required=True, help='Database connection URL')
    backup_parser.add_argument('--output', required=True, help='Output JSON file path')
    backup_parser.add_argument('--include-relationships', action='store_true', help='Include relationships in backup')
    backup_parser.add_argument('--version', default="1.0", help='Backup version')

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore database from JSON')
    restore_parser.add_argument('--db-url', required=True, help='Database connection URL')
    restore_parser.add_argument('--input', required=True, help='Input JSON file path')
    restore_parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retry attempts for locked database')
    restore_parser.add_argument('--retry-delay', type=int, default=5, help='Delay in seconds between retries')

    return parser.parse_args()

def main():
    """
    Main function to execute backup or restore based on command-line arguments.
    """
    args = parse_arguments()
    if args.command == 'backup':
        backup_database(
            db_url=args.db_url, 
            output_file=args.output, 
            include_relationships=args.include_relationships,
            version=args.version
        )
    elif args.command == 'restore':
        restore_database(
            db_url=args.db_url, 
            input_file=args.input, 
            max_retries=args.max_retries, 
            retry_delay=args.retry_delay
        )
    else:
        logger.info("Please specify a command: backup or restore")
        logger.info("Use -h for help.")

if __name__ == '__main__':
    main()
