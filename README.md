# Database Backup and Restore Script

A Python script to **backup** all data from a SQL database into a JSON file and **restore** it back into the database in case of data loss or database clearing.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Backup the Database](#backup-the-database)
  - [Restore the Database](#restore-the-database)
- [Examples](#examples)
- [Notes & Best Practices](#notes--best-practices)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- **Backup**: Extracts all data from the specified SQL database and saves it as a structured JSON file.
- **Restore**: Loads data from a JSON backup file back into the SQL database.
- **Database Agnostic**: Supports multiple SQL databases via SQLAlchemy (e.g., SQLite, MySQL, PostgreSQL).
- **Command-Line Interface**: Easy to use with command-line arguments for flexibility.

## Prerequisites

Before using the script, ensure you have the following:

1. **Python 3.x**: Installed on your system. Download from [python.org](https://www.python.org/downloads/).
2. **Database Access**: Credentials and access rights to the SQL database you intend to backup or restore.
3. **Required Python Libraries**:
   - `SQLAlchemy`: For database interactions.
   - Database-specific drivers (optional, depending on your database).

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/yourusername/db-backup-restore.git
   cd db-backup-restore
   ```

2. **Install Required Python Libraries**:

   It's recommended to use a virtual environment.

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install sqlalchemy
   ```

3. **Install Database-Specific Drivers**:

   Depending on your database, install the appropriate driver:

   - **SQLite**: No additional driver needed (included with Python).
   - **MySQL**:

     ```bash
     pip install pymysql
     ```

   - **PostgreSQL**:

     ```bash
     pip install psycopg2
     ```

   - **Other Databases**: Refer to [SQLAlchemy's Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html) for more options.

## Configuration

The script requires a **Database Connection URL** to connect to your SQL database. The format of this URL varies based on the database you are using.

### Database URL Formats

- **SQLite**:

  ```
  sqlite:///path_to_your_db.sqlite
  ```

  *Example*:

  ```
  sqlite:///./mydatabase.sqlite
  ```

- **MySQL**:

  ```
  mysql+pymysql://username:password@host:port/database_name
  ```

  *Example*:

  ```
  mysql+pymysql://user:pass@localhost:3306/mydatabase
  ```

- **PostgreSQL**:

  ```
  postgresql+psycopg2://username:password@host:port/database_name
  ```

  *Example*:

  ```
  postgresql+psycopg2://user:pass@localhost:5432/mydatabase
  ```

**Replace** `username`, `password`, `host`, `port`, and `database_name` with your actual database credentials and details.

## Usage

The script provides two main functionalities: **backup** and **restore**. Use command-line arguments to specify the desired operation and necessary parameters.

### Backup the Database

Creates a JSON file containing all the data from your database.

**Command Syntax**:

```bash
python db_backup_restore.py backup --db-url "your_database_url" --output backup.json
```

**Parameters**:

- `backup`: The command to initiate the backup process.
- `--db-url`: The database connection URL.
- `--output`: The path where the JSON backup file will be saved.

### Restore the Database

Loads data from a JSON backup file back into your database.

**Command Syntax**:

```bash
python db_backup_restore.py restore --db-url "your_database_url" --input backup.json
```

**Parameters**:

- `restore`: The command to initiate the restore process.
- `--db-url`: The database connection URL.
- `--input`: The path to the JSON backup file.

## Examples

### 1. Backup a SQLite Database

```bash
python db_backup_restore.py backup --db-url "sqlite:///./mydatabase.sqlite" --output backup.json
```

### 2. Restore a SQLite Database

```bash
python db_backup_restore.py restore --db-url "sqlite:///./mydatabase.sqlite" --input backup.json
```

### 3. Backup a MySQL Database

```bash
python db_backup_restore.py backup --db-url "mysql+pymysql://user:pass@localhost:3306/mydatabase" --output backup.json
```

### 4. Restore a PostgreSQL Database

```bash
python db_backup_restore.py restore --db-url "postgresql+psycopg2://user:pass@localhost:5432/mydatabase" --input backup.json
```

## Notes & Best Practices

- **Schema Consistency**: Ensure that the database schema remains unchanged between backup and restore operations. Altering tables or columns may lead to restore failures or data inconsistencies.
  
- **Data Types**: JSON supports standard data types. Complex types (e.g., binary data) may not serialize correctly. Modify the script if necessary to handle such cases.
  
- **Large Databases**: JSON files can become large and may not be efficient for very large databases. Consider using more optimized backup methods or splitting the backup into smaller chunks.
  
- **Security**: Store backup files securely, especially if they contain sensitive information. Unauthorized access to these files can lead to data breaches.
  
- **Testing**: Always test the restore process in a development or staging environment before applying it to production databases.
  
- **Automated Backups**: For regular backups, consider scheduling the script using cron jobs (Linux/macOS) or Task Scheduler (Windows).

## Troubleshooting

- **Connection Errors**: Ensure that your database URL is correct and that the database server is running and accessible.
  
- **Missing Tables**: If the restore process skips tables, verify that the tables exist in the target database and that the schema matches the backup.
  
- **Authentication Issues**: Double-check your database credentials and ensure that the user has the necessary permissions to read/write data.
  
- **JSON Decoding Errors**: Ensure that the backup JSON file is not corrupted and is properly formatted.

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contact

For any questions or issues, please open an issue in the repository or contact [christopheraliu07@gmail.com](mailto:christopheraliu07@gmail.com)

---
