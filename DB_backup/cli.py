import logging
import argparse
from .main import backup_database, restore_database

# Configure logging for better error tracking and debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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



# if __name__ == '__main__':
#     main()
