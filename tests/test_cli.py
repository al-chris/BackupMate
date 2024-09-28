# tests/test_cli.py

import pytest
import os
import tempfile
from unittest import mock
from DB_backup.cli import main as cli_main
from DB_backup.main import Base, User, Post
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Fixture for setting up a temporary SQLite database file
@pytest.fixture
def sqlite_db_path():
    fd, path = tempfile.mkstemp(suffix='.sqlite')
    os.close(fd)
    yield path
    os.remove(path)

# Fixture for creating and populating the SQLite database
@pytest.fixture
def populated_db(sqlite_db_path):
    engine = create_engine(f'sqlite:///{sqlite_db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Add sample data
    user1 = User(name='Alice', age=30, balance=1000.50, is_active=True, data='Sample data 1', status='active', tags=['tag1', 'tag2'])
    user2 = User(name='Bob', age=25, balance=200.75, is_active=False, data='Sample data 2', status='inactive', tags=['tag3'])
    session.add_all([user1, user2])
    session.commit()

    post1 = Post(user_id=user1.id, title='First Post', content='Content of first post')
    post2 = Post(user_id=user2.id, title='Second Post', content='Content of second post')
    session.add_all([post1, post2])
    session.commit()

    session.close()
    engine.dispose()

def test_cli_backup_restore(populated_db, sqlite_db_path):
    # Create a temporary directory for backup
    with tempfile.TemporaryDirectory() as backup_dir:
        backup_file = os.path.join(backup_dir, 'backup.json')

        # Mock sys.argv for backup
        test_args = [
            'cli.py',
            'backup',
            '--db-url', f'sqlite:///{sqlite_db_path}',
            '--output', backup_file,
            '--include-relationships',
            '--version', '1.0'
        ]

        with mock.patch('sys.argv', test_args):
            cli_main()

        # Check if backup file was created
        assert os.path.exists(backup_file)

        # Now, create a new temporary SQLite database for restore
        with tempfile.NamedTemporaryFile(suffix='.sqlite') as restore_db:
            restore_db_path = restore_db.name

            # Mock sys.argv for restore
            restore_args = [
                'cli.py',
                'restore',
                '--db-url', f'sqlite:///{restore_db_path}',
                '--input', backup_file,
                '--max-retries', '3',
                '--retry-delay', '1'
            ]

            with mock.patch('sys.argv', restore_args):
                cli_main()

            # Connect to restored database and verify data
            restored_engine = create_engine(f'sqlite:///{restore_db_path}')
            Base.metadata.reflect(bind=restored_engine)
            Session = sessionmaker(bind=restored_engine)
            session = Session()

            users = session.query(User).all()
            assert len(users) == 2
            assert users[0].name == 'Alice'
            assert users[1].name == 'Bob'

            posts = session.query(Post).all()
            assert len(posts) == 2
            assert posts[0].title == 'First Post'
            assert posts[1].title == 'Second Post'

            session.close()
            restored_engine.dispose()
