# tests/test_main.py

import pytest
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Date, Text,
    Numeric, LargeBinary, Enum, ARRAY, ForeignKey, MetaData, Table, create_engine
)
from sqlalchemy.orm import declarative_base, sessionmaker
import json
import os
from DB_backup.main import (
    serialize_column,
    deserialize_column,
    backup_database,
    restore_database
)
import tempfile
import shutil
from datetime import datetime

Base = declarative_base()

# Define sample tables for testing
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    age = Column(Integer, nullable=True)
    balance = Column(Numeric(10, 2), default=0.0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    data = Column(Text)
    profile_pic = Column(LargeBinary)
    status = Column(Enum('active', 'inactive', name='status_enum'))
    tags = Column(ARRAY(String))

class Post(Base):
    __tablename__ = 'posts'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    title = Column(String(100))
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

# Fixture for setting up a temporary SQLite database file
@pytest.fixture(scope='module')
def sqlite_db_path():
    fd, path = tempfile.mkstemp(suffix='.sqlite')
    os.close(fd)
    yield path
    os.remove(path)

# Fixture for creating and populating the SQLite database
@pytest.fixture(scope='module')
def populated_engine(sqlite_db_path):
    engine = create_engine(f'sqlite:///{sqlite_db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Add users
    user1 = User(name='Alice', age=30, balance=1000.50, is_active=True, data='Sample data 1', status='active', tags=['tag1', 'tag2'])
    user2 = User(name='Bob', age=25, balance=200.75, is_active=False, data='Sample data 2', status='inactive', tags=['tag3'])
    session.add_all([user1, user2])
    session.commit()

    # Add posts
    post1 = Post(user_id=user1.id, title='First Post', content='Content of first post')
    post2 = Post(user_id=user2.id, title='Second Post', content='Content of second post')
    session.add_all([post1, post2])
    session.commit()

    yield engine
    engine.dispose()

# Fixture for setting up a temporary directory for backup files
@pytest.fixture
def temp_backup_dir():
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath)

def test_serialize_column(populated_engine):
    # Reflect the tables
    metadata = MetaData()
    metadata.reflect(bind=populated_engine)
    users_table = metadata.tables['users']

    # Test serialize each column
    for column in users_table.columns:
        serialized = serialize_column(column)
        assert 'name' in serialized
        assert 'type' in serialized
        assert 'nullable' in serialized
        assert 'primary_key' in serialized
        assert 'unique' in serialized
        # Additional checks based on column
        if column.type.__class__.__name__ == 'String':
            assert 'length' in serialized['type']
            assert serialized['type']['length'] == column.type.length
        if isinstance(column.type, Enum):
            assert 'enum_values' in serialized['type']
            assert serialized['type']['enum_values'] == list(column.type.enums)
        if isinstance(column.type, ARRAY):
            assert 'item_type' in serialized['type']
            assert serialized['type']['item_type'] == 'String'

def test_deserialize_column(populated_engine):
    # Reflect the tables
    metadata = MetaData()
    metadata.reflect(bind=populated_engine)
    users_table = metadata.tables['users']

    # Serialize and then deserialize each column
    for column in users_table.columns:
        serialized = serialize_column(column)
        deserialized_column = deserialize_column(serialized, metadata)
        assert deserialized_column.name == column.name
        # Compare types
        original_type = type(column.type)
        deserialized_type = type(deserialized_column.type)
        assert deserialized_type == original_type or (
            deserialized_type.__name__ == column.type.__class__.__name__
        )
        # Compare attributes
        assert deserialized_column.nullable == column.nullable
        assert deserialized_column.primary_key == column.primary_key
        assert deserialized_column.unique == column.unique
        if column.default:
            assert str(deserialized_column.default.arg) == str(column.default.arg)

def test_backup_database(populated_engine, temp_backup_dir):
    # Path for backup file
    backup_file = os.path.join(temp_backup_dir, 'backup.json')

    # Perform backup
    backup_database(
        db_url=str(populated_engine.url),
        output_file=backup_file,
        include_relationships=True,
        version="1.0"
    )

    # Check if backup file exists
    assert os.path.exists(backup_file)

    # Read and verify backup data
    with open(backup_file, 'r') as f:
        backup_data = json.load(f)

    assert 'version' in backup_data
    assert backup_data['version'] == '1.0'
    assert 'tables' in backup_data
    assert 'users' in backup_data['tables']
    assert 'posts' in backup_data['tables']
    assert 'data' in backup_data['tables']['users']
    assert len(backup_data['tables']['users']['data']) == 2
    assert 'relationships' in backup_data

def test_restore_database(populated_engine, temp_backup_dir):
    # Path for backup file
    backup_file = os.path.join(temp_backup_dir, 'backup.json')

    # Perform backup first
    backup_database(
        db_url=str(populated_engine.url),
        output_file=backup_file,
        include_relationships=True,
        version="1.0"
    )

    # Now, perform restore to a new temporary database
    restore_db_fd, restore_db_path = tempfile.mkstemp(suffix='.sqlite')
    os.close(restore_db_fd)

    try:
        restore_database(
            db_url=f'sqlite:///{restore_db_path}',
            input_file=backup_file,
            max_retries=3,
            retry_delay=1
        )

        # Connect to the restored database and verify data
        restored_engine = create_engine(f'sqlite:///{restore_db_path}')
        restored_metadata = MetaData()
        restored_metadata.reflect(bind=restored_engine)

        # Check tables
        assert 'users' in restored_metadata.tables
        assert 'posts' in restored_metadata.tables

        # Check data
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

    finally:
        os.remove(restore_db_path)

def test_serialize_deserialize_roundtrip():
    # Create a sample column
    sample_enum = Enum('active', 'inactive', name='status_enum')
    sample_column = Column(
        'status',
        sample_enum,
        nullable=False,
        default='active',
        unique=True
    )

    metadata = MetaData()

    # Serialize
    serialized = serialize_column(sample_column)
    assert serialized['name'] == 'status'
    assert serialized['type']['name'] == 'Enum'
    assert serialized['type']['enum_values'] == ['active', 'inactive']
    assert serialized['nullable'] == False
    assert serialized['unique'] == True
    assert serialized['default'] == 'active'
    assert len(serialized['foreign_keys']) == 0

    # Deserialize
    deserialized_column = deserialize_column(serialized, metadata)
    assert deserialized_column.name == 'status'
    assert isinstance(deserialized_column.type, Enum)
    assert deserialized_column.type.enums == ('active', 'inactive')
    assert deserialized_column.nullable == False
    assert deserialized_column.unique == True
    assert deserialized_column.default.arg == 'active'
