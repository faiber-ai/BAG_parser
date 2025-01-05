from database_sqlite import DatabaseSqlite

# Use context manager for database connection
with DatabaseSqlite() as db_sqlite:
    db_sqlite.test_bag_adressen()
