from database_sqlite import DatabaseSqlite

import utils

# Use context manager for database connection
with DatabaseSqlite() as db_sqlite:
    utils.print_log("Delete full BAG tables")
    db_sqlite.delete_no_longer_needed_bag_tables()

    utils.print_log("cleaning up: vacuum")
    db_sqlite.commit()
    db_sqlite.vacuum()

    utils.print_log("ready")
