library(threadr)

# Create SQLite database
con <- db_connect("smonitor_example.db", config = FALSE)

# Use SQL script to create an smonitor database
db_use_sql(con, "create_table_statements_core.sql")

# Check
db_list_tables(con)

# Close
db_disconnect(con)
