library(threadr)

con <- db_connect("smonitor_example.db", config = FALSE)

sql <- readLines("create_table_statements.sql")
sql <- str_c(sql, collapse = "")
sql <- str_trim_many_spaces(sql)
sql <- str_split(sql, ";")
sql <- unlist(sql)
sql <- sql[!ifelse(sql == "", TRUE, FALSE)]

l_ply(sql, function(x) db_send(con, x))

db_list_tables(con)

db_disconnect(con)
