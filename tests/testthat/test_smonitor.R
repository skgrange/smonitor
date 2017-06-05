library(testthat)
library(smonitor)

# Connect to database
con <- databaser::db_connect("~/Dropbox/R/databases/smonitor_new_zealand.db")

context("Importing functions")

test_that("Low-level importer", {
  
  df_01 <- import_by_process(
    con,
    process = 1
  )
  
  df_02 <- import_by_process(
    con,
    process = 1, 
    summary = 1
  )
  
  df_03 <- import_by_process(
    con,
    process = c(1, 2), 
    summary = 1
  )
  
  df_04 <- import_by_process(
    con,
    process = c(1, 2), 
    summary = c(1, 20)
  )
  
  df_05 <- import_by_process(
    con,
    process = 1, 
    summary = 1,
    start = 2009,
    end = 2009,
    tz = tz_nz(),
    valid_only = TRUE
  )
  
  df_05 <- import_by_process(
    con,
    process = 1, 
    summary = 1,
    start = 2010,
    end = 2010,
    tz = tz_nz(),
    valid_only = FALSE
  )
  
  df_06 <- import_by_process(
    con,
    process = 1, 
    summary = 1,
    start = 2010,
    end = 2010,
    tz = tz_nz(),
    valid_only = TRUE, 
    print_query = TRUE
  )
  
  df_07 <- import_by_process(
    con,
    process = 1, 
    summary = 1,
    start = 2010,
    end = 2010,
    tz = tz_nz(),
    valid_only = TRUE,
    date_insert = TRUE
  )
  
  df_08 <- import_by_process(
    con,
    process = 1, 
    summary = 1,
    start = 2010,
    end = 2010,
    tz = tz_nz(),
    valid_only = TRUE,
    date_insert = FALSE,
    date_end = FALSE, 
    site_name = FALSE
  )
  
  
  # Checks
  expect_equal(class(df_01), "data.frame")
  
  expect_equal(
    names(df_01), 
    c("date", "date_end", "process", "summary", "validity", "value", "site", 
      "variable", "site_name")
  )
  
  expect_equal(unique(df_01$process), 1)
  expect_equal(length(unique(df_01$summary)), 10)
  
  expect_equal(class(df_02), "data.frame")
  expect_equal(unique(df_02$process), 1)
  expect_equal(length(unique(df_02$summary)), 1)
  
  expect_equal(class(df_03), "data.frame")
  expect_equal(unique(df_03$process), c(1, 2))
  expect_equal(length(unique(df_03$summary)), 1)
  
  expect_equal(class(df_04), "data.frame")
  expect_equal(unique(df_04$process), c(1, 2))
  expect_equal(length(unique(df_04$summary)), 2)
  
  expect_equal(class(df_05), "data.frame")
  # Hours in a year
  expect_equal(nrow(df_05), 8760)
  
  expect_equal(class(df_06), "data.frame")
  
  expect_equal(length(names(df_07)), 10)
  
  expect_equal(length(names(df_08)), 7)
  
  # Errors
  expect_error(import_by_process(con))
  
  # No data, warning but not an error
  expect_warning(import_by_process(con, process = 1000000))
  
})


test_that("Site-importer", {
  
  # 
  expect_equal(
    class(
      import_by_site(
        con, 
        site = "ns",
        start = "2014-01-01",
        end = "2014-01-02"
      )
    ), 
    "data.frame"
  )
  
  expect_equal(
    class(
      import_by_site(
        con, 
        site = "ns",
        start = "2014-01-01",
        end = "2014-01-02",
        period = "day"
      )
      
    ), 
    "data.frame"
  )
  
  expect_equal(
    class(
      import_by_site(
        con, 
        site = "ns",
        variable = "ws",
        start = "2014-01-01",
        end = "2014-01-02",
        period = "hour"
      )
    ), 
    "data.frame"
  )
  
  expect_equal(
    class(
      import_by_site(
        con, 
        site = "ns",
        variable = "ws",
        start = "2014-01-01",
        end = "2014-01-02",
        period = "hour",
        valid_only = FALSE,
        pad = FALSE,
        tz = tz_nz()
      )
    ), 
    "data.frame"
  )
  
  expect_warning(
    import_by_site(
      con, 
      site = "ns",
      start = "2014-01-01",
      end = "2014-01-02",
      period = "day",
      spread = TRUE
    )
  )
  
})


test_that("Site- importer", {
  
  expect_equal(
    class(
      import_summaries(con)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_summaries(con, extra = FALSE)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_aggregations(con)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_aggregations(con, extra = FALSE)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_invalidations(con)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_processes(con)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_processes(con, type = "minimal")
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_sites(con)
    ),
    "data.frame"
  )
  
  expect_equal(
    class(
      import_sites(con, extra = FALSE)
    ),
    "data.frame"
  )
  
})


