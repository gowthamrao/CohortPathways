testthat::test_that("Execute Cohort Pathways", {
  # generate unique name for a cohort table
  sysTime <- as.numeric(Sys.time()) * 100000
  tableName <- paste0("cr", sysTime)
  tempTableName <- paste0("#", tableName, "_1")

  cohortDefinitionSet <- dplyr::tibble(
    cohortId = c(1, 2, 10, 20),
    cohortName = c("A", "B", "C", "D")
  )

  # make up date for a cohort table
  targetCohort <- dplyr::tibble(
    cohortDefinitionId = c(1, 1, 2),
    subjectId = c(1, 1, 1),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("1999-02-20"),
      as.Date("1999-03-01")
    ),
    cohortEndDate = c(
      as.Date("1999-01-31"),
      as.Date("1999-02-28"),
      as.Date("1999-03-31")
    )
  )

  eventCohort <- dplyr::tibble(
    cohortDefinitionId = c(10, 10, 20),
    subjectId = c(1, 1, 1),
    cohortStartDate = c(
      as.Date("1999-01-01"),
      as.Date("1999-01-20"),
      as.Date("1999-04-10")
    ),
    cohortEndDate = c(
      as.Date("1999-01-10"),
      as.Date("1999-02-20"),
      as.Date("1999-04-20")
    )
  )

  # upload table
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = cohortDatabaseSchema,
    tableName = tableName,
    data = dplyr::bind_rows(targetCohort, eventCohort) %>% dplyr::distinct(),
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = FALSE,
    camelCaseToSnakeCase = TRUE,
    progressBar = FALSE
  )

  dataInserted <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = paste0(
        "SELECT * FROM @cohort_database_schema.@table_name
        order by cohort_definition_id, subject_id, cohort_start_date;"
      ),
      cohort_database_schema = cohortDatabaseSchema,
      table_name = tableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()

  testthat::expect_equal(
    object = dataInserted %>%
      nrow(),
    expected = nrow(dplyr::bind_rows(targetCohort, eventCohort) %>% dplyr::distinct())
  )

  exportFolder <- tempfile()
  dir.create(path = exportFolder, showWarnings = FALSE, recursive = TRUE)

  CohortPathways::executeCohortPathways(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableName = tableName,
    targetCohortIds = c(1, 2),
    eventCohortIds = c(10, 20),
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = exportFolder,
    overwrite = TRUE
  )

  testthat::expect_true(object = file.exists(
    file.path(exportFolder, "pathwayAnalysisCodes.csv")
  ))
  testthat::expect_true(object = file.exists(
    file.path(exportFolder, "pathwayAnalysisCodesLong.csv")
  ))
  testthat::expect_true(object = file.exists(
    file.path(exportFolder, "pathwayAnalysisStatsData.csv")
  ))
  testthat::expect_true(object = file.exists(
    file.path(exportFolder, "pathwaysAnalysisPathsData.csv")
  ))

  pathwayAnalysisCodes <-
    readr::read_csv(
      file = file.path(exportFolder, "pathwayAnalysisCodes.csv"),
      col_types = readr::cols()
    ) %>% SqlRender::snakeCaseToCamelCaseNames()
  testthat::expect_true(object = nrow(pathwayAnalysisCodes) > 0)

  pathwayAnalysisCodesLong <-
    readr::read_csv(
      file = file.path(exportFolder, "pathwayAnalysisCodesLong.csv"),
      col_types = readr::cols()
    ) %>% SqlRender::snakeCaseToCamelCaseNames()
  testthat::expect_true(object = nrow(pathwayAnalysisCodesLong) > 0)

  pathwayAnalysisStatsData <-
    readr::read_csv(
      file = file.path(exportFolder, "pathwayAnalysisStatsData.csv"),
      col_types = readr::cols()
    ) %>% SqlRender::snakeCaseToCamelCaseNames()
  testthat::expect_true(object = nrow(pathwayAnalysisStatsData) > 0)

  pathwaysAnalysisPathsData <-
    readr::read_csv(
      file = file.path(exportFolder, "pathwaysAnalysisPathsData.csv"),
      col_types = readr::cols()
    ) %>% SqlRender::snakeCaseToCamelCaseNames()
  testthat::expect_true(object = nrow(pathwayAnalysisStatsData) > 0)
})
