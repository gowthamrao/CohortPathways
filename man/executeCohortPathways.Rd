% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ExecuteCohortPathways.R
\name{executeCohortPathways}
\alias{executeCohortPathways}
\title{Execute cohort pathway analysis.}
\usage{
executeCohortPathways(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableName = "cohort",
  targetDatabaseSChema = NULL,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  targetCohortIds,
  eventCohortIds,
  cohortDefinitionSet,
  exportFolder,
  minCellCount = 5,
  allowRepeats = FALSE,
  maxDepth = 5,
  collapseWindow = 30,
  overwrite = TRUE
)
}
\arguments{
\item{connectionDetails}{An object of type \code{connectionDetails} as created using the
\code{\link[DatabaseConnector]{createConnectionDetails}} function in the
DatabaseConnector package. Can be left NULL if \code{connection} is
provided.}

\item{connection}{An object of type \code{connection} as created using the
\code{\link[DatabaseConnector]{connect}} function in the
DatabaseConnector package. Can be left NULL if \code{connectionDetails}
is provided, in which case a new connection will be opened at the start
of the function, and closed when the function finishes.}

\item{cohortDatabaseSchema}{Schema name where your cohort tables reside. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{tempEmulationSchema}{Some database platforms like Oracle and Impala do not truly support
temp tables. To emulate temp tables, provide a schema with write
privileges where temp tables can be created.}

\item{targetCohortIds}{A vector of one or more Cohort Ids corresponding to target cohort (s).}

\item{eventCohortIds}{A vector of one or more Cohort Ids corresponding to event cohort (s).}

\item{cohortDefinitionSet}{A data frame object with minimum two columns, cohortId and cohortName. It should have all the cohortId's
in targetCohortId and eventCohortId. This is the source of cohort names.}

\item{exportFolder}{The folder where the output will be exported to. If this folder
does not exist it will be created.}

\item{minCellCount}{(Default = 5) The minimum cell count for fields contains person counts or fractions.}

\item{allowRepeats}{(Default = FALSE) Allow cohort events/combos to appear multiple times in the same pathway.}

\item{maxDepth}{(Default = 5) Maximum number of steps in a given pathway to be included in the sunburst plot.}

\item{collapseWindow}{(Default = 30) Any dates found within the specified collapse days will be reassigned the earliest date. Collapsing dates reduces pathway variation, leading to a reduction in 'noise' in the result.}

\item{overwrite}{(Default = TRUE) Do you want to overwrite results?}

\item{targetDatabaseSchema}{(Optional) Schema name where output pathway tables would reside. This is also known as
as resultsDatabaseSChema. If not specified, scratch schema will be used. The output may not
persist in the database after disconnection. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{cohortTable}{The name of the cohort table.}
}
\description{
Runs the cohort pathways on all instantiated combinations of target and event cohorts.
Assumes the cohorts have already been instantiated.
}
\examples{
\dontrun{
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "ohdsi.com",
  port = 5432,
  user = "me",
  password = "secure"
)

executeCohortPathways(
  connectionDetails = connectionDetails,
  cohorts = cohorts,
  exportFolder = "export",
  cohortDatabaseSchema = "results"
)
}

}
