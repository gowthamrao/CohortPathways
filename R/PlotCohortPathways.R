# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortPathways
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' Plot cohort pathway.
#'
#' @description
#' Uses the output of executeCohortPathways and using sunburstD3 produces sunburst plots
#'
#' @param resultsFolder               The folder where the output from executeCohortPathways is located. If provided, then
#'                                    the function will look for pathwayAnalysisCodes.csv and pathwayAnlaysisPaths.csv. Ignored
#'                                    if pathwayAnalysisCodes or pathwayAnalysisPaths are provided.
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param pathwayAnalysisGenerationId (Optional) Generation id of pathway. This uniquely identifies every generation execution.
#' @param targetCohortId              (Optional) The cohort id of the target cohort.
#' @param height                      (Optional) height and width of sunburst htmlwidget containing div specified in any valid CSS size unit.
#' @param width                       (Optional) height and width of sunburst htmlwidget containing div specified in any valid CSS size unit.
#'
#' @examples
#' \dontrun{
#'
#' executeCohortPathways(
#'   resultsFolder = "results",
#'   pathwayAnalysisGenerationId = 1234,
#'   targetCohortId = 1
#' )
#' }
#'
#' @export
plotCohortPathways <- function(resultsFolder,
                               exportFolder,
                               pathwayAnalysisGenerationId = NULL,
                               targetCohortId = NULL,
                               height = 400,
                               width = "100%") {
  start <- Sys.time()
  
  errorMessage <- checkmate::makeAssertCollection()
  resultsFolder <- normalizePath(resultsFolder, mustWork = FALSE)
  errorMessage <-
    createIfNotExist(type = "folder",
                     name = resultsFolder,
                     errorMessage = errorMessage)
  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  errorMessage <-
    createIfNotExist(type = "folder",
                     name = exportFolder,
                     errorMessage = errorMessage)
  checkmate::assertDouble(
    x = pathwayAnalysisGenerationId,
    lower = 0,
    len = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::assertDouble(
    x = targetCohortId,
    lower = 0,
    len = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  if (file.exists(file.path(exportFolder, "CohortPathways.rds"))) {
    warning("Replacing previous CohortPathways.rds file.")
  }
  
  if (file.exists(file.path(resultsFolder, "pathwaysAnalysisPaths.csv"))) {
    pathwaysAnalysisPaths <-
      readr::read_csv(
        file = file.path(resultsFolder, "pathwaysAnalysisPaths.csv"),
        col_types = readr::cols()
      )
    pathwaysAnalysisPaths <- pathwaysAnalysisPaths %>%
      SqlRender::snakeCaseToCamelCaseNames()
  }
  
  if (file.exists(file.path(resultsFolder, "pathwayAnalysisCodes.csv"))) {
    pathwayAnalysisCodes <-
      readr::read_csv(
        file = file.path(resultsFolder, "pathwayAnalysisCodes.csv"),
        col_types = readr::cols()
      )
    pathwayAnalysisCodes <- pathwayAnalysisCodes %>%
      SqlRender::snakeCaseToCamelCaseNames()
  }
  
  if (file.exists(file.path(resultsFolder, "pathwayAnalysisCodesLong.csv"))) {
    pathwayAnalysisCodesLong <-
      readr::read_csv(
        file = file.path(resultsFolder, "pathwayAnalysisCodesLong.csv"),
        col_types = readr::cols()
      )
    pathwayAnalysisCodesLong <- pathwayAnalysisCodesLong %>%
      SqlRender::snakeCaseToCamelCaseNames()
  }
  
  combis <- pathwaysAnalysisPaths %>%
    dplyr::select(targetCohortId,
                  pathwayAnalysisGenerationId) %>%
    dplyr::distinct()
  
  if (!is.null(targetCohortId)) {
    combis <- combis %>%
      dplyr::filter(targetCohortId %in% c(!!targetCohortId))
  }
  if (!is.null(pathwayAnalysisGenerationId)) {
    combis <- combis %>%
      dplyr::filter(pathwayAnalysisGenerationId %in% c(!!pathwayAnalysisGenerationId))
  }
  
  combis <- combis %>%
    dplyr::arrange(targetCohortId,
                   pathwayAnalysisGenerationId) %>%
    dplyr::distinct() %>%
    dplyr::mutate(key = dplyr::row_number())
  
  sunBurstPlots <- list()
  
  for (i in (1:nrow(combis))) {
    nestData <- pathwaysAnalysisPaths %>%
      dplyr::inner_join(combis[i, ],
                        by = c("targetCohortId",
                               "pathwayAnalysisGenerationId")) %>%
      dplyr::distinct() %>%
      dplyr::mutate(key = dplyr::row_number()) %>%
      dplyr::select("key", dplyr::starts_with("step"), "countValue") %>%
      tidyr::pivot_longer(
        cols = dplyr::starts_with("step"),
        names_to = "level",
        values_to = "code"
      ) %>%
      dplyr::inner_join(
        pathwayAnalysisCodes %>%
          dplyr::filter(
            pathwayAnalysisGenerationId %in% c(combis[i, ]$pathwayAnalysisGenerationId %>% unique())
          ) %>%
          dplyr::select(-pathwayAnalysisGenerationId) %>%
          dplyr::distinct(),
        by = c("code")
      ) %>%
      dplyr::mutate(level = stringr::str_replace(
        string = level,
        pattern = "step",
        replacement = "level"
      )) %>%
      dplyr::rename(size = countValue) %>%
      tidyr::pivot_wider(
        id_cols = c("key", "size"),
        names_from = "level",
        values_from = "name"
      ) %>%
      dplyr::arrange(key) %>%
      dplyr::select(-key)
    
    nest <- nestData %>%
      d3r::d3_nest(value_cols = "size")
    
    sbType1 <- sunburstR::sunburst(
      data = nest,
      legend = FALSE,
      width = width,
      height = height
    )
    
    sbType2 <- sunburstR::sund2b(data = nest, width = width)
    
    results <- list(
      key = combis[i, ]$key,
      plot1 = sbType1,
      plot2 = sbType2,
      plotData = nestData,
      treeData = nest,
      targetCohortId = combis[i, ]$targetCohortId,
      pathwayAnalysisGenerationId = combis[i, ]$pathwayAnalysisGenerationId
    )
    
    sunBurstPlots[[combis[i, ]$key]] <-
      results
  }
  
  result <- list(metadata = combis,
                 sunBurstPlots = sunBurstPlots)
  
  if (file.exists(file.path(exportFolder, "CohortPathways.rds"))) {
    unlink(file.path(exportFolder, "CohortPathways.rds"))
  }
  
  saveRDS(object = result,
          file = file.path(exportFolder, "CohortPathways.rds"))
  
  delta <- Sys.time() - start
  
  ParallelLogger::logInfo("Plotting took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
