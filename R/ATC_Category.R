# SQLServerATCStats R Package

#' @title ATC Categories Definition
#' @description A list of Anatomical Therapeutic Chemical (ATC) categories with codes and names
#' @export
ATC_CATEGORIES <- list(
  A = list(name = "Alimentary tract and metabolism", code = "A"),
  B = list(name = "Blood and blood forming organs", code = "B"),
  C = list(name = "Cardiovascular system", code = "C"),
  D = list(name = "Dermatologicals", code = "D"),
  G = list(name = "Genito-urinary system and sex hormones", code = "G"),
  H = list(name = "Systemic hormonal preparations", code = "H"),
  J = list(name = "Antiinfectives for systemic use", code = "J"),
  L = list(name = "Antineoplastic and immunomodulating agents", code = "L"),
  M = list(name = "Musculoskeletal system", code = "M"),
  N = list(name = "Nervous system", code = "N"),
  P = list(name = "Antiparasitic products", code = "P"),
  R = list(name = "Respiratory system", code = "R"),
  S = list(name = "Sensory organs", code = "S"),
  V = list(name = "Various", code = "V")
)

#' Build ATC Query for SQL Server
#'
#' Builds a SQL Server compatible query to retrieve prescription statistics for a given ATC category
#'
#' @param schema The database schema name
#' @param atc_code The ATC category code (A-V)
#' @param start_date Start date for data analysis in YYYY-MM-DD format
#' @param end_date End date for data analysis in YYYY-MM-DD format
#'
#' @return A SQL Server compatible query string
#' @keywords internal
build_atc_query_sqlserver <- function(schema, atc_code, start_date, end_date) {
  # Sanitize inputs to prevent SQL injection
  schema <- gsub("[^a-zA-Z0-9_]", "", schema)
  atc_code <- gsub("[^A-Z]", "", atc_code)

  # Format dates for SQL Server
  start_date_str <- format(as.Date(start_date), "%Y-%m-%d")
  end_date_str <- format(as.Date(end_date), "%Y-%m-%d")

  # Build SQL Server compatible query
  sprintf("
  WITH atc_concepts AS (
      SELECT
          concept_id,
          concept_name,
          concept_code,
          concept_class_id
      FROM %s.concept
      WHERE vocabulary_id = 'ATC'
      AND concept_code LIKE '%s%%'
  ),
  direct_mappings AS (
      SELECT DISTINCT
          c1.concept_id as atc_concept_id,
          c2.concept_id as rx_concept_id
      FROM atc_concepts c1
      JOIN %s.concept_relationship cr
          ON c1.concept_id = cr.concept_id_1
      JOIN %s.concept c2
          ON cr.concept_id_2 = c2.concept_id
      WHERE c2.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
      AND cr.relationship_id = 'Maps to'
      AND c2.invalid_reason IS NULL
      AND cr.invalid_reason IS NULL
  ),
  all_relevant_concepts AS (
      SELECT DISTINCT
          dm.rx_concept_id,
          ca.descendant_concept_id
      FROM direct_mappings dm
      JOIN %s.concept_ancestor ca
          ON dm.rx_concept_id = ca.ancestor_concept_id
  ),
  prescription_stats AS (
      SELECT
          COUNT(*) as category_prescriptions,
          (SELECT COUNT(*)
           FROM %s.drug_exposure
           WHERE drug_concept_id IS NOT NULL
           AND drug_exposure_start_date >= '%s'
           AND (drug_exposure_end_date IS NULL OR drug_exposure_end_date <= '%s')
          ) as total_prescriptions
      FROM
          %s.drug_exposure de
          JOIN all_relevant_concepts arc
              ON de.drug_concept_id = arc.descendant_concept_id
      WHERE
          de.drug_exposure_start_date >= '%s'
          AND (de.drug_exposure_end_date IS NULL OR de.drug_exposure_end_date <= '%s')
  )
  SELECT
      category_prescriptions,
      total_prescriptions,
      ROUND(CAST((CASE WHEN total_prescriptions = 0 THEN 0
            ELSE (CAST(category_prescriptions AS FLOAT) / total_prescriptions) * 100 END) AS DECIMAL(10,2)), 2) as percentage_of_total
  FROM
      prescription_stats",
          schema, atc_code, schema, schema, schema, schema, start_date_str, end_date_str, schema, start_date_str, end_date_str)
}

#' Get ATC Statistics from SQL Server
#'
#' Retrieves prescription statistics for a specific ATC category from SQL Server
#'
#' @param conn A DBI or ODBC connection to SQL Server
#' @param schema The database schema name containing OMOP CDM tables
#' @param atc_code The ATC category code (single character A-V)
#' @param start_date Start date for data analysis (Default: "2016-01-01")
#' @param end_date End date for data analysis (Default: "2024-12-31")
#'
#' @return A data.frame with prescription statistics for the specified ATC category
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to SQL Server using odbc
#' conn <- DBI::dbConnect(odbc::odbc(),
#'                        Driver = "SQL Server",
#'                        Server = "SERVERNAME",
#'                        Database = "DBNAME",
#'                        Trusted_Connection = "Yes")
#'
#' # Get statistics for cardiovascular drugs
#' cardio_stats <- get_atc_stats_sqlserver(conn, "cdm_schema", "C")
#' }
get_atc_stats_sqlserver <- function(conn, schema, atc_code, start_date = "2016-01-01", end_date = "2024-12-31") {
  # Validate inputs
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required. Please install it with install.packages('DBI')")
  }

  if (is.null(conn)) {
    stop("Invalid connection object. Please provide a valid DBI connection to SQL Server")
  }

  if (is.null(schema) || schema == "") {
    stop("Schema name cannot be empty")
  }

  if (!atc_code %in% names(ATC_CATEGORIES)) {
    stop(sprintf("Invalid ATC code. Must be one of: %s",
                 paste(names(ATC_CATEGORIES), collapse = ", ")))
  }

  # Validate dates
  tryCatch({
    start_date <- as.Date(start_date)
    end_date <- as.Date(end_date)
  }, error = function(e) {
    stop("Invalid date format. Please use YYYY-MM-DD format")
  })

  if (start_date >= end_date) {
    stop("Start date must be before end date")
  }

  # Build SQL Server compatible query
  query <- build_atc_query_sqlserver(schema, atc_code, start_date, end_date)

  # Execute query with error handling
  tryCatch({
    # Execute query using DBI
    result <- DBI::dbGetQuery(conn, query)

    # Handle empty results
    if (nrow(result) == 0 || is.null(result)) {
      message(sprintf("No results found for ATC category %s", atc_code))
      # Create empty result with correct structure
      result <- data.frame(
        category_prescriptions = 0,
        total_prescriptions = 0,
        percentage_of_total = 0
      )
    }

    # Add category name to results
    result$atc_category <- ATC_CATEGORIES[[atc_code]]$name
    result$atc_code <- atc_code
    result$date_range <- paste(format(start_date, "%Y-%m-%d"), "to", format(end_date, "%Y-%m-%d"))

    return(result)

  }, error = function(e) {
    message(sprintf("Error executing query for ATC code %s: %s", atc_code, e$message))
    return(NULL)
  })
}

#' Get All ATC Statistics from SQL Server
#'
#' Retrieves prescription statistics for all ATC categories from SQL Server
#'
#' @param conn A DBI or ODBC connection to SQL Server
#' @param schema The database schema name containing OMOP CDM tables
#' @param start_date Start date for data analysis (Default: "2016-01-01")
#' @param end_date End date for data analysis (Default: "2024-12-31")
#'
#' @return A data.frame with prescription statistics for all ATC categories
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to SQL Server using odbc
#' conn <- DBI::dbConnect(odbc::odbc(),
#'                        Driver = "SQL Server",
#'                        Server = "SERVERNAME",
#'                        Database = "DBNAME",
#'                        Trusted_Connection = "Yes")
#'
#' # Get statistics for all drug categories
#' all_drug_stats <- get_all_atc_stats_sqlserver(conn, "cdm_schema")
#' }
get_all_atc_stats_sqlserver <- function(conn, schema, start_date = "2016-01-01", end_date = "2024-12-31") {
  if (is.null(conn)) {
    stop("Invalid connection object. Please provide a valid DBI connection to SQL Server")
  }

  if (is.null(schema) || schema == "") {
    stop("Schema name cannot be empty")
  }

  # Validate dates
  tryCatch({
    start_date <- as.Date(start_date)
    end_date <- as.Date(end_date)
  }, error = function(e) {
    stop("Invalid date format. Please use YYYY-MM-DD format")
  })

  if (start_date >= end_date) {
    stop("Start date must be before end date")
  }

  results_list <- list()

  # Process each ATC category with error handling
  for (code in names(ATC_CATEGORIES)) {
    tryCatch({
      stats <- get_atc_stats_sqlserver(conn, schema, code, start_date, end_date)
      if (!is.null(stats)) {
        results_list[[code]] <- stats
      }
    }, error = function(e) {
      message(sprintf("Error processing ATC code %s: %s", code, e$message))
    })
  }

  # Check if any results were obtained
  if (length(results_list) == 0) {
    stop("Failed to retrieve statistics for any ATC category")
  }

  # Combine results
  result_df <- do.call(rbind, results_list)
  rownames(result_df) <- NULL
  return(result_df)
}
