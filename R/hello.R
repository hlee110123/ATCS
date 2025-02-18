#' ATCTools: A package for analyzing ATC drug classifications in OMOP data
#'
#' @description
#' This package provides tools for analyzing drug usage by ATC classification
#' in OMOP CDM-compliant databases. It allows users to get prescription statistics
#' by ATC category and analyze medication utilization patterns.
#'
#' @docType package
#' @name ATCTools
"_PACKAGE"

#' ATC Categories
#'
#' A list containing all ATC top-level categories with their codes
#'
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

#' Get prescription statistics for an ATC category
#'
#' @param conn Database connection object
#' @param schema Schema name where OMOP tables are located
#' @param atc_code Single-letter ATC code (A-V)
#' @param start_date Optional start date filter (default: '2016-01-01')
#' @param end_date Optional end date filter (default: '2024-12-31')
#' @return A data frame with prescription statistics
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#' stats <- get_atc_stats(con, "omop.data", "J")
#' stats <- get_atc_stats(con, "omop.data", "J", "2018-01-01", "2023-12-31")
#' }
get_atc_stats <- function(conn, schema, atc_code, start_date = "2016-01-01", end_date = "2024-12-31") {
  if (!atc_code %in% names(ATC_CATEGORIES)) {
    stop(sprintf("Invalid ATC code. Must be one of: %s",
                 paste(names(ATC_CATEGORIES), collapse = ", ")))
  }

  # Validate dates
  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", start_date) || !grepl("^\\d{4}-\\d{2}-\\d{2}$", end_date)) {
    stop("Dates must be in YYYY-MM-DD format")
  }

  # Build the query with proper schema references
  query <- build_atc_query(schema, atc_code)

  # Replace date parameters
  query <- gsub("'2016-01-01'", paste0("'", start_date, "'"), query)
  query <- gsub("'2024-12-31'", paste0("'", end_date, "'"), query)

  # Execute query
  result <- DBI::dbGetQuery(conn, query)

  # Add category name to results
  result$atc_category <- ATC_CATEGORIES[[atc_code]]$name
  result$atc_code <- atc_code
  result$date_range <- paste(start_date, "to", end_date)

  return(result)
}

#' Build SQL query for ATC analysis
#'
#' @param schema Schema name
#' @param atc_code ATC code
#' @return SQL query string
#' @keywords internal
build_atc_query <- function(schema, atc_code) {
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
           AND drug_exposure_start_date >= '2016-01-01'
           AND (drug_exposure_end_date IS NULL OR drug_exposure_end_date <= '2024-12-31')
          ) as total_prescriptions
      FROM
          %s.drug_exposure de
          JOIN all_relevant_concepts arc
              ON de.drug_concept_id = arc.descendant_concept_id
      WHERE
          de.drug_exposure_start_date >= '2016-01-01'
          AND (de.drug_exposure_end_date IS NULL OR de.drug_exposure_end_date <= '2024-12-31')
  )
  SELECT
      category_prescriptions,
      total_prescriptions,
      ROUND(CAST(category_prescriptions AS FLOAT) / total_prescriptions * 100, 2) as percentage_of_total
  FROM
      prescription_stats",
          schema, atc_code, schema, schema, schema, schema, schema)
}

#' Get statistics for all ATC categories
#'
#' @param conn Database connection
#' @param schema Schema name where OMOP tables are located
#' @param start_date Optional start date filter (default: '2016-01-01')
#' @param end_date Optional end date filter (default: '2024-12-31')
#' @return A data frame with statistics for all ATC categories
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#' all_stats <- get_all_atc_stats(con, "omop.data")
#' all_stats <- get_all_atc_stats(con, "omop.data", "2018-01-01", "2023-12-31")
#' }
get_all_atc_stats <- function(conn, schema, start_date = "2016-01-01", end_date = "2024-12-31") {
  results <- lapply(names(ATC_CATEGORIES), function(code) {
    stats <- get_atc_stats(conn, schema, code, start_date, end_date)
    return(stats)
  })

  do.call(rbind, results)
}

#' Plot ATC prescription distribution
#'
#' @param stats Data frame from get_all_atc_stats
#' @param type Type of plot ("bar" or "pie")
#' @return A ggplot2 object
#' @export
#' @importFrom ggplot2 ggplot aes geom_bar geom_col coord_flip theme_minimal labs
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#' stats <- get_all_atc_stats(con, "omop.data")
#' plot_atc_distribution(stats)
#' }
plot_atc_distribution <- function(stats, type = "bar") {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("Package 'ggplot2' is required for plotting")
  }

  if (type == "bar") {
    p <- ggplot2::ggplot(stats, ggplot2::aes(x = reorder(atc_category, percentage_of_total),
                                             y = percentage_of_total,
                                             fill = atc_code)) +
      ggplot2::geom_col() +
      ggplot2::coord_flip() +
      ggplot2::theme_minimal() +
      ggplot2::labs(
        title = "Prescription Distribution by ATC Category",
        x = "ATC Category",
        y = "Percentage of Total Prescriptions",
        fill = "ATC Code"
      )
  } else if (type == "pie") {
    p <- ggplot2::ggplot(stats, ggplot2::aes(x = "", y = percentage_of_total, fill = atc_category)) +
      ggplot2::geom_bar(width = 1, stat = "identity") +
      ggplot2::coord_polar("y", start = 0) +
      ggplot2::theme_minimal() +
      ggplot2::labs(
        title = "Prescription Distribution by ATC Category",
        fill = "ATC Category"
      )
  } else {
    stop("Plot type must be 'bar' or 'pie'")
  }

  return(p)
}

#' Get detailed subcategory analysis for an ATC category
#'
#' @param conn Database connection
#' @param schema Schema name
#' @param atc_code Main ATC code (A-V)
#' @param start_date Optional start date filter (default: '2016-01-01')
#' @param end_date Optional end date filter (default: '2024-12-31')
#' @return Data frame with subcategory statistics
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#' j_detailed <- get_atc_subcategories(con, "omop.data", "J")
#' j_detailed <- get_atc_subcategories(con, "omop.data", "J", "2018-01-01", "2023-12-31")
#' }
get_atc_subcategories <- function(conn, schema, atc_code, start_date = "2016-01-01", end_date = "2024-12-31") {
  if (!atc_code %in% names(ATC_CATEGORIES)) {
    stop(sprintf("Invalid ATC code. Must be one of: %s",
                 paste(names(ATC_CATEGORIES), collapse = ", ")))
  }

  # Validate dates
  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", start_date) || !grepl("^\\d{4}-\\d{2}-\\d{2}$", end_date)) {
    stop("Dates must be in YYYY-MM-DD format")
  }

  query <- sprintf("
  WITH atc_concepts AS (
      SELECT
          concept_id,
          concept_name,
          concept_code,
          SUBSTRING(concept_code, 1, 3) as subcategory_code,
          concept_class_id
      FROM %s.concept
      WHERE vocabulary_id = 'ATC'
      AND concept_code LIKE '%s%%'
  ),
  subcategories AS (
      SELECT DISTINCT
          subcategory_code,
          MIN(concept_name) as subcategory_name
      FROM atc_concepts
      GROUP BY subcategory_code
  ),
  direct_mappings AS (
      SELECT DISTINCT
          c1.concept_id as atc_concept_id,
          c1.subcategory_code,
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
  all_concepts_by_subcategory AS (
      SELECT DISTINCT
          dm.subcategory_code,
          dm.rx_concept_id,
          ca.descendant_concept_id
      FROM direct_mappings dm
      JOIN %s.concept_ancestor ca
          ON dm.rx_concept_id = ca.ancestor_concept_id
  ),
  prescription_by_subcategory AS (
      SELECT
          ac.subcategory_code,
          COUNT(DISTINCT de.drug_exposure_id) as rx_count,
          COUNT(DISTINCT de.person_id) as patient_count
      FROM
          %s.drug_exposure de
          JOIN all_concepts_by_subcategory ac
              ON de.drug_concept_id = ac.descendant_concept_id
      WHERE
          de.drug_exposure_start_date >= '2016-01-01'
          AND (de.drug_exposure_end_date IS NULL OR de.drug_exposure_end_date <= '2024-12-31')
      GROUP BY ac.subcategory_code
  ),
  total_counts AS (
      SELECT
          COUNT(DISTINCT drug_exposure_id) as total_rx,
          COUNT(DISTINCT person_id) as total_patients
      FROM %s.drug_exposure
      WHERE drug_concept_id IS NOT NULL
      AND drug_exposure_start_date >= '2016-01-01'
      AND (drug_exposure_end_date IS NULL OR drug_exposure_end_date <= '2024-12-31')
  )
  SELECT
      s.subcategory_code,
      s.subcategory_name,
      p.rx_count,
      p.patient_count,
      t.total_rx,
      t.total_patients,
      ROUND(CAST(p.rx_count AS FLOAT) / t.total_rx * 100, 2) as rx_percentage,
      ROUND(CAST(p.patient_count AS FLOAT) / t.total_patients * 100, 2) as patient_percentage
  FROM
      subcategories s
      JOIN prescription_by_subcategory p ON s.subcategory_code = p.subcategory_code
      CROSS JOIN total_counts t
  ORDER BY rx_count DESC",
                   schema, atc_code, schema, schema, schema, schema, schema)

  # Replace date parameters
  query <- gsub("'2016-01-01'", paste0("'", start_date, "'"), query)
  query <- gsub("'2024-12-31'", paste0("'", end_date, "'"), query)

  result <- DBI::dbGetQuery(conn, query)
  result$date_range <- paste(start_date, "to", end_date)
  return(result)
}
