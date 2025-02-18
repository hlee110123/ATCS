# Define a complete SQL Server compatible solution
build_atc_query_sqlserver <- function(schema, atc_code) {
  # Sanitize inputs to prevent SQL injection
  schema <- gsub("[^a-zA-Z0-9_]", "", schema)
  atc_code <- gsub("[^A-Z]", "", atc_code)

  # SQL Server compatible query
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
      ROUND((CASE WHEN total_prescriptions = 0 THEN 0
            ELSE (CAST(category_prescriptions AS FLOAT) / CAST(total_prescriptions AS FLOAT)) * 100 END), 2) as percentage_of_total
  FROM
      prescription_stats",
          schema, atc_code, schema, schema, schema, schema, schema)
}

# Define the ATC categories again (in case they're not in your environment)
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

# Define SQL Server compatible get_atc_stats
get_atc_stats_sqlserver <- function(conn, schema, atc_code, start_date = "2016-01-01", end_date = "2024-12-31") {
  if (!atc_code %in% names(ATC_CATEGORIES)) {
    stop(sprintf("Invalid ATC code. Must be one of: %s",
                 paste(names(ATC_CATEGORIES), collapse = ", ")))
  }

  # Build the query
  query <- build_atc_query_sqlserver(schema, atc_code)

  # Replace date parameters
  query <- gsub("'2016-01-01'", paste0("'", start_date, "'"), query)
  query <- gsub("'2024-12-31'", paste0("'", end_date, "'"), query)

  # Execute query
  tryCatch({
    result <- DatabaseConnector::querySql(conn, query)

    # Add category name to results
    result$atc_category <- ATC_CATEGORIES[[atc_code]]$name
    result$atc_code <- atc_code
    result$date_range <- paste(start_date, "to", end_date)

    return(result)
  }, error = function(e) {
    message(sprintf("Error executing query for ATC code %s: %s", atc_code, e$message))
    return(NULL)
  })
}

# Define SQL Server compatible get_all_atc_stats
get_all_atc_stats_sqlserver <- function(conn, schema, start_date = "2016-01-01", end_date = "2024-12-31") {
  results_list <- list()

  # Process each ATC category
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

cat("SQL Server compatible functions created successfully!\n")
cat("Use get_atc_stats_sqlserver() and get_all_atc_stats_sqlserver() functions\n")
