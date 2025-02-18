# Define disease categories
DISEASE_CATEGORIES <- list(
  infectious = list(name = "Certain infectious and parasitic disease", code_range = c("A00", "B99")),
  neoplasms = list(name = "Neoplasm", code_range = c("C00", "D49")),
  blood_immune = list(name = "Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism", code_range = c("D50", "D89")),
  endocrine = list(name = "Endocrine, nutritional and metabolic diseases", code_range = c("E00", "E89")),
  mental = list(name = "Mental, Behavioral and Neurodevelopmental disorders", code_range = c("F01", "F99")),
  nervous = list(name = "Diseases of the nervous system", code_range = c("G00", "G99")),
  eye = list(name = "Disease of the eye and adnexa", code_range = c("H00", "H59")),
  ear = list(name = "Diseases of the ear and mastoid process", code_range = c("H60", "H95")),
  circulatory = list(name = "Diseases of the circulatory system", code_range = c("I00", "I99")),
  respiratory = list(name = "Diseases of the respiratory system", code_range = c("J00", "J99")),
  digestive = list(name = "Diseases of the digestive system", code_range = c("K00", "K95")),
  skin = list(name = "Diseases of the skin and subcutaneous tissue", code_range = c("L00", "L99")),
  musculoskeletal = list(name = "Diseases of the musculoskeletal system and connective tissue", code_range = c("M00", "M99")),
  genitourinary = list(name = "Diseases of the genitourinary system", code_range = c("N00", "N99")),
  pregnancy = list(name = "Pregnancy, childbirth and the puerperium", code_range = c("O00", "O9A")),
  perinatal = list(name = "Certain conditions originating in the perinatal period", code_range = c("P00", "P96")),
  congenital = list(name = "Congenital malformations, deformations and chromosomal abnormalities", code_range = c("Q00", "Q99")),
  symptoms = list(name = "Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified", code_range = c("R00", "R99")),
  injury = list(name = "Injury, poisoning and certain other consequences of external causes", code_range = c("S00", "T88")),
  external_causes = list(name = "External causes of morbidity and mortality", code_range = c("V00", "Y99")),
  health_status = list(name = "Factors influencing health status and contact with health services", code_range = c("Z00", "Z99")),
  special = list(name = "Codes for special purposes", code_range = c("U00", "U85"))
)

#' Get total number of patients in database
#'
#' @param conn Database connection
#' @param cdm_schema CDM schema name
#' @return Total number of patients
get_total_patients <- function(conn, cdm_schema) {
  query <- sprintf("
    SELECT COUNT(DISTINCT person_id) as total_patients
    FROM %s.person", cdm_schema)

  result <- DBI::dbGetQuery(conn, query)
  return(result$total_patients[1])
}

#' Get patient counts for a disease category
#'
#' @param conn Database connection
#' @param cdm_schema CDM schema name
#' @param category Disease category name
#' @return Number of unique patients
get_category_count <- function(conn, cdm_schema, category) {
  if (!category %in% names(DISEASE_CATEGORIES)) {
    stop(sprintf("Invalid category. Must be one of: %s",
                 paste(names(DISEASE_CATEGORIES), collapse = ", ")))
  }

  codes <- DISEASE_CATEGORIES[[category]]$code_range

  query <- sprintf("
    WITH standard_concepts AS (
      SELECT DISTINCT
        cr.concept_id_2 as standard_concept_id
      FROM
        %s.concept c
        INNER JOIN %s.concept_relationship cr
          ON c.concept_id = cr.concept_id_1
          AND cr.relationship_id = 'Maps to'
          AND cr.invalid_reason IS NULL
      WHERE
        c.vocabulary_id = 'ICD10CM'
        AND c.concept_code >= '%s'
        AND c.concept_code <= '%s'
        AND c.invalid_reason IS NULL
    )
    SELECT
      COUNT(DISTINCT co.person_id) as total_unique_patients
    FROM
      standard_concepts sc
      INNER JOIN %s.condition_occurrence co
        ON sc.standard_concept_id = co.condition_concept_id
    WHERE
      co.condition_start_date >= '2016-01-01'
      AND (co.condition_end_date IS NULL OR co.condition_end_date <= '2024-12-31')",
                   cdm_schema, cdm_schema, codes[1], codes[2], cdm_schema)

  result <- DBI::dbGetQuery(conn, query)
  return(result$total_unique_patients[1])
}

#' Get prevalence rates for all disease categories
#'
#' @param conn Database connection
#' @param cdm_schema CDM schema name
#' @return Data frame with counts and prevalence rates
get_prevalence_rates <- function(conn, cdm_schema) {
  total_patients <- get_total_patients(conn, cdm_schema)

  results <- lapply(names(DISEASE_CATEGORIES), function(category) {
    count <- get_category_count(conn, cdm_schema, category)
    prevalence <- (count / total_patients) * 100

    data.frame(
      category = category,
      category_name = DISEASE_CATEGORIES[[category]]$name,
      code_start = DISEASE_CATEGORIES[[category]]$code_range[1],
      code_end = DISEASE_CATEGORIES[[category]]$code_range[2],
      patient_count = count,
      total_patients = total_patients,
      prevalence_rate = prevalence,
      date_range = "2016-01-01 to 2024-12-31",
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, results)
}
