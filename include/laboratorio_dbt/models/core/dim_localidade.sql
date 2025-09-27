{{ config(unique_key='id_dim_localidade') }}

WITH src AS (
  SELECT
    TRIM(LOWER(nom_subsistema)) AS nom_subsistema,
    TRIM(LOWER(nom_estado))     AS nom_estado
  FROM {{ ref('stg_usina_disp_all') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['nom_subsistema','nom_estado']) }} AS id_dim_localidade,
  nom_subsistema,
  nom_estado
FROM src
GROUP BY 1,2,3
