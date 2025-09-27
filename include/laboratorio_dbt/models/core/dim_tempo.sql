{{ config(unique_key='id_dim_tempo') }}

WITH src AS (
  SELECT DISTINCT
    instante
  FROM {{ ref('stg_usina_disp_all') }}
  WHERE instante IS NOT NULL
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['TO_CHAR(instante, \'YYYY-MM-DD HH24:MI:SS\')']) }} AS id_dim_tempo,
  instante,
  YEAR(instante)  AS ano,
  MONTH(instante) AS mes,
  DAY(instante)   AS dia,
  HOUR(instante)  AS hora
FROM src
