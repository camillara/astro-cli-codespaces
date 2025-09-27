{{ config(unique_key='id_dim_usina') }}

WITH src AS (
  SELECT
    TRIM(LOWER(nom_usina))           AS nom_usina,
    TRIM(LOWER(nom_tipocombustivel)) AS nom_tipocombustivel,
    TRIM(UPPER(ceg))                 AS ceg
  FROM {{ ref('stg_usina_disp_all') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['nom_usina','nom_tipocombustivel','ceg']) }} AS id_dim_usina,
  nom_usina,
  nom_tipocombustivel,
  ceg
FROM src
GROUP BY 1,2,3,4
