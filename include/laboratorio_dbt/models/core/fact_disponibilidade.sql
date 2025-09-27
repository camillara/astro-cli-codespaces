{{ config(
    materialized='table',
    unique_key=['id_dim_usina','id_dim_localidade','id_dim_tempo'],
    post_hook=[
      "ALTER TABLE {{ this }} MODIFY COLUMN ID_DIM_USINA SET NOT NULL",
      "ALTER TABLE {{ this }} MODIFY COLUMN ID_DIM_LOCALIDADE SET NOT NULL",
      "ALTER TABLE {{ this }} MODIFY COLUMN ID_DIM_TEMPO SET NOT NULL"
    ]
) }}

WITH src AS (
  SELECT
    TRIM(LOWER(nom_usina))           AS nom_usina_n,
    TRIM(LOWER(nom_tipocombustivel)) AS nom_tipocombustivel_n,
    TRIM(UPPER(ceg))                 AS ceg_n,
    TRIM(LOWER(nom_subsistema))      AS nom_subsistema_n,
    TRIM(LOWER(nom_estado))          AS nom_estado_n,
    instante::TIMESTAMP_NTZ          AS instante,
    TRY_TO_DOUBLE(pot_instalada_mw)       AS pot_instalada_mw,
    TRY_TO_DOUBLE(disp_operacional_mw)    AS disp_operacional_mw,
    TRY_TO_DOUBLE(disp_sincronizada_mw)   AS disp_sincronizada_mw
  FROM {{ ref('stg_usina_disp_all') }}
  WHERE nom_usina        IS NOT NULL
    AND nom_tipocombustivel IS NOT NULL
    AND ceg              IS NOT NULL
    AND nom_subsistema   IS NOT NULL
    AND nom_estado       IS NOT NULL
    AND instante         IS NOT NULL
),

u AS ( SELECT * FROM {{ ref('dim_usina') }} ),
l AS ( SELECT * FROM {{ ref('dim_localidade') }} ),
t AS ( SELECT * FROM {{ ref('dim_tempo') }} )

SELECT
  u.id_dim_usina,
  l.id_dim_localidade,
  t.id_dim_tempo,
  DATE_TRUNC('day', s.instante)::DATE AS dia,
  s.pot_instalada_mw,
  s.disp_operacional_mw,
  s.disp_sincronizada_mw
FROM src s
JOIN u
  ON  s.nom_usina_n           = u.nom_usina
  AND s.nom_tipocombustivel_n = u.nom_tipocombustivel
  AND s.ceg_n                 = u.ceg
JOIN l
  ON  s.nom_subsistema_n      = l.nom_subsistema
  AND s.nom_estado_n          = l.nom_estado
JOIN t
  ON  s.instante              = t.instante
