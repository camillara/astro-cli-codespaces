{{ config(materialized='view') }}

WITH m07 AS (
  SELECT
    ID_SUBSISTEMA                              AS id_subsistema,
    NOM_SUBSISTEMA                             AS nom_subsistema,
    ID_ESTADO                                  AS id_estado,
    NOM_ESTADO                                 AS nom_estado,
    NOM_USINA                                  AS nom_usina,
    ID_TIPOUSINA                               AS id_tipousina,
    NOM_TIPOCOMBUSTIVEL                        AS nom_tipocombustivel,
    ID_ONS                                     AS id_ons,
    CEG                                        AS ceg,
    TO_TIMESTAMP_NTZ(DIN_INSTANTE)             AS instante,
    TRY_TO_DOUBLE(VAL_POTENCIAINSTALADA)       AS pot_instalada_mw,
    TRY_TO_DOUBLE(VAL_DISPOPERACIONAL)         AS disp_operacional_mw,
    TRY_TO_DOUBLE(VAL_DISPSINCRONIZADA)        AS disp_sincronizada_mw,
    '2025-07'                                  AS mes_ref
  FROM {{ source('staging','disponibilidade_usina_2025_07') }}
),
m08 AS (
  SELECT
    ID_SUBSISTEMA, NOM_SUBSISTEMA, ID_ESTADO, NOM_ESTADO, NOM_USINA,
    ID_TIPOUSINA, NOM_TIPOCOMBUSTIVEL, ID_ONS, CEG,
    TO_TIMESTAMP_NTZ(DIN_INSTANTE) AS instante,
    TRY_TO_DOUBLE(VAL_POTENCIAINSTALADA) AS pot_instalada_mw,
    TRY_TO_DOUBLE(VAL_DISPOPERACIONAL)   AS disp_operacional_mw,
    TRY_TO_DOUBLE(VAL_DISPSINCRONIZADA)  AS disp_sincronizada_mw,
    '2025-08' AS mes_ref
  FROM {{ source('staging','disponibilidade_usina_2025_08') }}
),
m09 AS (
  SELECT
    ID_SUBSISTEMA, NOM_SUBSISTEMA, ID_ESTADO, NOM_ESTADO, NOM_USINA,
    ID_TIPOUSINA, NOM_TIPOCOMBUSTIVEL, ID_ONS, CEG,
    TO_TIMESTAMP_NTZ(DIN_INSTANTE) AS instante,
    TRY_TO_DOUBLE(VAL_POTENCIAINSTALADA) AS pot_instalada_mw,
    TRY_TO_DOUBLE(VAL_DISPOPERACIONAL)   AS disp_operacional_mw,
    TRY_TO_DOUBLE(VAL_DISPSINCRONIZADA)  AS disp_sincronizada_mw,
    '2025-09' AS mes_ref
  FROM {{ source('staging','disponibilidade_usina_2025_09') }}
)
SELECT * FROM m07
UNION ALL
SELECT * FROM m08
UNION ALL
SELECT * FROM m09
