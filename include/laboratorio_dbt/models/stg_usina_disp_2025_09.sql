SELECT
  ID_SUBSISTEMA,
  NOM_ESTADO,
  NOM_USINA,
  DIN_INSTANTE::TIMESTAMP       AS instante,
  VAL_POTENCIAINSTALADA         AS pot_instalada_mw,
  VAL_DISPOPERACIONAL           AS disp_operacional_mw,
  VAL_DISPSINCRONIZADA          AS disp_sincronizada_mw
FROM {{ source('staging','disponibilidade_usina_2025_09') }}
