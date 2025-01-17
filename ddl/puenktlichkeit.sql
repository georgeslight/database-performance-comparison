WITH fahrt_details AS (
    SELECT
        id_fahrt,
        id_t_linie_bav,
        betriebstag,
        EXTRACT(EPOCH FROM (ist_tu - soll_bav)) AS puenktlichkeit_seconds
    FROM qdaba.puenkt
    WHERE soll_bav IS NOT NULL
      AND ist_tu IS NOT NULL
),
aggregated_stats AS (
    SELECT
        id_fahrt,
        id_t_linie_bav,
        betriebstag,
        AVG(puenktlichkeit_seconds) AS avg_puenktlichkeit,
        MIN(puenktlichkeit_seconds) AS min_puenktlichkeit,
        MAX(puenktlichkeit_seconds) AS max_puenktlichkeit,
        STDDEV_POP(puenktlichkeit_seconds) AS stddev_puenktlichkeit
    FROM fahrt_details
    GROUP BY id_fahrt, id_t_linie_bav, betriebstag
),
ranked_fahrten AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY id_t_linie_bav, betriebstag ORDER BY avg_puenktlichkeit DESC) AS rank_puenktlichkeit
    FROM aggregated_stats
)
SELECT
    id_fahrt,
    id_t_linie_bav,
    betriebstag,
    avg_puenktlichkeit,
    min_puenktlichkeit,
    max_puenktlichkeit,
    stddev_puenktlichkeit,
    rank_puenktlichkeit
FROM ranked_fahrten
ORDER BY betriebstag, id_t_linie_bav, rank_puenktlichkeit;

-- SELECT
--     id_fahrt,
--     id_t_linie_bav,
--     betriebstag,
--     AVG(EXTRACT(EPOCH FROM (ist_tu - soll_bav))) AS avg_puenktlichkeit_seconds,
--     MIN(EXTRACT(EPOCH FROM (ist_tu - soll_bav))) AS min_puenktlichkeit_seconds,
--     MAX(EXTRACT(EPOCH FROM (ist_tu - soll_bav))) AS max_puenktlichkeit_seconds
-- FROM qdaba.puenkt
-- WHERE soll_bav IS NOT NULL
--   AND ist_tu IS NOT NULL
-- GROUP BY id_fahrt, id_t_linie_bav, betriebstag
-- ORDER BY betriebstag, id_t_linie_bav, id_fahrt;
