SELECT
    p.betriebstag,
    countIf(p.tu_delta IS NOT NULL AND ((pk.puenkt_kat > 0 AND p.tu_delta <= pk.puenkt_kat) OR (pk.puenkt_kat <= 0 AND p.tu_delta >= pk.puenkt_kat))) as punctual_trips,
    countIf(p.tu_delta IS NOT NULL AND ((p.tu_delta > 0 AND p.tu_delta > pk.puenkt_kat) OR (pk.puenkt_kat <= 0 AND p.tu_delta < pk.puenkt_kat))) AS delayed_trips
FROM
(SELECT
    p_1.id_t_import AS id_t_import,
    p_1.id_anwendungsfall AS id_anwendungsfall,
    p_1.id_mvu AS id_mvu,
    p_1.id_linie_tu AS id_linie_tu,
    p_1.betriebstag AS betriebstag,
    p_1.id_messpunkt AS id_messpunkt,
    p_1.id_an_ab AS id_an_ab,
    p_1.id_t_linie_bav AS id_t_linie_bav,
    p_1.qrelevant AS qrelevant,
    p_1.id_verspcode AS id_verspcode,
    p_1.go_nr AS go_nr, 
    p_1.id_t_lb AS id_t_lb,
    p_1.direction_id AS direction_id,
    p_1.soll_bav AS soll_bav,
    p_1.soll_tu AS soll_tu,
    if(p_1.bav_delta_raw < 43200, p_1.bav_delta_raw, p_1.bav_delta_raw - 86400) AS bav_delta,
    if(p_1.tu_delta_raw < 43200, p_1.tu_delta_raw, p_1.tu_delta_raw - 86400) AS tu_delta
FROM
    (
        SELECT
            p_2.id_t_import AS id_t_import,
            p_2.id_anwendungsfall AS id_anwendungsfall,
            p_2.id_mvu AS id_mvu,
            p_2.id_linie_tu AS id_linie_tu,
            p_2.betriebstag AS betriebstag,
            p_2.id_messpunkt AS id_messpunkt,
            p_2.id_an_ab AS id_an_ab,
            p_2.id_t_linie_bav AS id_t_linie_bav,
            p_2.qrelevant AS qrelevant,
            p_2.id_verspcode AS id_verspcode,
            p_2.go_nr AS go_nr,
            CAST(p_2.ist_tu AS DateTime) - CAST(p_2.soll_bav AS DateTime) AS bav_delta_raw,
            CAST(p_2.ist_tu AS DateTime) - CAST(p_2.soll_tu AS DateTime) AS tu_delta_raw,
            p_2.soll_bav AS soll_bav,
            p_2.soll_tu AS soll_tu,
            lb.id_t_lb AS id_t_lb,
            pf.direction_id AS direction_id
        FROM
            puenkt p_2
                LEFT JOIN puenkt_fahrt pf ON pf.id_t_puenkt_fahrt = p_2.id_t_puenkt_fahrt
                LEFT JOIN linie l ON l.id_t_linie = p_2.id_t_linie_bav
                LEFT JOIN linie_lb llb ON llb.id_t_linie = l.id_t_linie AND p_2.betriebstag BETWEEN toDate(substring(llb.gueltigkeit, 2, 10)) AND toDateOrNull(substring(llb.gueltigkeit, 13, length(llb.gueltigkeit) - 13))
                LEFT JOIN linienbuendel lb ON lb.id_t_lb = llb.id_t_lb AND p_2.betriebstag BETWEEN toDate(substring(lb.gueltigkeit, 2, 10)) AND toDateOrNull(substring(lb.gueltigkeit, 13, length(lb.gueltigkeit) - 13))
        ) p_1) p JOIN puenkt_kat pk ON CAST(p.id_anwendungsfall AS String) = CAST(p.id_anwendungsfall AS String)
    AND (CAST(pk.move_type AS String) = CAST('ALL' AS String)
        OR CAST(pk.move_type AS String) = if(p.id_an_ab = 1, 'ARRIVAL', 'DEPARTURE'))
GROUP BY p.betriebstag;