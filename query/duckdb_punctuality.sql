SELECT pk.puenkt_kat,
       p.id_t_import,
       p.id_anwendungsfall,
       p.id_mvu,
       p.id_linie_tu,
       p.betriebstag,
       p.id_messpunkt,
       p.id_an_ab,
       p.id_t_linie_bav,
       p.qrelevant,
       p.id_verspcode,
       sum(
               CASE
                   WHEN p.tu_delta IS NULL OR pk.puenkt_kat > 0 AND p.tu_delta > pk.puenkt_kat::double precision OR
                        pk.puenkt_kat <= 0 AND p.tu_delta < pk.puenkt_kat::double precision THEN 0
                   ELSE 1
                   END)                                  AS anz_puenkt_tu,
       sum(
               CASE
                   WHEN p.bav_delta IS NULL OR pk.puenkt_kat > 0 AND p.bav_delta > pk.puenkt_kat::double precision OR
                        pk.puenkt_kat <= 0 AND p.bav_delta < pk.puenkt_kat::double precision THEN 0
                   ELSE 1
                   END)                                  AS anz_puenkt_bav,
       sum(
               CASE
                   WHEN p.soll_tu IS NOT NULL THEN 1
                   ELSE 0
                   END)                                  AS anz_mess_tu,
       sum(
               CASE
                   WHEN p.soll_bav IS NOT NULL THEN 1
                   ELSE 0
                   END)                                  AS anz_mess_bav,
       p.go_nr,
       p.id_t_lb,
       COALESCE(p.direction_id, '-1'::integer::smallint) AS direction_id
FROM (SELECT p_1.id_t_import,
             p_1.id_anwendungsfall,
             p_1.id_mvu,
             p_1.id_linie_tu,
             p_1.betriebstag,
             p_1.id_messpunkt,
             p_1.id_an_ab,
             p_1.id_t_linie_bav,
             p_1.qrelevant,
             p_1.id_verspcode,
             p_1.go_nr,
             p_1.id_t_lb,
             p_1.direction_id,
             CASE
                 WHEN p_1.bav_delta_raw < 43200::double precision THEN p_1.bav_delta_raw
                 ELSE p_1.bav_delta_raw - 86400::double precision
                 END AS bav_delta,
             CASE
                 WHEN p_1.tu_delta_raw < 43200::double precision THEN p_1.tu_delta_raw
                 ELSE p_1.tu_delta_raw - 86400::double precision
                 END AS tu_delta,
             p_1.soll_bav,
             p_1.soll_tu
      FROM (SELECT p_2.id_t_import,
                   p_2.id_anwendungsfall,
                   p_2.id_mvu,
                   p_2.id_linie_tu,
                   p_2.betriebstag,
                   p_2.id_messpunkt,
                   p_2.id_an_ab,
                   p_2.id_t_linie_bav,
                   p_2.qrelevant,
                   p_2.id_verspcode,
                   p_2.go_nr,
                   date_part('epoch'::text, p_2.ist_tu - p_2.soll_bav) AS bav_delta_raw,
                   date_part('epoch'::text, p_2.ist_tu - p_2.soll_tu)  AS tu_delta_raw,
                   p_2.soll_bav,
                   p_2.soll_tu,
                   lb.id_t_lb,
                   pf.direction_id
            FROM qdaba.puenkt p_2
                LEFT JOIN qdaba.puenkt_fahrt pf ON pf.id_t_puenkt_fahrt = p_2.id_t_puenkt_fahrt
                LEFT JOIN qdaba.linie l ON l.id_t_linie = p_2.id_t_linie_bav
                LEFT JOIN qdaba.linie_lb llb ON llb.id_t_linie = l.id_t_linie AND p_2.betriebstag BETWEEN llb.gueltigkeit_start AND llb.gueltigkeit_ende
                LEFT JOIN qdaba.linienbuendel lb ON lb.id_t_lb = llb.id_t_lb AND p_2.betriebstag BETWEEN lb.gueltigkeit_start AND lb.gueltigkeit_ende) p_1) p
         JOIN qdaba.puenkt_kat pk ON p.id_anwendungsfall::text = pk.id_anwendungsfall::text AND
    (pk.move_type::text = 'ALL'::text OR pk.move_type::text =
    CASE
    WHEN p.id_an_ab = 1 THEN 'ARRIVAL'::text
    WHEN p.id_an_ab = 2 THEN 'DEPARTURE'::text
    ELSE NULL::text
    END)
GROUP BY pk.puenkt_kat, p.id_t_import, p.id_anwendungsfall, p.id_mvu, p.go_nr, p.id_linie_tu, p.id_t_lb, p.betriebstag,
    p.id_messpunkt, p.id_an_ab, p.id_t_linie_bav, p.qrelevant, p.id_verspcode, p.direction_id
ORDER BY p.betriebstag, p.id_mvu, p.go_nr, p.id_t_linie_bav, p.id_messpunkt;