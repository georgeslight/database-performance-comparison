SELECT
    p.*,
    CASE
        WHEN DATE_PART('epoch', p.ist_tu - p.soll_bav) < 43400::DOUBLE PRECISION THEN DATE_PART('epoch', p.ist_tu - p.soll_bav)
        ELSE DATE_PART('epoch', p.ist_tu - p.soll_bav) - 86400::DOUBLE PRECISION
    END AS bav_delta,
    CASE
        WHEN DATE_PART('epoch', p.ist_tu - p.soll_tu) < 43400::DOUBLE PRECISION THEN DATE_PART('epoch', p.ist_tu - p.soll_tu)
        ELSE DATE_PART('epoch', p.ist_tu - p.soll_tu) - 86400::DOUBLE PRECISION
    END AS tu_delta,
    DATE_PART('epoch', p.ist_tu - p.soll_tu) AS tu_delta_raw
FROM
    puenkt p;


------------------------ JOIN WITH puenkt_fahrt
SELECT
    p.*,
    CASE
        WHEN DATE_PART('epoch', p.ist_tu - p.soll_bav) < 43400::DOUBLE PRECISION THEN DATE_PART('epoch', p.ist_tu - p.soll_bav)
        ELSE DATE_PART('epoch', p.ist_tu - p.soll_bav) - 86400::DOUBLE PRECISION
    END AS bav_delta,
    CASE
        WHEN DATE_PART('epoch', p.ist_tu - p.soll_tu) < 43400::DOUBLE PRECISION THEN DATE_PART('epoch', p.ist_tu - p.soll_tu)
        ELSE DATE_PART('epoch', p.ist_tu - p.soll_tu) - 86400::DOUBLE PRECISION
    END AS tu_delta,
    DATE_PART('epoch', p.ist_tu - p.soll_tu) AS tu_delta_raw,
    pf.id_t_puenkt_fahrt
FROM
    puenkt p
    LEFT JOIN puenkt_fahrt pf ON pf.id_t_puenkt_fahrt = p.id_t_puenkt_fahrt
LIMIT
    50;

----------------------- ALL

SELECT 
    p.*, 
    CASE 
        WHEN DATE_PART('epoch', p.ist_tu - p.soll_bav) < 43400::DOUBLE PRECISION THEN DATE_PART('epoch', p.ist_tu - p.soll_bav) 
        ELSE DATE_PART('epoch', p.ist_tu - p.soll_bav) - 86400::DOUBLE PRECISION 
    END AS bav_delta, 
    CASE 
        WHEN DATE_PART('epoch', p.ist_tu - p.soll_tu) < 43400::DOUBLE PRECISION THEN DATE_PART('epoch', p.ist_tu - p.soll_tu) 
        ELSE DATE_PART('epoch', p.ist_tu - p.soll_tu) - 86400::DOUBLE PRECISION 
    END AS tu_delta, 
    lb.id_t_lb, 
    pf.direction_id, 
    pk.puenkt_kat, 
    pk.move_type 
FROM 
    puenkt p 
    LEFT JOIN puenkt_fahrt pf ON pf.id_t_puenkt_fahrt = p.id_t_puenkt_fahrt 
    LEFT JOIN linie l ON l.id_t_linie = p.id_t_linie_bav 
    LEFT JOIN linie_lb llb ON llb.id_t_linie = l.id_t_linie AND p.betriebstag BETWEEN llb.gueltigkeit_start AND llb.gueltigkeit_end 
    LEFT JOIN linienbuendel lb ON lb.id_t_lb = llb.id_t_lb AND p.betriebstag BETWEEN lb.gueltigkeit_start AND lb.gueltigkeit_end 
    JOIN puenkt_kat pk ON p.id_anwendungsfall::TEXT = pk.id_anwendungsfall::TEXT 
        AND (
            pk.move_type::TEXT = 'ALL'::TEXT 
            OR pk.move_type::TEXT = CASE 
                WHEN p.id_an_ab = 1 THEN 'ARRIVAL'::TEXT 
                WHEN p.id_an_ab = 2 THEN 'DEPARTURE'::TEXT 
                ELSE NULL::TEXT 
            END); 