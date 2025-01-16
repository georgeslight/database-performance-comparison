-- auto-generated definition
CREATE TABLE
    mv_dpm_vorberechnet_neu (
        puenkt_kat INTEGER,
        id_t_import INTEGER CONSTRAINT fk_mv_dpm_vorberechnet_neu_import REFERENCES IMPORT ON UPDATE CASCADE ON DELETE CASCADE,
        id_anwendungsfall VARCHAR(4),
        id_mvu VARCHAR(10),
        id_linie_tu VARCHAR(10),
        betriebstag date,
        id_messpunkt INTEGER,
        id_an_ab INTEGER,
        id_t_linie_bav INTEGER,
        qrelevant BOOLEAN,
        id_verspcode VARCHAR(10),
        anz_puenkt_tu BIGINT,
        anz_puenkt_bav BIGINT,
        anz_mess_tu BIGINT,
        anz_mess_bav BIGINT,
        go_nr INTEGER,
        id_t_lb INTEGER,
        direction_id SMALLINT,
        id_t_betreiber INTEGER,
        id_t_haltecode SMALLINT,
        id serial CONSTRAINT pk_mv_dpm_vorberechnet_neu PRIMARY KEY
    );


CREATE INDEX idx_mv_dpm_vorberechnet_neu_auswertung_2 ON mv_dpm_vorberechnet_neu (
    id_anwendungsfall,
    puenkt_kat,
    id_an_ab,
    id_mvu,
    betriebstag,
    id_t_linie_bav
)
WHERE
    (
        qrelevant
        AND (id_t_linie_bav IS NOT NULL)
        AND (
            (id_verspcode IS NULL)
            OR ((id_verspcode)::TEXT <> '99'::TEXT)
        )
    );


CREATE INDEX idx_mv_dpm_vorberechnet_neu_auswertung ON mv_dpm_vorberechnet_neu (id_anwendungsfall, puenkt_kat, betriebstag);


CREATE INDEX idx_mv_dpm_vorberechnet_neu_liefernachweis ON mv_dpm_vorberechnet_neu (id_t_import);


CREATE INDEX idx_mv_dpm_vorberechnet_neu_vorberechnen ON mv_dpm_vorberechnet_neu (
    (DATE_PART('year'::TEXT, betriebstag)::INTEGER),
    id_t_linie_bav,
    id_messpunkt
);


CREATE INDEX idx_mv_dpm_vorberechnet_neu_auswertung_linie ON mv_dpm_vorberechnet_neu (id_t_linie_bav, betriebstag, puenkt_kat, id_an_ab)
WHERE
    (
        qrelevant
        AND (id_t_linie_bav IS NOT NULL)
        AND (
            (id_verspcode IS NULL)
            OR ((id_verspcode)::TEXT <> '99'::TEXT)
        )
    );


CREATE INDEX idx_mv_dpm_vorberechnet_neu_auswertung_linie_tu ON mv_dpm_vorberechnet_neu (
    id_anwendungsfall,
    id_t_linie_bav,
    betriebstag,
    puenkt_kat,
    id_an_ab,
    id_mvu
)
WHERE
    (
        qrelevant
        AND (id_t_linie_bav IS NOT NULL)
        AND (
            (id_verspcode IS NULL)
            OR ((id_verspcode)::TEXT <> '99'::TEXT)
        )
    );


CREATE INDEX idx_mv_dpm_vorberechnet_neu_lieferprotokoll ON mv_dpm_vorberechnet_neu (betriebstag, id_mvu)
WHERE
    (puenkt_kat = 119);


CREATE INDEX idx_mv_dpm_vorberechnet_pag_auswertung ON mv_dpm_vorberechnet_neu (
    id_anwendungsfall,
    betriebstag,
    id_mvu,
    puenkt_kat,
    id_t_linie_bav,
    direction_id,
    id_t_betreiber,
    qrelevant,
    id_an_ab,
    id_t_haltecode
);