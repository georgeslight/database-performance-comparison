-- auto-generated definition
create table mv_dpm_vorberechnet_neu
(
    puenkt_kat        integer,
    id_t_import       integer
        constraint fk_mv_dpm_vorberechnet_neu_import
            references import
            on update cascade on delete cascade,
    id_anwendungsfall varchar(4),
    id_mvu            varchar(10),
    id_linie_tu       varchar(10),
    betriebstag       date,
    id_messpunkt      integer,
    id_an_ab          integer,
    id_t_linie_bav    integer,
    qrelevant         boolean,
    id_verspcode      varchar(10),
    anz_puenkt_tu     bigint,
    anz_puenkt_bav    bigint,
    anz_mess_tu       bigint,
    anz_mess_bav      bigint,
    go_nr             integer,
    id_t_lb           integer,
    direction_id      smallint,
    id_t_betreiber    integer,
    id_t_haltecode    smallint,
    id                serial
        constraint pk_mv_dpm_vorberechnet_neu
            primary key
);

create index idx_mv_dpm_vorberechnet_neu_auswertung_2
    on mv_dpm_vorberechnet_neu (id_anwendungsfall, puenkt_kat, id_an_ab, id_mvu, betriebstag, id_t_linie_bav)
    where (qrelevant AND (id_t_linie_bav IS NOT NULL) AND
           ((id_verspcode IS NULL) OR ((id_verspcode)::text <> '99'::text)));

create index idx_mv_dpm_vorberechnet_neu_auswertung
    on mv_dpm_vorberechnet_neu (id_anwendungsfall, puenkt_kat, betriebstag);

create index idx_mv_dpm_vorberechnet_neu_liefernachweis
    on mv_dpm_vorberechnet_neu (id_t_import);

create index idx_mv_dpm_vorberechnet_neu_vorberechnen
    on mv_dpm_vorberechnet_neu ((date_part('year'::text, betriebstag)::integer), id_t_linie_bav, id_messpunkt);

create index idx_mv_dpm_vorberechnet_neu_auswertung_linie
    on mv_dpm_vorberechnet_neu (id_t_linie_bav, betriebstag, puenkt_kat, id_an_ab)
    where (qrelevant AND (id_t_linie_bav IS NOT NULL) AND
           ((id_verspcode IS NULL) OR ((id_verspcode)::text <> '99'::text)));

create index idx_mv_dpm_vorberechnet_neu_auswertung_linie_tu
    on mv_dpm_vorberechnet_neu (id_anwendungsfall, id_t_linie_bav, betriebstag, puenkt_kat, id_an_ab, id_mvu)
    where (qrelevant AND (id_t_linie_bav IS NOT NULL) AND
           ((id_verspcode IS NULL) OR ((id_verspcode)::text <> '99'::text)));

create index idx_mv_dpm_vorberechnet_neu_lieferprotokoll
    on mv_dpm_vorberechnet_neu (betriebstag, id_mvu)
    where (puenkt_kat = 119);

create index idx_mv_dpm_vorberechnet_pag_auswertung
    on mv_dpm_vorberechnet_neu (id_anwendungsfall, betriebstag, id_mvu, puenkt_kat, id_t_linie_bav, direction_id,
                                id_t_betreiber, qrelevant, id_an_ab, id_t_haltecode);