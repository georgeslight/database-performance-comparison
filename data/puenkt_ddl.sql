create table if not exists qdaba.puenkt
(
    id_t_puenkt       bigint    not null
        constraint pk_puenkt_neu
            primary key,
    id_t_import       integer,
    id_anwendungsfall varchar(4),
    id_mvu            varchar(10),
    id_linie_tu       varchar(10)                                                            not null,
    id_fahrt          varchar(20)                                                            not null,
    betriebstag       date                                                                   not null,
    fahrt_halt_lauf   integer   default '-1'::integer                                        not null,
    id_messpunkt      integer,
    id_an_ab          integer                                                                not null,
    soll_bav          timestamp,
    soll_tu           timestamp                                                              not null,
    ist_tu            timestamp                                                              not null,
    id_verspcode      varchar(10),
    insert_time       timestamp default now(),
    id_t_linie_bav    integer,
    qrelevant         boolean   default false                                                not null,
    go_nr             integer   default '-1'::integer                                        not null,
    id_t_puenkt_fahrt integer,
    manipulation_time timestamp,
    id_fahrt_tu       varchar(20),
    constraint uk_puenkt_neu
        unique (id_anwendungsfall, id_mvu, go_nr, id_t_linie_bav, betriebstag, id_fahrt, fahrt_halt_lauf, id_messpunkt,
                id_an_ab)
);
