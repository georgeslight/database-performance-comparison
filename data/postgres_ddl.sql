CREATE SCHEMA IF NOT EXISTS qdaba;

create table qdaba.anwendungsfall (
    id_anwendungsfall varchar(5) not null
        constraint pk_anwendungsfall primary key,
    anwendungsfalllang varchar not null
        constraint chk_anwfa_no_duplikat unique,
    gueltig_ab date not null,
    gueltig_bis date not null,
    typen bit varying not null,
    messart integer,
    anwendungsfall_metaname varchar(25)
        constraint uk_anwendungsfall unique,
    show_puenkt_einzel boolean default true,
    id_mvu varchar(10),
    ranking_wichtung_obj numeric,
    ranking_wichtung_subj numeric,
    color char(7),
    afz boolean default false
);

create table qdaba.verkehrstraeger (
    id_t_verkehrstraeger integer not null
        constraint pk_verkehrstraeger primary key,
    id_verkehrstraeger integer                                                                                not null
        constraint uk_id_verkehrstraeger unique,
    vt_sprach_key_kurz varchar(20),
    vt_sprach_key_lang varchar(100)
);

create table qdaba.kanton (
    id_t_kanton integer not null
        constraint pk_kanton primary key,
    id_kanton varchar(5) not null
        constraint uk_kanton_idkanton unique,
    kanton_kurz varchar not null
        constraint uk_kanton_kantonkurz unique,
    kanton_lang varchar(150) not null
        constraint uk_kanton_kantonlang unique,
    geom geometry
);

create table qdaba.mvu (
    id_mvu varchar(10) not null
        constraint pk_mvu primary key,
    mvulang varchar(50) not null
        constraint chk_mvu_no_duplikat unique,
    webservice_pin varchar(10)
        constraint uk_mvu_webservicepin unique,
    verkehrsmitteltyp integer[],
    mss_web boolean default true  not null,
    mss_mda boolean default true  not null,
    dpm_web boolean default true  not null,
    status integer default 0     not null,
    dpm_log_email text,
    dpm_log_email_absender boolean default false not null,
    zustaendigkeitsbereiche_ausschluss text,
    foto bytea,
    lnp_datei bytea,
    kontaktpersonen text
);

create table qdaba.verkehrsmittel_kategorie (
    id_anwendungsfall  varchar(4)        not null
        constraint fk_vmkat_anw references qdaba.anwendungsfall,
    id_vmkat           integer           not null,
    vmkat_kurz         varchar(25)       not null,
    vmkat_lang         varchar(50)       not null,
    id_verkehrstraeger integer default 1 not null
        constraint fk_id_verkehrstraeger references qdaba.verkehrstraeger (id_verkehrstraeger),
    vmkat_kurz_fr      varchar(25),
    vmkat_lang_fr      varchar(50),
    vmkat_kurz_it      varchar(25),
    vmkat_lang_it      varchar(50),
    lang_key           varchar(100)      not null,
    constraint pk_vmkat primary key (id_anwendungsfall, id_vmkat),
    constraint chk_vmkat_no_duplikat unique (vmkat_lang, id_anwendungsfall)
);

create table qdaba.import (
    id_t_import            integer not null
        constraint pk_import primary key,
    id_anwendungsfall      char(4)
        constraint fk_import_anw references qdaba.anwendungsfall,
    beschreibg             varchar(500)                                                         not null,
    import_typ             integer                                                              not null,
    import_time            timestamp                                                            not null,
    datpfad                varchar(500),
    datname                varchar(500),
    status                 integer                                                              not null,
    daten_von              date,
    daten_bis              date,
    freischaltung          integer default 0                                                    not null,
    vorberechnet           boolean default false,
    quelle                 integer default '-1'::integer,
    id_mvu                 varchar(10)
        constraint fk_import_mvu references qdaba.mvu on update cascade on delete restrict,
    status_pruefung        integer default 0                                                    not null,
    bemerkung              text,
    anschluss_vorberechnet boolean default false,
    username               varchar(500),
    fpl_id_t_import        integer,
    gonr                   integer
);

create table qdaba.bhf (
    id_anwendungsfall varchar(4)                                                     not null
        constraint fk_bhf_anw references qdaba.anwendungsfall,
    id_bhf            varchar                                                        not null
        constraint chk_didok_nr check (length((id_bhf)::text) = 7),
    bahnhof           varchar                                                        not null,
    import_date       date    default now(),
    qmt_messung       numeric(1),
    quelle            integer                                                        not null,
    typ               integer[],
    koord_x           numeric,
    koord_y           numeric,
    koord_z           numeric,
    id_t_bhf          integer not null
        constraint pk_bhf primary key,
    abkuerzung        varchar(10),
    id_t_kanton       integer
        references qdaba.kanton,
    gueltigkeit       daterange                                                      not null,
    go_nr             integer,
    gemeinde          varchar(40)
);

create table qdaba.linie (
    id_anwendungsfall      varchar(4)                                                         not null
        constraint fk_linie_anw
            references qdaba.anwendungsfall,
    id_linie               varchar                                                            not null,
    linie                  varchar(200)                                                       not null,
    linie_produkt          integer                                                            not null
        constraint chk_zuganschlussdefinition_linieprodukt
            check (linie_produkt = ANY (ARRAY ['-1'::integer, 1, 2, 3])),
    liniebemerkg           varchar(200),
    id_mvu                 varchar(10)                                                        not null
        constraint fk_linie_mvu
            references qdaba.mvu
            on update cascade,
    id_t_linie             integer not null
        constraint pk_linie
            primary key,
    typ                    integer default '-1'::integer                                      not null,
    gueltigkeit            daterange                                                          not null,
    linienlaenge           numeric(7, 3),
    fahrtkm_projahr        numeric(10, 3),
    personenkm_projahr     numeric(15, 3),
    kurspaare_projahr_mofr text,
    kurspaare_projahr_sa   text,
    kurspaare_projahr_so   text,
    id_mvu_gefahren        varchar(10)                                                        not null,
    messart                integer,
    produkt_detail         integer default '-1'::integer,
    dpmtool_ignore         boolean default false                                              not null,
    id_vmkat               integer                                                            not null,
    constraint uk_linie_idtlinie_mvu
        unique (id_t_linie, id_mvu),
    constraint fk_linie_vmkat
        foreign key (id_anwendungsfall, id_vmkat) references qdaba.verkehrsmittel_kategorie
            on update cascade
);

create table qdaba.puenkt (
    id_t_puenkt       bigint    not null
        constraint pk_puenkt_neu
            primary key,
    id_t_import       integer                                                                not null
        constraint fk_puenkt_neu_import
            references qdaba.import
            on update cascade on delete cascade,
    id_anwendungsfall varchar(4)                                                             not null
        constraint fk_puenkt_neu_anw
            references qdaba.anwendungsfall
            on delete restrict,
    id_mvu            varchar(10)                                                            not null
        constraint fk_puenkt_neu_mvu
            references qdaba.mvu
            on update cascade,
    id_linie_tu       varchar(10)                                                            not null,
    id_fahrt          varchar(20)                                                            not null,
    betriebstag       date                                                                   not null,
    fahrt_halt_lauf   integer   default '-1'::integer                                        not null,
    id_messpunkt      integer                                                                not null
        constraint fk_puenkt_neu_bhf
            references qdaba.bhf
            on update cascade,
    id_an_ab          integer                                                                not null,
    soll_bav          timestamp,
    soll_tu           timestamp                                                              not null,
    ist_tu            timestamp                                                              not null,
    id_verspcode      varchar(10),
    insert_time       timestamp default now(),
    id_t_linie_bav    integer
        constraint puenkt_neu_id_t_linie_bav_fkey
            references qdaba.linie
            on update cascade on delete restrict,
    qrelevant         boolean   default false                                                not null,
    go_nr             integer   default '-1'::integer                                        not null,
    id_t_puenkt_fahrt integer,
    manipulation_time timestamp,
    id_fahrt_tu       varchar(20),
    constraint uk_puenkt_neu
        unique (id_anwendungsfall, id_mvu, go_nr, id_t_linie_bav, betriebstag, id_fahrt, fahrt_halt_lauf, id_messpunkt,
                id_an_ab)
);

create table qdaba.puenkt_kat (
    id_t_puenktkat    integer  not null
        constraint pk_puenktkat
            primary key,
    id_anwendungsfall varchar(4)                                                                  not null
        constraint fk_puenktkat_anw
            references qdaba.anwendungsfall
            on delete restrict,
    puenkt_kat        integer                                                                     not null,
    move_type         varchar,
    constraint uk_puenktkat
        unique (id_anwendungsfall, puenkt_kat)
);

create table qdaba.puenkt_fahrt (
    id_t_puenkt_fahrt       serial
        constraint pk_puenkt_fahrt
            primary key,
    id_t_import             integer                        not null
        constraint fk_puenkt_fahrt_import
            references qdaba.import
            on update cascade on delete cascade,
    id_anwendungsfall       varchar(4)                     not null
        constraint fk_puenkt_fahrt_anw
            references qdaba.anwendungsfall
            on delete restrict,
    betriebstag             date                           not null,
    id_mvu                  varchar(10)                    not null
        constraint fk_puenkt_fahrt_mvu
            references qdaba.mvu
            on update cascade,
    go_nr                   integer                        not null,
    id_fahrt                varchar(50)                    not null,
    id_fahrt_differentiator smallint default 0             not null,
    id_fahrt_tu             varchar(50)                    not null,
    direction_id            smallint default '-1'::integer not null,
    fahrzeugtyp             varchar(10)
);


create table qdaba.linienbuendel (
    id_lb             varchar(4)                                                              not null,
    lb_kurz           varchar(20)                                                             not null,
    lb_lang           varchar(50)                                                             not null,
    id_anwendungsfall varchar(4)                                                              not null,
    id_vmkat          integer                                                                 not null,
    id_t_lb           integer not null
        constraint pk_lb
            primary key,
    gueltigkeit       daterange                                                               not null,
    constraint fk_lb_vmkat
        foreign key (id_anwendungsfall, id_vmkat) references qdaba.verkehrsmittel_kategorie
            on update cascade
);

create table qdaba.linie_lb (
    id_t_linie_lb integer not null
        constraint pk_linielb
            primary key,
    id_t_linie    integer                                                                  not null
        references qdaba.linie
            on update cascade on delete restrict,
    id_t_lb       integer                                                                  not null
        references qdaba.linienbuendel
            on update cascade on delete restrict,
    gueltigkeit   daterange                                                                not null
);
