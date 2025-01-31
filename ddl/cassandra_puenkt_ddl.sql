INSTALL postgres;


LOAD postgres;


CREATE SCHEMA qdaba;


CREATE TABLE
    qdaba.verkehrstraeger (
        id_t_verkehrstraeger INTEGER PRIMARY KEY,
        id_verkehrstraeger INTEGER NOT NULL UNIQUE,
        vt_sprach_key_kurz VARCHAR(20),
        vt_sprach_key_lang VARCHAR(100)
    );


CREATE TABLE
    qdaba.kanton (
        id_t_kanton INTEGER PRIMARY KEY,
        id_kanton VARCHAR(5) NOT NULL UNIQUE,
        kanton_kurz VARCHAR NOT NULL UNIQUE,
        kanton_lang VARCHAR(150) NOT NULL UNIQUE,
        geom VARCHAR,
    );


CREATE TABLE
    qdaba.verkehrsmittel_kategorie (
        id_anwendungsfall VARCHAR(4) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        id_vmkat INTEGER,
        vmkat_kurz VARCHAR(25) NOT NULL,
        vmkat_lang VARCHAR(50) NOT NULL,
        id_verkehrstraeger INTEGER NOT NULL DEFAULT 1 REFERENCES qdaba.verkehrstraeger (id_verkehrstraeger),
        vmkat_kurz_fr VARCHAR(25),
        vmkat_lang_fr VARCHAR(50),
        vmkat_kurz_it VARCHAR(25),
        vmkat_lang_it VARCHAR(50),
        lang_key VARCHAR(100) NOT NULL,
        PRIMARY KEY (id_anwendungsfall, id_vmkat),
        UNIQUE (vmkat_lang, id_anwendungsfall)
    );


------------------------------------------------------------------------------------------------------------------------
CREATE TABLE
    qdaba.puenkt (
        id_t_puenkt BIGINT,
        id_t_import INT,
        id_anwendungsfall TEXT,
        id_mvu TEXT,
        id_linie_tu TEXT,
        id_fahrt TEXT,
        betriebstag DATE,
        fahrt_halt_lauf INT,
        id_messpunkt INT,
        id_an_ab INT,
        soll_bav TIMESTAMP,
        soll_tu TIMESTAMP,
        ist_tu TIMESTAMP,
        id_verspcode TEXT,
        insert_time TIMESTAMP,
        id_t_linie_bav INT,
        qrelevant BOOLEAN,
        go_nr INT,
        id_t_puenkt_fahrt INT,
        manipulation_time TIMESTAMP,
        id_fahrt_tu TEXT,
        bav_delta DOUBLE,
        tu_delta DOUBLE,
        id_t_lb INT,
        direction_id SMALLINT,
        PRIMARY KEY (
            (id_mvu),
            id_anwendungsfall,
            go_nr,
            id_t_linie_bav,
            betriebstag,
            id_fahrt,
            fahrt_halt_lauf,
            id_messpunkt,
            id_an_ab
        )
    );


CREATE
OR REPLACE TABLE qdaba.linie (
    id_t_linie INTEGER PRIMARY KEY,
    id_anwendungsfall VARCHAR(4) NOT NULL REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    id_linie VARCHAR NOT NULL,
    linie VARCHAR(200) NOT NULL,
    linie_produkt INTEGER NOT NULL,
    liniebemerkg VARCHAR(200),
    id_mvu VARCHAR(10) NOT NULL REFERENCES qdaba.mvu (id_mvu),
    typ INTEGER NOT NULL DEFAULT '-1'::INTEGER,
    gueltigkeit_start DATE NOT NULL,
    gueltigkeit_end DATE,
    linienlaenge NUMERIC(7, 3),
    fahrtkm_projahr NUMERIC(10, 3),
    personenkm_projahr NUMERIC(15, 3),
    kurspaare_projahr_mofr VARCHAR,
    kurspaare_projahr_sa VARCHAR,
    kurspaare_projahr_so VARCHAR,
    id_mvu_gefahren VARCHAR(10) NOT NULL,
    messart INTEGER,
    produkt_detail INTEGER DEFAULT '-1'::INTEGER,
    dpmtool_ignore BOOLEAN NOT NULL DEFAULT FALSE,
    id_vmkat INTEGER NOT NULL,
    FOREIGN KEY (id_anwendungsfall, id_vmkat) REFERENCES qdaba.verkehrsmittel_kategorie (id_anwendungsfall, id_vmkat),
    UNIQUE (id_t_linie, id_mvu)
);


CREATE TABLE
    qdaba.puenkt_fahrt (
        id_t_puenkt_fahrt INTEGER PRIMARY KEY,
        id_t_import INTEGER NOT NULL REFERENCES qdaba.import (id_t_import),
        id_anwendungsfall VARCHAR(4) NOT NULL REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        betriebstag DATE,
        id_mvu VARCHAR(10) NOT NULL REFERENCES qdaba.mvu (id_mvu),
        go_nr INTEGER NOT NULL,
        id_fahrt VARCHAR(50) NOT NULL,
        id_fahrt_differentiator SMALLINT NOT NULL DEFAULT 0,
        id_fahrt_tu VARCHAR(50) NOT NULL,
        direction_id SMALLINT NOT NULL DEFAULT '-1'::INTEGER,
        fahrzeugtyp VARCHAR(10)
    );


CREATE TABLE
    qdaba.linienbuendel (
        id_lb VARCHAR(4) NOT NULL,
        lb_kurz VARCHAR(20) NOT NULL,
        lb_lang VARCHAR(50) NOT NULL,
        id_anwendungsfall VARCHAR(4) NOT NULL,
        id_vmkat INTEGER NOT NULL,
        id_t_lb INTEGER PRIMARY KEY,
        gueltigkeit_start DATE NOT NULL,
        gueltigkeit_end DATE,
        FOREIGN KEY (id_anwendungsfall, id_vmkat) REFERENCES qdaba.verkehrsmittel_kategorie (id_anwendungsfall, id_vmkat)
    );


CREATE TABLE
    qdaba.linie_lb (
        id_t_linie_lb INTEGER PRIMARY KEY,
        id_t_linie INTEGER NOT NULL REFERENCES qdaba.linie (id_t_linie),
        id_t_lb INTEGER NOT NULL REFERENCES qdaba.linienbuendel (id_t_lb),
        gueltigkeit_start DATE NOT NULL,
        gueltigkeit_end DATE
    );


CREATE TABLE
    qdaba.puenkt_kat (
        id_t_puenktkat INTEGER PRIMARY KEY,
        id_anwendungsfall VARCHAR(4) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        puenkt_kat INTEGER,
        move_type VARCHAR,
        UNIQUE (id_anwendungsfall, puenkt_kat)
    );