INSTALL postgres;


LOAD postgres;


ATTACH 'dbname=qdaba user=postgres host=192.168.100.103 port=5432 password=oracle' AS pg (
    TYPE POSTGRES,
    READ_ONLY
);


CREATE SCHEMA qdaba;


CREATE TABLE
    qdaba.anwendungsfall (
        id_anwendungsfall VARCHAR(5) PRIMARY KEY,
        anwendungsfalllang VARCHAR NOT NULL UNIQUE,
        gueltig_ab DATE NOT NULL,
        gueltig_bis DATE NOT NULL,
        typen BIT NOT NULL,
        messart INTEGER,
        anwendungsfall_metaname VARCHAR(25) UNIQUE,
        show_puenkt_einzel BOOLEAN DEFAULT TRUE,
        id_mvu VARCHAR(10),
        ranking_wichtung_obj NUMERIC,
        ranking_wichtung_subj NUMERIC,
        color CHAR(7),
        afz BOOLEAN DEFAULT FALSE
    );


INSERT INTO
    qdaba.anwendungsfall
SELECT
    *
FROM
    pg.qdaba.anwendungsfall;


CREATE TABLE
    qdaba.verkehrstraeger (
        id_t_verkehrstraeger INTEGER PRIMARY KEY,
        id_verkehrstraeger INTEGER NOT NULL UNIQUE,
        vt_sprach_key_kurz VARCHAR(20),
        vt_sprach_key_lang VARCHAR(100)
    );


INSERT INTO
    qdaba.verkehrstraeger
SELECT
    *
FROM
    pg.qdaba.verkehrstraeger;


CREATE TABLE
    qdaba.mvu (
        id_mvu VARCHAR(10) PRIMARY KEY,
        mvulang VARCHAR(50) NOT NULL UNIQUE,
        webservice_pin VARCHAR(10) UNIQUE,
        verkehrsmitteltyp INTEGER[],
        mss_web BOOLEAN NOT NULL DEFAULT TRUE,
        mss_mda BOOLEAN NOT NULL DEFAULT TRUE,
        dpm_web BOOLEAN NOT NULL DEFAULT TRUE,
        status INTEGER NOT NULL DEFAULT 0,
        dpm_log_email VARCHAR,
        dpm_log_email_absender BOOLEAN NOT NULL DEFAULT FALSE,
        zustaendigkeitsbereiche_ausschluss VARCHAR,
        foto BYTEA,
        lnp_datei BYTEA,
        kontaktpersonen VARCHAR,
    );


INSERT INTO
    qdaba.mvu
SELECT
    *
FROM
    pg.qdaba.mvu;


CREATE TABLE
    qdaba.kanton (
        id_t_kanton INTEGER PRIMARY KEY,
        id_kanton VARCHAR(5) NOT NULL UNIQUE,
        kanton_kurz VARCHAR NOT NULL UNIQUE,
        kanton_lang VARCHAR(150) NOT NULL UNIQUE,
        geom VARCHAR,
    );


INSERT INTO
    qdaba.kanton
SELECT
    *
FROM
    pg.qdaba.kanton;


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


INSERT INTO
    qdaba.verkehrsmittel_kategorie
SELECT
    *
FROM
    pg.qdaba.verkehrsmittel_kategorie;


CREATE TABLE
    qdaba.import (
        id_t_import INTEGER PRIMARY KEY,
        id_anwendungsfall CHAR(4) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        beschreibg VARCHAR(100) NOT NULL,
        import_typ INTEGER NOT NULL,
        import_time TIMESTAMP NOT NULL,
        datpfad VARCHAR(500),
        datname VARCHAR(500),
        status INTEGER NOT NULL,
        daten_von DATE,
        daten_bis DATE,
        freischaltung INTEGER NOT NULL DEFAULT 0,
        vorberechnet BOOLEAN DEFAULT FALSE,
        quelle INTEGER DEFAULT '-1'::INTEGER,
        id_mvu VARCHAR(10) REFERENCES qdaba.mvu (id_mvu),
        status_pruefung INTEGER NOT NULL DEFAULT 0,
        bemerkung VARCHAR,
        anschluss_vorberechnet BOOLEAN DEFAULT FALSE,
        username VARCHAR(500),
        fpl_id_t_import INTEGER,
        gonr INTEGER,
    );


INSERT INTO
    qdaba.import
SELECT
    *
FROM
    pg.qdaba.import;


CREATE
OR REPLACE TABLE qdaba.bhf (
    id_t_bhf INTEGER PRIMARY KEY,
    id_anwendungsfall VARCHAR(4) NOT NULL REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    id_bhf VARCHAR NOT NULL,
    bahnhof VARCHAR NOT NULL,
    import_date DATE,
    qmt_messung NUMERIC(1),
    quelle INTEGER NOT NULL,
    typ INTEGER[],
    koord_x NUMERIC,
    koord_y NUMERIC,
    koord_z NUMERIC,
    abkuerzung VARCHAR(10),
    id_t_kanton INTEGER REFERENCES qdaba.kanton (id_t_kanton),
    gueltigkeit_start DATE NOT NULL,
    gueltigkeit_end DATE NOT NULL,
    go_nr INTEGER,
    gemeinde VARCHAR(40),
);


INSERT INTO
    qdaba.bhf (
        id_t_bhf,
        id_anwendungsfall,
        id_bhf,
        bahnhof,
        import_date,
        qmt_messung,
        quelle,
        typ,
        koord_x,
        koord_y,
        koord_z,
        abkuerzung,
        id_t_kanton,
        gueltigkeit_start,
        gueltigkeit_end,
        go_nr,
        gemeinde
    )
SELECT
    id_t_bhf,
    id_anwendungsfall,
    id_bhf,
    bahnhof,
    import_date,
    qmt_messung,
    quelle,
    typ,
    koord_x,
    koord_y,
    koord_z,
    abkuerzung,
    id_t_kanton,
    -- Extract gueltigkeit_start and gueltigkeit_end from the range string
    CAST(
        SUBSTRING(gueltigkeit, 2, POSITION(',' IN gueltigkeit) - 2) AS DATE
    ) AS gueltigkeit_start,
    CAST(
        SUBSTRING(
            gueltigkeit,
            POSITION(',' IN gueltigkeit) + 1,
            LENGTH(gueltigkeit) - POSITION(',' IN gueltigkeit) - 1
        ) AS DATE
    ) AS gueltigkeit_end,
    go_nr,
    gemeinde
FROM
    pg.qdaba.bhf;


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


INSERT INTO
    qdaba.linie (
        id_t_linie,
        id_anwendungsfall,
        id_linie,
        linie,
        linie_produkt,
        liniebemerkg,
        id_mvu,
        typ,
        gueltigkeit_start,
        gueltigkeit_end,
        linienlaenge,
        fahrtkm_projahr,
        personenkm_projahr,
        kurspaare_projahr_mofr,
        kurspaare_projahr_sa,
        kurspaare_projahr_so,
        id_mvu_gefahren,
        messart,
        produkt_detail,
        dpmtool_ignore,
        id_vmkat
    )
SELECT
    id_t_linie,
    id_anwendungsfall,
    id_linie,
    linie,
    linie_produkt,
    liniebemerkg,
    id_mvu,
    typ,
    -- Extract gueltigkeit_start and gueltigkeit_end from the range string
    CAST(
        SUBSTRING(gueltigkeit, 2, POSITION(',' IN gueltigkeit) - 2) AS DATE
    ) AS gueltigkeit_start,
    CASE
        WHEN LENGTH(gueltigkeit) > 13 THEN CAST(
            SUBSTRING(
                gueltigkeit,
                POSITION(',' IN gueltigkeit) + 1,
                LENGTH(gueltigkeit) - POSITION(',' IN gueltigkeit) - 1
            ) AS DATE
        )
        ELSE NULL
    END AS gueltigkeit_end,
    linienlaenge,
    fahrtkm_projahr,
    personenkm_projahr,
    kurspaare_projahr_mofr,
    kurspaare_projahr_sa,
    kurspaare_projahr_so,
    id_mvu_gefahren,
    messart,
    produkt_detail,
    dpmtool_ignore,
    id_vmkat
FROM
    pg.qdaba.linie;


-------------------------------- Still have to create and INSERT --------------------------------
CREATE TABLE
    qdaba.linie_lb (
        id_t_linie_lb INTEGER PRIMARY KEY,
        id_t_linie INTEGER NOT NULL REFERENCES qdaba.linie (id_t_linie),
        id_t_lb INTEGER NOT NULL REFERENCES qdaba.linienbuendel (id_t_lb),
        gueltigkeit_start DATE NOT NULL,
        gueltigkeit_end DATE
    );


INSERT INTO
    qdaba.linie_lb (
        id_t_linie_lb,
        id_t_linie,
        id_t_lb,
        gueltigkeit_start,
        gueltigkeit_end
    )
SELECT
    id_t_linie_lb,
    id_t_linie,
    id_t_lb,
    -- Extract gueltigkeit_start and gueltigkeit_end from the range string
    CAST(
        SUBSTRING(gueltigkeit, 2, POSITION(',' IN gueltigkeit) - 2) AS DATE
    ) AS gueltigkeit_start,
    CASE
        WHEN LENGTH(gueltigkeit) > 13 THEN CAST(
            SUBSTRING(
                gueltigkeit,
                POSITION(',' IN gueltigkeit) + 1,
                LENGTH(gueltigkeit) - POSITION(',' IN gueltigkeit) - 1
            ) AS DATE
        )
        ELSE NULL
    END AS gueltigkeit_end
FROM
    pg.qdaba.linie_lb;


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


INSERT INTO
    qdaba.puenkt_fahrt
SELECT
    *
FROM
    pg.qdaba.puenkt_fahrt;


CREATE TABLE
    qdaba.puenkt_kat (
        id_t_puenktkat INTEGER PRIMARY KEY,
        id_anwendungsfall VARCHAR(4) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        puenkt_kat INTEGER,
        move_type VARCHAR,
        UNIQUE (id_anwendungsfall, puenkt_kat)
    );


INSERT INTO
    qdaba.puenkt_kat
SELECT
    *
FROM
    pg.qdaba.puenkt_kat
CREATE TABLE
    qdaba.puenkt (
        id_t_puenkt BIGINT PRIMARY KEY,
        id_t_import INTEGER NOT NULL REFERENCES qdaba.import (id_t_import),
        id_anwendungsfall VARCHAR(4) NOT NULL REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        id_mvu VARCHAR NOT NULL REFERENCES qdaba.mvu (id_mvu),
        id_linie_tu VARCHAR(10) NOT NULL,
        id_fahrt VARCHAR(20) NOT NULL,
        betriebstag DATE NOT NULL,
        fahrt_halt_lauf INTEGER NOT NULL DEFAULT '-1'::INTEGER,
        id_messpunkt INTEGER NOT NULL REFERENCES qdaba.bhf (id_t_bhf),
        id_an_ab INTEGER NOT NULL,
        soll_bav TIMESTAMP,
        soll_tu TIMESTAMP NOT NULL,
        ist_tu TIMESTAMP NOT NULL,
        id_verspcode VARCHAR(10),
        insert_time TIMESTAMP,
        id_t_linie_bav INTEGER REFERENCES qdaba.linie (id_t_linie),
        qrelevant BOOLEAN NOT NULL DEFAULT FALSE,
        go_nr INTEGER NOT NULL DEFAULT '-1'::INTEGER,
        id_t_puenkt_fahrt INTEGER,
        manipulation_time TIMESTAMP,
        id_fahrt_tu VARCHAR(20),
        UNIQUE (
            id_anwendungsfall,
            id_mvu,
            go_nr,
            id_t_linie_bav,
            betriebstag,
            id_fahrt,
            fahrt_halt_lauf,
            id_messpunkt,
            id_an_ab
        )
    );


INSERT INTO
    qdaba.puenkt
SELECT
    *
FROM
    pg.qdaba.puenkt;