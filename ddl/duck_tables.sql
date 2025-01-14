ALTER TABLE qdaba.anwendungsfall RENAME TO anwendungsfall_old;
ALTER TABLE qdaba.verkehrstraeger RENAME TO verkehrstraeger_old;
ALTER TABLE qdaba.mvu RENAME TO mvu_old;
ALTER TABLE qdaba.kanton RENAME TO kanton_old;
ALTER TABLE qdaba.verkehrsmittel_kategorie RENAME TO verkehrsmittel_kategorie_old;
ALTER TABLE qdaba.import RENAME TO import_old;
ALTER TABLE qdaba.bhf RENAME TO bhf_old;
ALTER TABLE qdaba.linie RENAME TO linie_old;
ALTER TABLE qdaba.puenkt RENAME TO puenkt_old;

CREATE TABLE qdaba.anwendungsfall (
    id_anwendungsfall VARCHAR PRIMARY KEY,
    anwendungsfalllang VARCHAR NOT NULL UNIQUE,
    gueltig_ab DATE NOT NULL,
    gueltig_bis DATE NOT NULL,
    typen VARCHAR NOT NULL,
    messart INTEGER,
    anwendungsfall_metaname VARCHAR UNIQUE,
    show_puenkt_einzel BOOLEAN,
    id_mvu VARCHAR,
    ranking_wichtung_obj NUMERIC,
    ranking_wichtung_subj NUMERIC,
    color VARCHAR,
    afz BOOLEAN
);

CREATE TABLE qdaba.verkehrstraeger (
    id_t_verkehrstraeger INTEGER PRIMARY KEY,
    id_verkehrstraeger INTEGER NOT NULL UNIQUE,
    vt_sprach_key_kurz VARCHAR,
    vt_sprach_key_lang VARCHAR
);

CREATE TABLE qdaba.mvu (
    id_mvu VARCHAR PRIMARY KEY,
    mvulang VARCHAR NOT NULL UNIQUE,
    webservice_pin VARCHAR UNIQUE,
    verkehrsmitteltyp INTEGER[],
    mss_web BOOLEAN NOT NULL,
    mss_mda BOOLEAN NOT NULL,
    dpm_web BOOLEAN NOT NULL,
    status INTEGER NOT NULL,
    dpm_log_email VARCHAR,
    dpm_log_email_absender BOOLEAN NOT NULL,
    zustaendigkeitsbereiche_ausschluss VARCHAR,
    foto BLOB,
    lnp_datei BLOB,
    kontaktpersonen VARCHAR,
);

CREATE TABLE qdaba.kanton (
    id_t_kanton INTEGER PRIMARY KEY,
    id_kanton   VARCHAR NOT NULL UNIQUE,
    kanton_kurz VARCHAR NOT NULL UNIQUE,
    kanton_lang VARCHAR NOT NULL UNIQUE,
    geom VARCHAR,
);

CREATE TABLE qdaba.verkehrsmittel_kategorie (
    id_anwendungsfall VARCHAR,
    id_vmkat INTEGER,
    vmkat_kurz VARCHAR NOT NULL,
    vmkat_lang VARCHAR NOT NULL,
    id_verkehrstraeger INTEGER NOT NULL,
    vmkat_kurz_fr VARCHAR,
    vmkat_lang_fr VARCHAR,
    vmkat_kurz_it VARCHAR,
    vmkat_lang_it VARCHAR,
    lang_key VARCHAR NOT NULL,
    FOREIGN KEY (id_anwendungsfall) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    FOREIGN KEY (id_verkehrstraeger) REFERENCES qdaba.verkehrstraeger (id_verkehrstraeger),
    PRIMARY KEY (id_anwendungsfall, id_vmkat)
);

CREATE TABLE qdaba.import (
    id_t_import INTEGER PRIMARY KEY,
    id_anwendungsfall VARCHAR,
    beschreibg VARCHAR NOT NULL,
    import_typ INTEGER NOT NULL,
    import_time TIMESTAMP NOT NULL,
    datpfad VARCHAR,
    datname VARCHAR,
    status INTEGER NOT NULL,
    daten_von DATE,
    daten_bis DATE,
    freischaltung INTEGER NOT NULL,
    vorberechnet BOOLEAN,
    quelle INTEGER,
    id_mvu VARCHAR,
    status_pruefung INTEGER NOT NULL,
    bemerkung VARCHAR,
    anschluss_vorberechnet BOOLEAN,
    username VARCHAR,
    fpl_id_t_import INTEGER,
    gonr INTEGER,
    FOREIGN KEY (id_anwendungsfall) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    FOREIGN KEY (id_mvu) REFERENCES qdaba.mvu (id_mvu),
);

CREATE TABLE qdaba.bhf (
    id_t_bhf INTEGER PRIMARY KEY,
    id_anwendungsfall VARCHAR NOT NULL,
    id_bhf VARCHAR NOT NULL,
    bahnhof VARCHAR NOT NULL,
    import_date DATE,
    qmt_messung DECIMAL(2,0),
    quelle INTEGER NOT NULL,
    typ INTEGER[],
    koord_x DOUBLE,
    koord_y DOUBLE,
    koord_z DOUBLE,
    abkuerzung VARCHAR,
    id_t_kanton INTEGER,
    gueltigkeit VARCHAR NOT NULL,
    go_nr INTEGER,
    gemeinde VARCHAR,
    FOREIGN KEY (id_anwendungsfall) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    FOREIGN KEY (id_t_kanton) REFERENCES qdaba.kanton (id_t_kanton)
);

CREATE TABLE qdaba.linie (
    id_t_linie INTEGER PRIMARY KEY,
    id_anwendungsfall VARCHAR NOT NULL,
    id_linie VARCHAR NOT NULL,
    linie VARCHAR NOT NULL,
    linie_produkt INTEGER NOT NULL,
    liniebemerkg VARCHAR,
    id_mvu VARCHAR NOT NULL,
    typ INTEGER NOT NULL,
    gueltigkeit VARCHAR NOT NULL,
    linienlaenge DECIMAL(7,3),
    fahrtkm_projahr DECIMAL(10,3),
    personenkm_projahr DECIMAL(15,3),
    kurspaare_projahr_mofr VARCHAR,
    kurspaare_projahr_sa VARCHAR,
    kurspaare_projahr_so VARCHAR,
    id_mvu_gefahren VARCHAR NOT NULL,
    messart INTEGER,
    produkt_detail INTEGER,
    dpmtool_ignore BOOLEAN NOT NULL,
    id_vmkat INTEGER NOT NULL,
    FOREIGN KEY (id_anwendungsfall) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    FOREIGN KEY (id_mvu) REFERENCES qdaba.mvu (id_mvu),
    FOREIGN KEY (id_anwendungsfall, id_vmkat) REFERENCES qdaba.verkehrsmittel_kategorie (id_anwendungsfall, id_vmkat)
);

CREATE table qdaba.puenkt (
    id_t_puenkt BIGINT PRIMARY KEY,
    id_t_import INTEGER NOT NULL,
    id_anwendungsfall VARCHAR NOT NULL UNIQUE,
    id_mvu VARCHAR NOT NULL UNIQUE,
    id_linie_tu VARCHAR NOT NULL,
    id_fahrt VARCHAR NOT NULL UNIQUE,
    betriebstag DATE NOT NULL UNIQUE,
    fahrt_halt_lauf INTEGER NOT NULL UNIQUE,
    id_messpunkt INTEGER NOT NULL UNIQUE,
    id_an_ab INTEGER NOT NULL UNIQUE,
    soll_bav TIMESTAMP,
    soll_tu TIMESTAMP NOT NULL,
    ist_tu TIMESTAMP NOT NULL,
    id_verspcode VARCHAR,
    insert_time TIMESTAMP,
    id_t_linie_bav INTEGER UNIQUE,
    qrelevant BOOLEAN NOT NULL,
    go_nr INTEGER NOT NULL UNIQUE,
    id_t_puenkt_fahrt INTEGER,
    manipulation_time TIMESTAMP,
    id_fahrt_tu VARCHAR,
    last_update DATE,
    FOREIGN KEY (id_t_import) REFERENCES qdaba.import (id_t_import),
    FOREIGN KEY (id_anwendungsfall) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
    FOREIGN KEY (id_mvu) REFERENCES qdaba.mvu (id_mvu),
    FOREIGN KEY (id_messpunkt) REFERENCES qdaba.bhf (id_t_bhf),
    FOREIGN KEY (id_t_linie_bav) REFERENCES qdaba.linie (id_t_linie),
);


INSERT INTO qdaba.anwendungsfall FROM qdaba.anwendungsfall_old;
INSERT INTO qdaba.verkehrstraeger FROM qdaba.verkehrstraeger_old;
INSERT INTO qdaba.mvu FROM qdaba.mvu_old;
INSERT INTO qdaba.kanton FROM qdaba.kanton_old;
INSERT INTO qdaba.verkehrsmittel_kategorie FROM qdaba.verkehrsmittel_kategorie_old;
INSERT INTO qdaba.import FROM qdaba.import_old;
INSERT INTO qdaba.bhf FROM qdaba.bhf_old;
INSERT INTO qdaba.linie FROM qdaba.linie_old;
INSERT INTO qdaba.puenkt FROM qdaba.puenkt_old;