CREATE TABLE
    qdaba.linie (
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
    qdaba.linie_lb (
        id_t_linie_lb INTEGER PRIMARY KEY,
        id_t_linie INTEGER NOT NULL REFERENCES qdaba.linie (id_t_linie),
        id_t_lb INTEGER NOT NULL REFERENCES qdaba.linienbuendel (id_t_lb),
        gueltigkeit_start DATE NOT NULL,
        gueltigkeit_end DATE
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
    qdaba.puenkt_kat (
        id_t_puenktkat INTEGER PRIMARY KEY,
        id_anwendungsfall VARCHAR(4) REFERENCES qdaba.anwendungsfall (id_anwendungsfall),
        puenkt_kat INTEGER,
        move_type VARCHAR,
        UNIQUE (id_anwendungsfall, puenkt_kat)
    );


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