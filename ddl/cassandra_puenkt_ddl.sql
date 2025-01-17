CREATE TABLE IF NOT EXISTS
    qdaba.puenkt (
        id_t_puenkt BIGINT,
        id_t_import INT,
        id_anwendungsfall TEXT,
        id_mvu TEXT,
        id_linie_tu TEXT,
        id_fahrt TEXT,
        betriebstag date,
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
        id_t_lb INTEGER, -- Primary Key qdaba.linienbuendel
        direction_id SMALLINT, -- qdaba.puenkt_fahrt
        gueltigkeit_start, -- qdaba.linie_lb
        PRIMARY KEY ((id_mvu))
    );