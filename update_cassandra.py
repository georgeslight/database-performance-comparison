from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra host IP
session = cluster.connect('qdaba')  # Replace with your keyspace

# Select data where id_an_ab is either 1 or 2
rows = session.execute("SELECT id_mvu, id_anwendungsfall, go_nr, id_t_linie_bav, betriebstag, id_fahrt, fahrt_halt_lauf, id_messpunkt, id_an_ab FROM puenkt WHERE id_an_ab IN (1, 2) ALLOW FILTERING")

# Update rows based on id_an_ab value
for row in rows:
    move_type = 'ARRIVAL' if row.id_an_ab == 1 else 'DEPARTURE'
    session.execute("""
        UPDATE puenkt
        SET move_type = %s
        WHERE id_mvu = %s AND id_anwendungsfall = %s AND go_nr = %s AND id_t_linie_bav = %s 
        AND betriebstag = %s AND id_fahrt = %s AND fahrt_halt_lauf = %s AND id_messpunkt = %s AND id_an_ab = %s;
    """, (move_type, row.id_mvu, row.id_anwendungsfall, row.go_nr, row.id_t_linie_bav, row.betriebstag, row.id_fahrt, row.fahrt_halt_lauf, row.id_messpunkt, row.id_an_ab))

print("Rows updated successfully.")
