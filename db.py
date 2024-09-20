import psycopg2

# Connect to the PostgreSQL database
def connect_db():
    conn = psycopg2.connect(
        dbname="concerts_db", 
        user="postgres", 
        password="pass", 
        host="localhost"
    )
    return conn

# Concert Methods

# Get the Band for a specific concert
def get_band_for_concert(concert_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT bands.id, bands.name, bands.hometown
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        WHERE concerts.id = %s;
    """, (concert_id,))
    band = cur.fetchone()
    cur.close()
    conn.close()
    return band

# Get the Venue for a specific concert
def get_venue_for_concert(concert_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT venues.id, venues.title, venues.city
        FROM concerts
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.id = %s;
    """, (concert_id,))
    venue = cur.fetchone()
    cur.close()
    conn.close()
    return venue

# Venue Methods

# Get all concerts at a venue
def get_concerts_for_venue(venue_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT concerts.id, concerts.date, bands.name
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        WHERE concerts.venue_id = %s;
    """, (venue_id,))
    concerts = cur.fetchall()
    cur.close()
    conn.close()
    return concerts

# Get all bands that played at a venue
def get_bands_for_venue(venue_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT bands.id, bands.name
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        WHERE concerts.venue_id = %s;
    """, (venue_id,))
    bands = cur.fetchall()
    cur.close()
    conn.close()
    return bands

# Band Methods

# Get all concerts a band has played
def get_concerts_for_band(band_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT concerts.id, concerts.date, venues.title
        FROM concerts
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.band_id = %s;
    """, (band_id,))
    concerts = cur.fetchall()
    cur.close()
    conn.close()
    return concerts

# Get all venues a band has performed at
def get_venues_for_band(band_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT venues.id, venues.title, venues.city
        FROM concerts
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.band_id = %s;
    """, (band_id,))
    venues = cur.fetchall()
    cur.close()
    conn.close()
    return venues

# Aggregate and Relationship Methods

# Check if concert is a hometown show for the band
def is_hometown_show(concert_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT bands.hometown, venues.city
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.id = %s;
    """, (concert_id,))
    band_hometown, venue_city = cur.fetchone()
    cur.close()
    conn.close()
    return band_hometown == venue_city

# Get concert introduction for a band
def concert_introduction(concert_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT bands.name, bands.hometown, venues.city
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.id = %s;
    """, (concert_id,))
    band_name, band_hometown, venue_city = cur.fetchone()
    cur.close()
    conn.close()
    return f"Hello {venue_city}!!!!! We are {band_name} and we're from {band_hometown}"

# Band plays in a new venue on a given date
def band_play_in_venue(band_id, venue_id, date):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO concerts (band_id, venue_id, date) VALUES (%s, %s, %s);
    """, (band_id, venue_id, date))
    conn.commit()
    cur.close()
    conn.close()

# Get all concert introductions for a band
def get_all_introductions(band_id):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT concerts.id
        FROM concerts
        WHERE concerts.band_id = %s;
    """, (band_id,))
    concerts = cur.fetchall()
    cur.close()
    conn.close()

    introductions = []
    for concert in concerts:
        introductions.append(concert_introduction(concert[0]))
    return introductions
