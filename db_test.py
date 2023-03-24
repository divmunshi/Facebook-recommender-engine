import psycopg2

# Connect to an existing database
conn = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

# Open a cursor to perform database operations
cur = conn.cursor()

cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur.fetchall():
    print(table)



# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",  (100, "abc'def"))

# cur.execute("SELECT * FROM test;")
# for row in cur:
#   print(row)

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()