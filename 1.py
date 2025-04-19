import psycopg2
conn = psycopg2.connect(
    host='localhost',
    database='pp2',
    user='pp2',
    password='pp2pass'
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS phonebook (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL
);
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE insert_or_update_user(p_name VARCHAR, p_phone VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM phonebook WHERE name = p_name) THEN
        UPDATE phonebook SET phone = p_phone WHERE name = p_name;
    ELSE
        INSERT INTO phonebook(name, phone) VALUES (p_name, p_phone);
    END IF;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE insert_multiple_users(
    IN p_names TEXT[],
    IN p_phones TEXT[],
    OUT invalid_entries TEXT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT := 1;
    name TEXT;
    phone TEXT;
BEGIN
    invalid_entries := '{}';
    WHILE i <= array_length(p_names, 1) LOOP
        name := p_names[i];
        phone := p_phones[i];
        IF phone ~ '^[0-9]+$' THEN
            CALL insert_or_update_user(name, phone);
        ELSE
            invalid_entries := array_append(invalid_entries, name || ':' || phone);
        END IF;
        i := i + 1;
    END LOOP;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE FUNCTION get_users_with_pagination(p_limit INT, p_offset INT)
RETURNS TABLE(id INT, name TEXT, phone TEXT)
LANGUAGE sql
AS $$
    SELECT id, name, phone
    FROM phonebook
    ORDER BY id
    LIMIT p_limit OFFSET p_offset;
$$;
""")

conn.commit()

def call_insert_or_update(cur, name, phone):
    cur.execute("CALL insert_or_update_user(%s, %s)", (name, phone))

def call_insert_many(cur, names, phones):
    cur.callproc("insert_multiple_users", (names, phones))
    return cur.fetchone()[0]

def query_paginated(cur, limit, offset):
    cur.execute("SELECT * FROM get_users_with_pagination(%s, %s)", (limit, offset))
    return cur.fetchall()

call_insert_or_update(cur, "Alice", "123456")
call_insert_or_update(cur, "Bob", "654321")

users = query_paginated(cur, 10, 0)
print("📋 Users:")
for user in users:
    print(user)

cur.close()
conn.close()
