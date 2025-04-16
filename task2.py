import psycopg2
import csv
import re


def connect_db():
    return psycopg2.connect(dbname="phonebook", user="postgres", password="newpassword", host="localhost", port="5433")


def create_phonebook_table():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phonebook (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50),
                    phone VARCHAR(15) UNIQUE
                );
            """)
            conn.commit()


def insert_from_csv(filepath):
    with connect_db() as conn:
        cur = conn.cursor()
        with open(filepath, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file, delimiter=';')
            next(reader, None)
            for row in reader:
                cur.execute(
                    "INSERT INTO phonebook (first_name, phone) VALUES (%s, %s) ON CONFLICT (phone) DO NOTHING;",
                    row
                )
        conn.commit()


def insert_from_console():
    first_name = input("First name: ")
    phone = input("Phone: ")
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO phonebook (first_name, phone) VALUES (%s, %s) ON CONFLICT (phone) DO NOTHING;", (first_name, phone))
            conn.commit()


def update_phonebook_entry(identifier, new_first_name=None, new_phone=None):
    with connect_db() as conn:
        with conn.cursor() as cur:
            if new_first_name:
                cur.execute("UPDATE phonebook SET first_name = %s WHERE phone = %s;", (new_first_name, identifier))
            if new_phone:
                cur.execute("UPDATE phonebook SET phone = %s WHERE phone = %s;", (new_phone, identifier))
            conn.commit()


def query_phonebook(filter_by=None, value=None):
    with connect_db() as conn:
        with conn.cursor() as cur:
            if filter_by == 'first_name':
                cur.execute("SELECT * FROM phonebook WHERE first_name = %s;", (value,))
            elif filter_by == 'phone':
                cur.execute("SELECT * FROM phonebook WHERE phone = %s;", (value,))
            else:
                cur.execute("SELECT * FROM phonebook;")
            rows = cur.fetchall()
            for row in rows:
                print(row)


def delete_from_phonebook(identifier):
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM phonebook WHERE first_name = %s OR phone = %s;", (identifier, identifier))
            conn.commit()


# ==================== 🔽 NEW TASK FUNCTIONS START HERE 🔽 ====================

def search_by_pattern(pattern):
    with connect_db() as conn:
        with conn.cursor() as cur:
            like_pattern = f"%{pattern}%"
            cur.execute("""
                SELECT * FROM phonebook
                WHERE first_name ILIKE %s OR phone ILIKE %s;
            """, (like_pattern, like_pattern))
            results = cur.fetchall()
            for row in results:
                print(row)


def insert_or_update_user(name, phone):
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM phonebook WHERE first_name = %s;", (name,))
            existing = cur.fetchone()
            if existing:
                cur.execute("UPDATE phonebook SET phone = %s WHERE first_name = %s;", (phone, name))
                print(f"Updated phone for {name}")
            else:
                cur.execute("INSERT INTO phonebook (first_name, phone) VALUES (%s, %s);", (name, phone))
                print(f"Inserted new user {name}")
            conn.commit()


def insert_many_users(name_phone_list):
    incorrect_entries = []
    valid_pattern = r'^\+?[0-9\- ()]{7,15}$'  # updated to be a bit more flexible

    with connect_db() as conn:
        with conn.cursor() as cur:
            for name, phone in name_phone_list:
                if not re.match(valid_pattern, phone):
                    incorrect_entries.append((name, phone))
                    continue
                try:
                    # Check if user exists by name
                    cur.execute("SELECT * FROM phonebook WHERE first_name = %s;", (name,))
                    existing = cur.fetchone()
                    if existing:
                        cur.execute("UPDATE phonebook SET phone = %s WHERE first_name = %s;", (phone, name))
                    else:
                        cur.execute("INSERT INTO phonebook (first_name, phone) VALUES (%s, %s);", (name, phone))
                except Exception as e:
                    incorrect_entries.append((name, phone))
            conn.commit()
    return incorrect_entries



def query_paginated(limit, offset):
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM phonebook ORDER BY id LIMIT %s OFFSET %s;", (limit, offset))
            rows = cur.fetchall()
            for row in rows:
                print(row)


def delete_by_username_or_phone(identifier):
    delete_from_phonebook(identifier)


# ==================== 🔼 NEW TASK FUNCTIONS END HERE 🔼 ====================


if __name__ == "__main__":
    create_phonebook_table()
    while True:
        print("\nPhoneBook Menu")
        print("1. Insert from CSV")
        print("2. Insert from Console")
        print("3. Update Entry")
        print("4. Query PhoneBook")
        print("5. Delete Entry")
        print("6. Search by Pattern")
        print("7. Insert or Update User")
        print("8. Insert Many Users")
        print("9. Paginated Query")
        print("10. Exit")
        choice = input("Choose an option: ")

        if choice == '1':
            insert_from_csv("./data.csv")
        elif choice == '2':
            insert_from_console()
        elif choice == '3':
            identifier = input("Enter the phone of the user to update: ")
            new_first_name = input("Enter new first name (or press Enter to skip): ")
            new_phone = input("Enter new phone (or press Enter to skip): ")
            update_phonebook_entry(identifier, new_first_name or None, new_phone or None)
        elif choice == '4':
            filter_by = input("Filter by (first_name/phone/none): ")
            value = None if filter_by == 'none' else input(f"Enter value for {filter_by}: ")
            query_phonebook(filter_by if filter_by != 'none' else None, value)
        elif choice == '5':
            identifier = input("Enter first name or phone to delete: ")
            delete_from_phonebook(identifier)
        elif choice == '6':
            pattern = input("Enter search pattern (name or phone): ")
            search_by_pattern(pattern)
        elif choice == '7':
            name = input("Enter name: ")
            phone = input("Enter phone: ")
            insert_or_update_user(name, phone)
        elif choice == '8':
            n = int(input("How many users to insert? "))
            user_list = []
            for _ in range(n):
                name = input("Name: ")
                phone = input("Phone: ")
                user_list.append((name, phone))
            incorrect = insert_many_users(user_list)
            if incorrect:
                print("Incorrect entries:")
                for entry in incorrect:
                    print(entry)
        elif choice == '9':
            limit = int(input("Enter limit: "))
            offset = int(input("Enter offset: "))
            query_paginated(limit, offset)
        elif choice == '10':
            break
        else:
            print("Invalid choice")
