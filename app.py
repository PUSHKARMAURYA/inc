from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Configuration for the source databases
configs = {
    "config_v3": {
        "host": "new-cars-db-7726.postgresql.b.osc-fr1.scalingo-dbs.com",
        "port": 34422,
        "database": "new_cars_db_7726",
        "user": "new_cars_db_7726",
        "password": "ALTkGiybw9MJI5usR3jJnLWBOYQgol1uOMqwTYowNP4opRrrQiuYHnnxUoeoYVNM",
        "sslmode": "prefer"
    },
    "config_v2": {
        "host": "old-cars-db-1274.postgresql.b.osc-fr1.scalingo-dbs.com",
        "port": 33401,
        "database": "old_cars_db_1274",
        "user": "old_cars_db_1274",
        "password": "zA7ewf3xaivCuy_VED_EKjwfmlTuoNxzoBlldY7wMBaTcyrFaQAjpeH557BCALvf",
        "sslmode": "prefer"
    },
    "config_car_details": {
        "host": "dbforcars-4139.postgresql.b.osc-fr1.scalingo-dbs.com",
        "port": 31395,
        "database": "dbforcars_4139",
        "user": "dbforcars_4139",
        "password": "J70G_jss1jVlpNJHTh1JWfZChuJcZ_rWtQIDlneFvQrmTnkfG1ehz82JQ1rFAtZV",
        "sslmode": "prefer"
    }
}

# Configuration for the combined database
combined_db_config = {
    "host": "b1qtx5ubvtqnepc0w8zy-postgresql.services.clever-cloud.com",
    "port": 50013,
    "database": "b1qtx5ubvtqnepc0w8zy",
    "user": "ukmtoyqfsdon7mpw9qtb",
    "password": "InJW4q2DtaiSlEvnAygcXNpZIwseA0"
}

def fetch_and_insert_data():
    try:
        # Loop through each source database
        for config_name, config in configs.items():
            try:
                source_conn = psycopg2.connect(**config)
                combined_conn = psycopg2.connect(**combined_db_config)

                with source_conn, combined_conn:
                    with source_conn.cursor() as source_cursor, combined_conn.cursor() as combined_cursor:
                        print(f"Fetching incremental data from {config_name}...")

                        # Get the last update time for this source
                        combined_cursor.execute("""
                            SELECT MAX(last_updated)
                            FROM combined_cars
                            WHERE source_db = %s;
                        """, (config_name,))
                        last_updated_time = combined_cursor.fetchone()[0]

                        # Default to a very old date if no records are present
                        if not last_updated_time:
                            last_updated_time = '1970-01-01 00:00:00'

                        # Fetch only records updated after the last update
                        fetch_query = ""
                        if config_name == "config_v2":
                            fetch_query = f"""
                                SELECT car_model, '' AS brand, year AS year_of_manufacture, price, kms_driven AS mileage,
                                       'old' AS condition, 'config_v2' AS source_db, NOW() AS last_updated
                                FROM car_details_v2
                                WHERE last_updated > '{last_updated_time}';
                            """
                        elif config_name == "config_v3":
                            fetch_query = f"""
                                SELECT name AS car_model, '' AS brand, year AS year_of_manufacture, selling_price AS price,
                                       km_driven AS mileage, 'new' AS condition, 'config_v3' AS source_db, NOW() AS last_updated
                                FROM car_details_v3
                                WHERE last_updated > '{last_updated_time}';
                            """
                        elif config_name == "config_car_details":
                            fetch_query = f"""
                                SELECT model AS car_model, make AS brand, year AS year_of_manufacture, price,
                                       kilometer AS mileage, 'old' AS condition, 'config_car_details' AS source_db, NOW() AS last_updated
                                FROM car_details
                                WHERE last_updated > '{last_updated_time}';
                            """
                        source_cursor.execute(fetch_query)
                        rows = source_cursor.fetchall()

                        # Insert or update rows in the combined database
                        insert_query = """
                            INSERT INTO combined_cars (car_model, brand, year_of_manufacture, price, mileage, condition, source_db, last_updated)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (car_model, source_db) DO UPDATE
                            SET brand = EXCLUDED.brand,
                                year_of_manufacture = EXCLUDED.year_of_manufacture,
                                price = EXCLUDED.price,
                                mileage = EXCLUDED.mileage,
                                condition = EXCLUDED.condition,
                                last_updated = EXCLUDED.last_updated;
                        """
                        combined_cursor.executemany(insert_query, rows)
                        combined_conn.commit()

                        print(f"Incremental data inserted/updated successfully from {config_name}!")
            except Exception as e:
                print(f"Error fetching incremental data from {config_name}: {e}")
    except Exception as e:
        print(f"Error: {e}")

@app.route('/run-job', methods=['GET'])
def run_job():
    try:
        fetch_and_insert_data()
        return jsonify({"message": "Job executed successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
