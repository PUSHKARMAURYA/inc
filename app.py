from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)


import psycopg2
from psycopg2 import sql

# Configuration for source databases
configs = {
    "config_v2": {
        "host": "old-cars-db-1274.postgresql.b.osc-fr1.scalingo-dbs.com",
        "port": 33401,
        "database": "old_cars_db_1274",
        "user": "old_cars_db_1274",
        "password": "zA7ewf3xaivCuy_VED_EKjwfmlTuoNxzoBlldY7wMBaTcyrFaQAjpeH557BCALvf",
        "sslmode": "prefer"
    },
    "config_v3": {
        "host": "new-cars-db-7726.postgresql.b.osc-fr1.scalingo-dbs.com",
        "port": 34422,
        "database": "new_cars_db_7726",
        "user": "new_cars_db_7726",
        "password": "ALTkGiybw9MJI5usR3jJnLWBOYQgol1uOMqwTYowNP4opRrrQiuYHnnxUoeoYVNM",
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

# Combined database configuration
combined_db_config = {
    "host": "b1qtx5ubvtqnepc0w8zy-postgresql.services.clever-cloud.com",
    "port": 50013,
    "database": "b1qtx5ubvtqnepc0w8zy",
    "user": "ukmtoyqfsdon7mpw9qtb",
    "password": "InJW4q2DtaiSlEvnAygcXNpZIwseA0"
}

def etl_function(rows, config_name):
    """ETL Function to process and load data into combined_cars2."""
    transformed_rows = []
    for row in rows:
        car_model = row[0].strip() if isinstance(row[0], str) else row[0]
        brand = row[1].strip() if isinstance(row[1], str) else row[1]
        year_of_manufacture = row[2]
        price = row[3]
        mileage = row[4]
        condition = row[5]
        source_db = config_name
        last_updated = row[6]

        # Handle decimal price transformation
        if isinstance(price, float):
            price *= 100000

        # Append the transformed row
        transformed_rows.append((car_model, brand, year_of_manufacture, price, mileage, condition, source_db, last_updated))

    # Load data into the combined database
    try:
        combined_conn = psycopg2.connect(**combined_db_config)
        with combined_conn:
            with combined_conn.cursor() as combined_cursor:
                insert_query = """
                    INSERT INTO combined_cars2 (car_model, brand, year_of_manufacture, price, mileage, condition, source_db, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (car_model, source_db) DO UPDATE
                    SET brand = EXCLUDED.brand,
                        year_of_manufacture = EXCLUDED.year_of_manufacture,
                        price = EXCLUDED.price,
                        mileage = EXCLUDED.mileage,
                        condition = EXCLUDED.condition,
                        last_updated = EXCLUDED.last_updated;
                """
                combined_cursor.executemany(insert_query, transformed_rows)
                combined_conn.commit()
                print(f"{len(transformed_rows)} records inserted or updated for {config_name}.")
    except Exception as e:
        print(f"Error during ETL process for {config_name}: {e}")

def fetch_and_sync_data():
    """Fetch new data and trigger the ETL process."""
    try:
        # Connect to the combined database
        combined_conn = psycopg2.connect(**combined_db_config)
        with combined_conn:
            with combined_conn.cursor() as combined_cursor:
                # Loop through each source database
                for config_name, config in configs.items():
                    try:
                        source_conn = psycopg2.connect(**config)
                        with source_conn:
                            with source_conn.cursor() as source_cursor:
                                print(f"Fetching data from {config_name}...")

                                # Fetch the last update time for this source
                                combined_cursor.execute("""
                                    SELECT MAX(last_updated)
                                    FROM combined_cars2
                                    WHERE source_db = %s;
                                """, (config_name,))
                                last_updated_time = combined_cursor.fetchone()[0] or '1970-01-01 00:00:00'

                                # Fetch new or updated records from the source database
                                if config_name == "config_v2":
                                    fetch_query = """
                                        SELECT car_model, '' AS brand, year AS year_of_manufacture, price, kms_driven AS mileage,
                                               'old' AS condition, NOW() AS last_updated
                                        FROM car_details_v2
                                        WHERE last_updated > %s;
                                    """
                                elif config_name == "config_v3":
                                    fetch_query = """
                                        SELECT name AS car_model, '' AS brand, year AS year_of_manufacture, selling_price AS price,
                                               km_driven AS mileage, 'new' AS condition, NOW() AS last_updated
                                        FROM car_details_v3
                                        WHERE last_updated > %s;
                                    """
                                elif config_name == "config_car_details":
                                    fetch_query = """
                                        SELECT model AS car_model, make AS brand, year AS year_of_manufacture, price,
                                               kilometer AS mileage, 'old' AS condition, NOW() AS last_updated
                                        FROM car_details
                                        WHERE last_updated > %s;
                                    """

                                source_cursor.execute(fetch_query, (last_updated_time,))
                                rows = source_cursor.fetchall()

                                # Call ETL function
                                etl_function(rows, config_name)

                                # Fetch and compare current records for deletions
                                source_cursor.execute("""
                                    SELECT car_model
                                    FROM car_details_v2
                                """ if config_name == "config_v2" else """
                                    SELECT name AS car_model
                                    FROM car_details_v3
                                """ if config_name == "config_v3" else """
                                    SELECT model AS car_model
                                    FROM car_details
                                """)
                                source_records = {row[0] for row in source_cursor.fetchall()}

                                combined_cursor.execute("""
                                    SELECT car_model
                                    FROM combined_cars2
                                    WHERE source_db = %s;
                                """, (config_name,))
                                combined_records = {row[0] for row in combined_cursor.fetchall()}

                                # Identify and delete outdated records
                                outdated_records = combined_records - source_records
                                if outdated_records:
                                    delete_query = """
                                        DELETE FROM combined_cars2
                                        WHERE car_model = %s AND source_db = %s;
                                    """
                                    combined_cursor.executemany(delete_query, [(record, config_name) for record in outdated_records])
                                    combined_conn.commit()
                                    print(f"Deleted {len(outdated_records)} outdated records for {config_name}.")
                    except Exception as e:
                        print(f"Error fetching data from {config_name}: {e}")
    except Exception as e:
        print(f"Error: {e}")





@app.route('/run-job', methods=['GET'])
def run_job():
    try:
        fetch_and_sync_data()
        return jsonify({"message": "Job executed successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
