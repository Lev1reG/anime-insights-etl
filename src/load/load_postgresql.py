import psycopg2
from psycopg2 import sql
import pandas as pd

class LoadPsql:
    @staticmethod
    def load_to_postgresql(dataframe, table_name, db_config):
        
        conn = None
        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                host=db_config['host'],
                dbname=db_config['dbname'],
                user=db_config['user'],
                password=db_config['password'],
                port=db_config.get('port', 5432),
                sslmode='require'  # Ensure SSL connection
            )
            cursor = conn.cursor()

            # Insert data into table
            for _, row in dataframe.iterrows():
                insert_query = sql.SQL(f"""
                    INSERT INTO {table_name} ({', '.join(dataframe.columns)})
                    VALUES ({', '.join(['%s'] * len(dataframe.columns))});
                """)
                cursor.execute(insert_query, tuple(row))
            
            conn.commit()
            print(f"Data successfully loaded into {table_name}!")
        
        except Exception as e:
            print(f"Error loading data: {e}")
        
        finally:
            if conn:
                cursor.close()
                conn.close()



