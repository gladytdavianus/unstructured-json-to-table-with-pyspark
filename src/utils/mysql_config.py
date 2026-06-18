import pymysql
from dotenv import load_dotenv
from pathlib import Path
import os
import subprocess

load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

def get_mysql_connection(database=None, host=None, user=None, password=None):
    host = host or os.getenv("MYSQL_HOST", "localhost")
    user = user or os.getenv("MYSQL_USER", "root")
    password = password or os.getenv("MYSQL_PASSWORD", "")
    database = database or os.getenv("MYSQL_DATABASE", "yelp_db")
    
    return pymysql.connect(host=host, user=user, password=password, database=database)

def close_mysql_connection(conn):
    conn.close()

def setup_mysql_database(sql_file_path="sql/schema_data.sql"):
    print(f"Setting up MySQL database from: {sql_file_path}")
    password = os.getenv("MYSQL_PASSWORD", "")
    
    subprocess.run([
        "mysql", "-u", os.getenv("MYSQL_USER", "root"),
        f"-p{password}",
        "-e", f"source {sql_file_path}"
    ], check=True)
    
    print("MySQL database setup completed!")
