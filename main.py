import awswrangler as wr
import configparser
import logging
import os
import pandas as pd
import redshift_connector
from typing import Tuple

# Verificar si la carpeta 'logs' existe, si no, creala
if not os.path.exists("logs"):
    os.makedirs("logs")

# Configuracion de logger
logging.basicConfig(
    filename="logs/redshift.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def read_redshift_credentials(config_file: str) -> Tuple[str, str, str, str, str]:
    """
    Read database credentials from a configuration file.
    Args:
        config_file: The path to the configuration file.
    Returns:
        A tuple containing hostname, database name, username, and password.
    """
    try:
        parser = configparser.ConfigParser()
        parser.read(config_file)
        host = parser.get("redshift_config", "hostname")
        port = parser.get("redshift_config", "port")
        user = parser.get("redshift_config", "username")
        password = parser.get("redshift_config", "password")
        database = parser.get("redshift_config", "database")
        return host, port, user, password, database
    except Exception as e:
        logger.error(f"Failed to read redshift credentials: {e}")
        raise


def read_query(query_file: str) -> str:
    """
    Read SQL query from a file.
    Args:
        query_file: The path to the query file.
    Returns:
        The SQL query as a string.
    """
    try:
        with open(query_file, "r") as file:
            query = file.read()
        logger.info(f"Query read successfully from {query_file}.")
        return query
    except Exception as e:
        logger.error(f"Failed to read query: {e}")
        raise


def execute_query(query: str) -> pd.DataFrame:
    """
    Execute a SQL query on a database connection.
    Args:
        query: The SQL query to execute.
    Returns:
        A pandas DataFrame containing the result of the query.
    """
    try:
        host, port, user, password, database = read_redshift_credentials(
            "pipeline.conf"
        )

        conn = redshift_connector.connect(
            host=host, port=int(port), database=database, user=user, password=password
        )

        data = wr.redshift.read_sql_query(sql=query, con=conn)


        logger.info("Query executed successfully.")
        return data
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise


def write_to_csv(data: pd.DataFrame, csv_file: str) -> None:
    """
    Write a pandas DataFrame to a CSV file.
    Args:
        data: The pandas DataFrame to write.
        csv_file: The path to the CSV file.
    Returns: None
    """
    try:
        data.to_csv(csv_file, index=False)
        logger.info(f"Data written successfully to {csv_file}.")
    except Exception as e:
        logger.error(f"Failed to write data to CSV: {e}")
        raise


def main():
    """
    Main function to read query, execute query, write result to CSV.
    Returns: None
    """
    try:
        # Lee la consulta SQL desde un archivo
        query = read_query("query.sql")

        # Ejecuta la consulta
        data = execute_query(query)

        # Escribe los resultados en un archivo CSV
        write_to_csv(data, "sales_data.csv")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
