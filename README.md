# Ejercicio 2

Supongamos que tienes acceso a una base de datos en Amazon Redshift que contiene datos de ventas. Tu tarea es generar un archivo CSV a partir de una consulta SQL y guardar este archivo en un directorio local en tu computadora.

Tareas: 
1. Escribe una consulta SQL que seleccione datos de ventas de la tabla ventas en la base de datos de Redshift. La consulta debe incluir al menos las siguientes columnas: fecha, producto, cantidad_vendida, precio_unitario
2. Utiliza Python y la biblioteca psycopg2 (o cualquier otra biblioteca de tu elección) para ejecutar la consulta SQL en la base de datos de Redshift.
3. Almacena los resultados de la consulta en un archivo CSV local llamado sales_data.csv. Asegúrate de q

# Respuesta

Se debe de tener un archivo llamado **pipeline.conf** con la siguiente estructura:

    [redshift_config]
    hostname = XXXXXXXXXXXXXX
    port = 5439
    username = XXXXXXXXXXXXXX
    password = XXXXXXXXXXXXXX
    database = XXXXXXXXXXXXXX

donde se deben colocar las credenciales correspondientes para hacer la interaccion con Redshift,
se debe tener un archivo llamado **query.sql** que tendra la consulta a realizar,
Instala las dependencias con:

    pip install -r requirements.txt