import mariadb
import sys

def db_connect():
    """Conectar a una base de datos, retornar connection y cursor"""
    # Agregar lo de leer el ,env y sacar los datos de ahí
    dbconfig = { 'host': '192.168.0.9',
                'user': 'joyastv_user',
                'password': 'joyastvstats',
                'database': 'joyastv', }

    try:
        # return mariadb.connect(**dbconfig)
        conn = mariadb.connect(**dbconfig)
        cursor = conn.cursor()
        return (conn, cursor)
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)


def db_select(cursor, _SQL):
    """Ejecutar un SELECT y retornar la lista de registros encontrados"""
    cursor.execute(_SQL)
    return cursor.fetchall()


def main():
    conn, cursor = db_connect()
    print(conn, cursor)

    _SQL = """SELECT id, nombre_excel, nombre_monitor, nombre_salida FROM programas WHERE activo = 1"""
    for row in db_select(cursor, _SQL):
        print(f'{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}')


if __name__ == "__main__":
    main()