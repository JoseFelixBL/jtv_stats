import mariadb
import sys
import csv


def check_fecha(fecha) -> str:
    """Si existe, quitar la hora de la fecha.
        Ej.: Incomming fecha: 'Call day': '19/8/2017 0:00:00' or 'yyyy-mm-dd'
    Outgoing fecha: 2017/08/19"""
    # CHECK: ¿Existe funciones de fecha para hacer esto?
    if len(fecha) >= 9 and len(fecha) <= 10:
        # return fecha
        separador = '/'
        encontrado = False
        for car in fecha:
            if car == separador:
                encontrado = True
                exit
        if not encontrado:
            separador = '-'
            
        dd, mm, aaaa = fecha.split(sep=separador)
        if len(dd) == 4:
            x = dd
            dd = aaaa
            aaaa = x
        return f'{aaaa}/{int(mm):02}/{int(dd):02}'

    dd, mm, aaaa = fecha.split(sep=' ')[0].split(sep=separador)
    return f'{aaaa}/{int(mm):02}/{int(dd):02}'


def check_hora(hora) -> str:
    """Incomming hora: 'Call time': '30/12/1899 7:54:15'
    Outgoing hora: 07:54:15"""
    # CHECK: ¿Existe funciones de hora para hacer esto?
    if len(hora) == 8:
        return hora
    if len(hora) == 7:
        h, m, s = hora.split(sep=':')
        return f'{int(h):02}:{int(m):02}:{int(s):02}'
    h, m, s = hora.split(sep=' ')[1].split(sep=':')
    return f'{int(h):02}:{int(m):02}:{int(s):02}'


def check_dur(dur) -> int:
    """Devolver la parte entera de Dur
        Ej.: Incomming: 'Length': '56,00', Outgoing: 56"""
    return int(dur.split(sep=',')[0])


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
    """Ejecutar un SELECT y retornar la lista de registros encontrados cursor.fetchall()"""
    cursor.execute(_SQL)
    return cursor.fetchall()


def select_programa(cursor):
    _SELECT = """SELECT id, nombre_excel, nombre_monitor, nombre_salida 
                FROM programas WHERE activo = 1"""
    while True:
        print('ID\t%-25s\t%-15s\t%s' % ('Programa', 'Monitor', 'Salida'))
        print('==\t','='*25, '\t', '='*15, '\t', '='*8)
        for row in db_select(cursor, _SELECT):
            print(f'{row[0]}\t{row[1]:25}\t{row[2]:15}\t{row[3]}')
        prog = input('Por favor seleccione un ID de programa de la lista:')
        if int(prog) > 0:
            return(prog)


def obtener_datos_de_csv(prog):
    # El o los ficheros CSV los debemos obtener de un directorio específico
    # comprobando que exista el directorio y el fichero
    csv_file = '202209 to_access.csv'
    with open(csv_file, 'r', encoding="cp1252") as llamadas:
        reader = csv.DictReader(llamadas, delimiter=';')
        
        # A veces el primer campo tiene caraceres raros en el título...
        call_day = reader.fieldnames[0]
        valores = []
        for row in reader:
            ff = check_fecha(row[call_day]),
            hh = check_hora(row['Call time']),
            cc = row['Caller'],
            dd = check_dur(row['Length']),
            ss = row['Station'],
            vv = row['Voice file'],
            ll = row['Login name'],
            rr = row['Call result'],
            # prog
            valores.append((ff[0], hh[0], cc[0], dd[0], ss[0], vv[0], ll[0], rr[0], prog))
        return(valores)
    
def db_insert(conn, cursor, datos):
    _INSERT = """
        INSERT IGNORE INTO llamadas_copy (fecha,  hora, llamante, dur, station, voice_file,
            log_name, resultado, programa_id
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    try:
        cursor.executemany(_INSERT, datos)
    except mariadb.Error as e:
        print(f"Error: {e}")
        return
    conn.commit()
    
    
def main():
    conn, cursor = db_connect()
    #print(conn, cursor)

    programa_id = select_programa(cursor)
    # programa_id = str(1)
    datos_a_insertar = obtener_datos_de_csv(programa_id)
    db_insert(conn, cursor, datos_a_insertar)
    # print(datos_a_insertar)


if __name__ == "__main__":
    main()