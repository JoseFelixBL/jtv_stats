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


def db_select(cursor, _SQL, valores = ()):
    """Ejecutar un SELECT y retornar la lista de registros encontrados cursor.fetchall()"""
    if len(valores) == 0:
        cursor.execute(_SQL)
    else:
        cursor.execute(_SQL, valores)
    return cursor.fetchall()


def select_programa(cursor):
    """Selecciona el programa de la lista de programas activos"""
    _SELECT = """SELECT id, nombre_excel, nombre_monitor, nombre_salida 
                FROM programas WHERE activo = 1"""
    while True:
        print('ID\t%-25s\t%-15s\t%s' % ('Programa', 'Monitor', 'Salida'))
        print('==\t'+'='*25+ '\t'+ '='*15+ '\t'+ '='*8)

        programas = []
        for row in db_select(cursor, _SELECT):
            print(f'{row[0]}\t{row[1]:25}\t{row[2]:15}\t{row[3]}')
            programas.append(str(row[0]))

        prog = input('Por favor seleccione un ID de programa de la lista:')
        if prog in programas:
            return(prog)


def obtener_datos_de_csv(prog):
    """Lee el fichero CSV y devuelve la lista de Tuplas (valores) a insertar en la DB"""
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
    """Ejecuta el INSERT en la DB"""
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


def introducir_datos(conn, cursor):
    programa_id = select_programa(cursor)
    datos_a_insertar = obtener_datos_de_csv(programa_id)
    db_insert(conn, cursor, datos_a_insertar)


def filtros_dias_agente(programa_id):
    aaaa = '2022'
    mm = '10'
    return(aaaa, mm, programa_id)


def dias_por_agente(cursor):
    _SELECT =   """SELECT log_name AS nombre, COUNT(fecha) AS n_dias
                        FROM ( 
                            SELECT DISTINCT log_name, fecha
                                FROM llamadas 
                            WHERE YEAR(fecha) = ? AND MONTH(fecha) = ? 
                            AND programa_id = ?
                        ) table_alias
                        GROUP BY nombre
                """
    programa_id = select_programa(cursor)
    filtros = filtros_dias_agente(programa_id)

    print('%-16s %s' % ('Agente', 'Días'))
    print('='*16, '='*4)
    for row in db_select(cursor, _SELECT, filtros):
        print(f'{row[0]:16} {row[1]:4}')
    print()

    
def main():
    conn, cursor = db_connect()

    while True:
        print('1 - Para introducir datos')
        print('2 - Para número de dias por agente por mes y año')
        hacer = input('0 - Para salir: ')
        if hacer == '0':
            return()
        elif hacer == '1':
            introducir_datos(conn, cursor)
        elif hacer == '2':
            dias_por_agente(cursor)


if __name__ == "__main__":
    main()