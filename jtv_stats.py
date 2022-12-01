import mariadb
import sys
import csv
import pandas as pd
import os
import shutil
from time import sleep
from datetime import datetime
from datetime import timedelta
from pathlib import Path
# If you need some kind of interaction with the page, use Selenium.
from selenium import webdriver
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
# from webdriver_manager.firefox import FirefoxDriverManager


def check_fecha(fecha:str) -> str:
    """Si existe, quitar la hora de la fecha.
        Ej.: Incomming fecha: 'Call day': '19/8/2017 0:00:00' or 'yyyy-mm-dd'
    Outgoing fecha: 2017/08/19"""
    # CHECK: ¿Existe funciones de fecha para hacer esto?
    if len(fecha) >= 9 and len(fecha) <= 10:
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


def check_hora(hora:str) -> str:
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


def check_dur(dur:str) -> int:
    """Devolver la parte entera de Dur
        Ej.: Incomming: 'Length': '56,00', Outgoing: 56"""
    return int(dur.split(sep=',')[0])


def db_connect() -> tuple:
    """Conectar a una base de datos, retornar connection y cursor"""
    # Agregar lo de leer el ,env y sacar los datos de ahí
    """dbconfig = { 'host': '192.168.0.9',
                'user': 'joyastv_user',
                'password': 'joyastvstats',
                'database': 'joyastv', }

    dbconfig = { 'host': f'{DB_HOST}',
                'user': f'{DB_USER}',
                'password': f'{DB_PASSWORD}',
                'database': f'{DB_NAME}', }"""

    dbconfig = { 'host': DB_HOST,
                'user': DB_USER,
                'password': DB_PASSWORD,
                'database': DB_NAME, }

    try:
        conn = mariadb.connect(**dbconfig)
        cursor = conn.cursor()
        return (conn, cursor)
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)


"""No sé cómo poner el type de cursor:
>>> conn = mariadb.connect(**dbconfig)
>>> cursor = conn.cursor()
>>> type(conn)
<class 'mariadb.connections.Connection'>
>>> type(cursor)
<class 'mariadb.cursors.Cursor'> """


def db_select(cursor, _SQL:str, valores:tuple = ())->list:
    """Ejecutar un SELECT y retornar la lista de registros encontrados cursor.fetchall()"""
    if len(valores) == 0:
        cursor.execute(_SQL)
    else:
        cursor.execute(_SQL, valores)
    return cursor.fetchall()


def select_programa(cursor)->tuple:
    """Selecciona el programa de la lista de programas activos
        retorna 4 campos: 
        - ID del programa, 
        - Nombre que sale en el MONITOR
        - Nombre del programa (SALIDA) a agregar en el nombre del fichero de Excel
        - Nombre del DIRECTORIO donde se guarda"""

    _SELECT = f"""SELECT id, nombre_excel, nombre_monitor, nombre_salida, directorio 
                FROM {DB_TABLE_PROGRAMAS} WHERE activo = 1"""

    while True:
        print('ID\t%-25s\t%-15s\t%s' % ('Programa', 'Monitor', 'Salida'))
        print('==\t'+'='*25+ '\t'+ '='*15+ '\t'+ '='*8)

        programas = []
        monitor = dict()
        salida = dict()
        directorio = dict()

        # Escribe la lista de programas activos
        for row in db_select(cursor, _SELECT):
            print(f'{row[0]}\t{row[1]:25}\t{row[2]:15}\t{row[3]}\t{row[4]}')
            programas.append(str(row[0]))
            monitor[str(row[0])] = row[2]
            salida[str(row[0])] = row[3]
            directorio[str(row[0])] = row[4]

        prog = input('Por favor seleccione un ID de programa de la lista: ')
        if prog in programas:
            sal = salida[prog]
            if salida[prog] == None:
                sal = ''
            dir = directorio[prog]
            if directorio[prog] == None:
                dir = ''
            return(prog, monitor[prog], sal, dir)


def directorios()->tuple:
    """Devuelve una lista de directorios a usar:
    - abs_xls_dir, path absoluto del directorio de XLS
    - abs_stats_dir, path absoluto del directorio para guardar las estadísticas sacadas
    - abs_csv_dir, path absoluto del directorio de CSV
    - abs_downloads_dir, path absoluto del directorio de descargas"""
    
    # anchor = Path(os.getenv('OneDrive'))
    relative = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats')
    xls_dir = Path( r'automation\JoyasSQL\PruPandas')
    csv_dir = Path(r'automation\JoyasSQL\DatosCSV')

    abs_xls_dir =       DIR_ABS_ONEDRIVE.joinpath(relative, xls_dir)
    abs_stats_dir =     DIR_ABS_ONEDRIVE.joinpath(relative)
    abs_csv_dir =       DIR_ABS_ONEDRIVE.joinpath(relative, csv_dir)
    abs_downloads_dir = DIR_ABS_ONEDRIVE.parent.joinpath('Downloads')

    return(abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir)


def obtener_datos_de_csv(prog:str, salida:str)->tuple:
    """Lee el fichero CSV y devuelve la lista de Tuplas (valores) a insertar en la DB"""
    # El o los ficheros CSV los debemos obtener de un directorio específico
    # comprobando que exista el directorio y el fichero

    # Directorios
    # abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()

    if salida != '':
        salida = f'{salida} '
    patron = f'?????? {salida}to_access.csv'

    valores = []
    for csv_file in DIR_ABS_CSV.glob(patron):
        print(f'csv_file: {csv_file}')
        with open(csv_file, 'r', encoding="cp1252") as llamadas:
            reader = csv.DictReader(llamadas, delimiter=';')
            # A veces el primer campo tiene caraceres raros en el título...
            call_day = reader.fieldnames[0]
            for row in reader:
                ff = check_fecha(row[call_day]),
                hh = check_hora(row['Call time']),
                cc = row['Caller'],
                dd = check_dur(row['Length']),
                ss = row['Station'],
                vv = row['Voice file'],
                ll = row['Login name'],
                rr = row['Call result'],
                # prog es el último campo necesario
                valores.append((ff[0], hh[0], cc[0], dd[0], ss[0], vv[0], ll[0], rr[0], prog))
    return(valores)


def db_insert(conn, cursor, datos)->None:
    """Ejecuta el INSERT en la DB"""
    # Corregir el INSERT poniendo la tabla adecuada: llamadas vs. llamadas_copy
    _INSERT = f"""
        INSERT IGNORE INTO {DB_TABLE_LLAMADAS} (fecha,  hora, llamante, dur, station, voice_file,
            log_name, resultado, programa_id
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    try:
        cursor.executemany(_INSERT, datos)
        print(f'Insertados {cursor.rowcount} registros')
    except mariadb.Error as e:
        print(f"Error: {e}")
        return
    conn.commit()


def introducir_datos(conn, cursor)->None:
    print('select_programa')
    programa_id, _, salida, _ = select_programa(cursor)
    print('obtener_datos_de_csv')
    datos_a_insertar = obtener_datos_de_csv(programa_id, salida)
    print('db_insert')
    db_insert(conn, cursor, datos_a_insertar)


def filtros_dias_agente(programa_id:str)->tuple:
    """Preparar los parámetros para filtrar los datos de la consulta de días por agente"""
    aaaa, mm = ano_mes()
    return(aaaa, mm, programa_id)


def dias_por_agente(cursor)->None:
    """Consulta de días trabajados por agente.
    Salida por stdout."""
    programa_id, _, _, _ = select_programa(cursor)
    filtros = filtros_dias_agente(programa_id)

    if programa_id == '17' and filtros[0] == '2022' and filtros[1] == '11':
        # Quitar a Tomás y Yudith de la atención nocturna
        _SELECT =   f"""SELECT log_name AS nombre, COUNT(fecha) AS n_dias
                        FROM ( 
                            SELECT DISTINCT log_name, fecha
                                FROM {DB_TABLE_LLAMADAS}
                            WHERE YEAR(fecha) = ? AND MONTH(fecha) = ? 
                            AND programa_id = ?
                            AND log_name <> 'tomfp' AND log_name <> 'yudith'
                        ) table_alias
                        GROUP BY nombre
        """
        _SELECT_TOT = f"""SELECT * FROM (
            SELECT 'Año', 'Mes', 'Nombre', 'Agente', 'Programa', '€/día', 
            CAST( 'Núm. Días' AS CHAR ), 'Total €'
            UNION ALL
            (
                SELECT YEAR(llamadas.fecha) AS Año, MONTH(llamadas.fecha) AS Mes, 
                agentes.nombre AS Nombre, agentes.log_name AS Agente, 
                programas.nombre_monitor AS Programa, 
                FORMAT( programas.factura_hora , 2, 'es_ES') AS '€/hora',
                COUNT(DISTINCT fecha) AS 'Núm. días', 
                FORMAT ( COUNT(DISTINCT fecha) * programas.factura_hora, 2, 'es_ES') AS 'Total €' 
                FROM agentes 
                INNER JOIN {DB_TABLE_LLAMADAS} ON llamadas.log_name = agentes.log_name
                INNER JOIN grupos ON grupos.grupo = agentes.grupo
                INNER JOIN programas ON programas.id = llamadas.programa_id
                WHERE YEAR(llamadas.fecha) = ? AND MONTH(llamadas.fecha) = ?
                AND llamadas.programa_id = ?
                AND {DB_TABLE_LLAMADAS}.log_name <> 'tomfp' AND {DB_TABLE_LLAMADAS}.log_name <> 'yudith'
                GROUP BY llamadas.log_name
                ORDER BY grupos.grupo, llamadas.log_name
            )
        ) resulting_set
        UNION (
            SELECT '-----', '---', '-----', '-----', '-----', '-----', '-----', 
            CAST( FORMAT ( SUM(kk) , 2, 'es_ES') AS CHAR)
            FROM (
                SELECT 
                (COUNT(DISTINCT fecha) * programas.factura_hora ) AS kk
                FROM agentes 
                INNER JOIN llamadas ON llamadas.log_name = agentes.log_name
                INNER JOIN grupos ON grupos.grupo = agentes.grupo
                INNER JOIN programas ON programas.id = llamadas.programa_id
                WHERE YEAR(llamadas.fecha) = ? AND MONTH(llamadas.fecha) = ?
                AND llamadas.programa_id = ?
                AND {DB_TABLE_LLAMADAS}.log_name <> 'tomfp' AND {DB_TABLE_LLAMADAS}.log_name <> 'yudith'
                GROUP BY llamadas.log_name
            ) tt
        )
        """
    else:
        _SELECT =   f"""SELECT log_name AS nombre, COUNT(fecha) AS n_dias
                        FROM ( 
                            SELECT DISTINCT log_name, fecha
                                FROM {DB_TABLE_LLAMADAS}
                            WHERE YEAR(fecha) = ? AND MONTH(fecha) = ? 
                            AND programa_id = ?
                        ) table_alias
                        GROUP BY nombre
        """
        _SELECT_TOT = f"""SELECT * FROM (
            SELECT 'Año', 'Mes', 'Nombre', 'Agente', 'Programa', '€/día', 
            CAST( 'Núm. Días' AS CHAR ), 'Total €'
            UNION ALL
            (
                SELECT YEAR(llamadas.fecha) AS Año, MONTH(llamadas.fecha) AS Mes, 
                agentes.nombre AS Nombre, agentes.log_name AS Agente, 
                programas.nombre_monitor AS Programa, 
                FORMAT( programas.factura_hora , 2, 'es_ES') AS '€/hora',
                COUNT(DISTINCT fecha) AS 'Núm. días', 
                FORMAT ( COUNT(DISTINCT fecha) * programas.factura_hora, 2, 'es_ES') AS 'Total €' 
                FROM agentes 
                INNER JOIN {DB_TABLE_LLAMADAS} ON llamadas.log_name = agentes.log_name
                INNER JOIN grupos ON grupos.grupo = agentes.grupo
                INNER JOIN programas ON programas.id = llamadas.programa_id
                WHERE YEAR(llamadas.fecha) = ? AND MONTH(llamadas.fecha) = ?
                AND llamadas.programa_id = ?
                GROUP BY llamadas.log_name
                ORDER BY grupos.grupo, llamadas.log_name
            )
        ) resulting_set
        UNION (
            SELECT '-----', '---', '-----', '-----', '-----', '-----', '-----', 
            CAST( FORMAT ( SUM(kk) , 2, 'es_ES') AS CHAR)
            FROM (
                SELECT 
                (COUNT(DISTINCT fecha) * programas.factura_hora ) AS kk
                FROM agentes 
                INNER JOIN llamadas ON llamadas.log_name = agentes.log_name
                INNER JOIN grupos ON grupos.grupo = agentes.grupo
                INNER JOIN programas ON programas.id = llamadas.programa_id
                WHERE YEAR(llamadas.fecha) = ? AND MONTH(llamadas.fecha) = ?
                AND llamadas.programa_id = ?
                GROUP BY llamadas.log_name
            ) tt
        )
        """

    print('%-16s %s' % ('Agente', 'Días'))
    print('='*16, '='*4)
    for row in db_select(cursor, _SELECT, filtros):
        print(f'{row[0]:16} {row[1]:4}')
    print()

    for row in db_select(cursor, _SELECT_TOT, list(filtros) + list(filtros)):
        print(f'{row[0]:5};{row[1]:4};{row[2]:20};{row[3]:10};{row[4]:16};{row[5]:7};{str(row[6]):>10};{row[7]:>10}')
    print()


def media_por_agente(cursor)->None:
    """Consulta la media de duración de llamadas atendidas por agente.
    Salida por stdout."""

    programa_id, _, _, _ = select_programa(cursor)
    filtros = [programa_id]
    print('\n1 - si quiere sacar los datos de un mes específico')
    print('0 - si quiere los datos de toda la serie')
    todo = True
    select_fechas = ''
    titulo = 'Datos para toda la serie'
    if input('Elija: ') == '1':
        todo = False
        filtros = filtros_dias_agente(programa_id)
        select_fechas = f'YEAR(fecha) = ? AND MONTH(fecha) = ? AND '
        titulo = f'Datos para el {filtros[1]} de {filtros[0]}'

    # print(type(filtros),filtros)
    _SELECT = f"""SELECT log_name, SEC_TO_TIME( AVG(dur) DIV 1 ) AS dur_media, SUM(dur) AS tot_sec, COUNT(id) AS num_llamadas
            FROM llamadas
            WHERE {select_fechas} programa_id = ?
            GROUP BY log_name
            ORDER BY dur_media ASC
        """
    print('\n' + titulo)
    print('%-16s %-10s %-14s %-12s' % ('Agente', 'dur_media', 'tot_sec', 'num_llamadas'))
    print('='*16, '='*10, '='*14, '='*12)
    for row in db_select(cursor, _SELECT, filtros):
        print(f'{row[0]:16} {str(row[1]):10} {row[2]:14} {row[3]:12}')
    print()
   


def ano_mes()->tuple:
    """Permite seleccionar el año y mes a ser procesado."""
    while True:
        mm = input('Escriba el mes (1-12) que desea procesar: ')
        if int(mm) not in range(1,13):
            continue
        mm = f'{int(mm):02d}'

        aaaa = input('Escriba el año que desea procesar: ')
        if int(aaaa) not in range(2017,2050):
            continue

        return(aaaa, mm)


def crear_csv(cursor)->None:
    """Crea el fichero CSV que será usado para actualizar la DB.
    El nombre del fichero se compone de '{aaaa}{mm} {salida} to_access.csv'."""
    programa_id, monitor, salida, _ = select_programa(cursor)
    aaaa, mm = ano_mes()

    # Directorios
    # abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()
    # anchor = Path(os.getenv('OneDrive'))
    # relative = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats')
    # xls_dir = Path( r'automation\JoyasSQL\PruPandas')
    # csv_dir = Path(r'automation\JoyasSQL\DatosCSV')

    lista_df = list()
    if salida != '':
        salida = f'{salida} '

    patron = f'{aaaa}{mm}?? {salida}multioption_monitor_*.xlsx'
    # print(f'PATRON; {patron}')

    vacio = True
    for file in DIR_ABS_XLSX.glob(patron):
        vacio = False
        # Concatenar en lista_df todos los Excel
        # print(f'===> Fichero a concatenar: {file} <===')
        lista_df.append(pd.read_excel(DIR_ABS_XLSX.joinpath(file)))

    # Crea un DataFrame con todos los registros
    if vacio:
        print('WARNING: Lista de ficheros XLSX a concatenar vacía')
        return
    new_df = pd.concat(lista_df, ignore_index=True)

    cols = new_df.columns
    if 'CC' in cols:
        new_df = new_df.drop("CC", axis=1)

    csv_file = DIR_ABS_CSV.joinpath(f'{aaaa}{mm} {salida}to_access.csv')
    # Guardar el DataFrame como un CSV
    new_df.to_csv(csv_file, sep=';', index=False)


def fin_de_mes(aaaa:str, mm:str)->str:
    """Ver cuantos días tiene un mes usando datetime y timedelta"""
    fi = f'01-{int(mm):02}-{aaaa}'
    mf = int(mm) + 1
    aaaaf = int(aaaa)
    if mf == 13:
        mf = 1
        aaaaf = int(aaaa) + 1
    ff = f'01-{int(mf):02}-{aaaaf}'
    di = datetime.strptime(fi, "%d-%m-%Y")
    df = datetime.strptime(ff, "%d-%m-%Y")
    t = df - di
    return(t.days)


def d_ini_d_fin(aaaa, mm)->tuple:
    """Preguntar y comprobar día de inicio y día de fin a sacar."""
    d_ini = 1
    d_fin = int(fin_de_mes(aaaa, mm))

    ini = d_ini
    fin = d_fin
    while True:
        ok = input(f'¿Sacar todo el mes, del 1 al {d_fin}? 1 = Sí, 0 = no: ')
        if ok == '1':
            return(d_ini, d_fin)
        
        ini = input('Día inicial: ')
        if int(ini) > d_fin:
            print(f'Error: el día de inicio no puede ser mayor que el máximo número de días del mes ({d_fin})')
            continue
        elif int(ini) < 1:
            print('Error: el día de inicio no puede ser menor que 1')
            continue
       
        fin = input('Día final: ')
        if int(fin) > d_fin:
            print(f'Error: el día de fin no puede ser mayor que el máximo número de días del mes ({d_fin})')
            continue
        elif int(fin) < 1 or int(fin) < int(ini):
            print('Error: el día de fin no puede ser menor que 1 o anterior al día de inicio')
            continue

        return(ini, fin)


def create_dir(name:str)->None:
    """Comprobar que existe el directorio, y si no, crearlo."""
    dir = Path(name)
    if not dir.is_dir():
        dir.mkdir()


def check_directorios(stat_dir:str, servicio:str, salida:str)->None:
    """Comprobar que existe el directorio de servicio donde se guardan las STATS."""
    
    # Directorios
    # abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()

    # Directorio de almacén de los .XLSX por servicio
    create_dir(DIR_ABS_STATS)
    create_dir(DIR_ABS_STATS.joinpath(stat_dir))

    # Directorio de procesado de .XLSX para crear CSV
    create_dir(DIR_ABS_XLSX)
    # Limpiar directorio de procesado de ficheros viejos del 'servicio'
    # no sé si será bueno... puede que quiera acumular ficheros y procesarlos otro día...
    patron = f'???????? {salida} multioption_monitor_*.xlsx'
    if salida == '':
        patron = f'???????? multioption_monitor_*.xlsx'
    anteriores = False
    for fichero in DIR_ABS_XLSX.glob(patron):
        print(fichero)
        anteriores = True
        os.remove(fichero)
    if anteriores:
        if input('\nHe encontrado ficheros anteriores, ¿quiere borrarlos? (s/n): ') == 's':
            for fichero in DIR_ABS_XLSX.glob(patron):
                os.remove(fichero)
           

    # Directorio de CSV
    create_dir(DIR_ABS_CSV)

    # Directorio de OLD_CSV_NO_BORRAR
    create_dir(DIR_ABS_STATS.joinpath(r'automation\JoyasSQL\OLD_CSV_NO_BORRAR', f'CSV {servicio}'))

    # Directorio de descargas _OLD_multioption_monitor
    create_dir(DIR_ABS_DOWNLOADS.joinpath('_OLD_multioption_monitor'))

    # Borrar ficheros "multioption_monitor_*" de Downloads antes de empezar a bajar de la web
    for mul_mon in DIR_ABS_DOWNLOADS.glob('multioption_monitor_*.xls'):
        print(mul_mon)
        os.remove(mul_mon)


def mover_a_almacen(dir_servicio:str, fch:str, salida:str)->None:
    """Comprobar que existe el directorio de servicio donde se guardan las STATS."""

    suffix = f'.xlsx'

    # Directorios
    # abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()

    # Directorio de almacén de los .XLSX por servicio
    dir_abs_almacen = DIR_ABS_STATS.joinpath(dir_servicio)

    # Fichero "multioption_monitor_*"
    for mul_mon in DIR_ABS_DOWNLOADS.glob('multioption_monitor_*.xls'):
        final_name = f'{fch} {salida} {mul_mon.stem}{suffix}'
        if salida == '':
            final_name = f'{fch} {mul_mon.stem}{suffix}'
        final_path = Path(mul_mon).rename(Path(dir_abs_almacen, final_name))
        shutil.copy2(final_path, DIR_ABS_XLSX)


def pruebas_ficheros()->None:
    """Pruebas de funciones de ficheros, directorios y cambiar nombres."""
    """ Con pathlib.Path:
    exists()
    glob(pattern) iterates over this subtree and yields all existing files matching pattern
    is_dir()
    is_file()
    iterdir() iterates over all files of this directory
    mkdir()
    rename(target)

    Class methods:
    cwd()
    Recordar que también se pueden usar métodos o funciones de os, por ejemplo os.getcwd()

    Methods from Purepath:
    match()
    with_name(name) sustituye TODO el nombre con name
    with_stem(stem) sustituye el nombre (stem), no el sufijo
    with suffix(suffix) sustituye el sufijo por otro (tiene que empezar por ".")

    joinpath(*args)
    """

    # C:\Users\José\Downloads\multioption_monitor_08_53_14.xls
    one_drive = Path(os.getenv('OneDrive'))
    r_dir_xlsx = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats\automation\JoyasSQL\PruPandas')
    a_dir_xlsx = one_drive.joinpath(r_dir_xlsx)
    print(f'{a_dir_xlsx}')

    for f in a_dir_xlsx.glob('*multioption_monitor_*'):
        print(f)

    return

    p = Path(r'C:\Users\José\Downloads')
    y_n = "no "
    if p.is_dir():
        y_n = ""
    print(f'{p} {y_n}es un directorio.')

    y_n = "no "
    if p.is_file():
        y_n = ""
    print(f'{p} {y_n}es un fichero.')

    print(f'Yo estoy en {Path.cwd()}')

    for f in p.glob('multioption_monitor_*'):
        print(f)

    one_drive = Path(os.getenv('OneDrive'))
    print(f'{one_drive}')
    print(f'{one_drive.parent}')
 
    r_dir_xlsx = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats\automation\JoyasSQL\PruPandas')
    a_dir_xlsx = one_drive.joinpath(r_dir_xlsx)
    print(f'{a_dir_xlsx}')

    f = Path(r'C:\Users\José\Downloads\multioption_monitor_08_53_14.xls')
    print(f'{f}')
    kk = 'pepito'
    print(f'with_name({kk}) : {f.with_name(kk)}')
    print(f'with_stem({kk}) : {f.with_stem(kk)}')
    print(f'with_suffix({kk}) : {f.with_suffix(f".{kk}")}')


def sacar_datos_web(cursor)->None:
    """Saca los datos de la web para procesarlos."""
    aaaa, mm = ano_mes()
    d_ini, d_fin = d_ini_d_fin(aaaa, mm)
    programa_id, monitor, salida, dir_servicio = select_programa(cursor)

    # prueba de la creación de directorios
    check_directorios(dir_servicio, monitor, salida)

    # https://developer.mozilla.org/en-US/docs/Web/WebDriver
    # https://github.com/mozilla/geckodriver/releases/
    # usar geckodriver-v0.32.0-win32.zip 

    # profile_path = r'C:\Users\José\AppData\Roaming\Mozilla\Firefox\Profiles\jwbt8302.default-1596351137250'
    # default_profile = FirefoxProfile(profile_path)
    options=Options()
    # options.set_preference('profile', profile_path)
    options.set_preference('profile', FIREFOX_PROFILE)
    # options.binary_location = r"C:\Program Files (x86)\Mozilla Firefox\firefox.exe"
    # service = Service(r'G:/Workspace/jtv_stats/geckodriver.exe')
    options.binary_location = FIREFOX_BINARY_LOCATION
    service = Service(FIREFOX_GECKODRIVER)

    with Firefox(service=service, options=options) as driver:
        wait = WebDriverWait(driver, 15)
        driver.get('https://nstat.telemedia.hu/jeweladmin/multioption/monitor/')

        """ 
        # test focus & send_keys for user and password
        sleep(7)
        driver.find_element(By.LINK_TEXT, "Nombre de usuario").send_keys('supervisor')
        driver.find_element(By.LINK_TEXT, "Contraseña").send_keys('multioption17')
        sleep(2)
        driver.find_element(By.LINK_TEXT, "Iniciar sesión").send_keys(Keys.RETURN)

        Exception has occurred: UnexpectedAlertPresentException
        Alert Text: None
        Message: Dismissed user prompt dialog: Este sitio le pide que inicie sesión.
        Stacktrace:
        RemoteError@chrome://remote/content/shared/RemoteError.sys.mjs:8:8
        WebDriverError@chrome://remote/content/shared/webdriver/Errors.sys.mjs:182:5
        UnexpectedAlertOpenError@chrome://remote/content/shared/webdriver/Errors.sys.mjs:487:5
        GeckoDriver.prototype._handleUserPrompts@chrome://remote/content/marionette/driver.sys.mjs:2695:13
        File "G:\Workspace\jtv_stats\jtv_stats.py", line 562, in sacar_datos_web
            driver.find_element(By.LINK_TEXT, "Nombre de usuario").send_keys('supervisor')
        File "G:\Workspace\jtv_stats\jtv_stats.py", line 713, in main
            sacar_datos_web(cursor)
        File "G:\Workspace\jtv_stats\jtv_stats.py", line 727, in <module>
            main()
        """

        # Dar tiempo para poner usuario y password
        sleep(15)

        for dia in range(int(d_ini), int(d_fin) + 1):
            # Preparar fecha a procesar
            fecha = '{:s}-{:02d}-{:02d}'.format(aaaa, int(mm), dia)
            fch = '{:s}{:02d}{:02d}'.format(aaaa, int(mm), dia)
            buscar = monitor + ' - ' + fecha
            print('La fecha es: ' + fecha + ' - ', end='')

            # Poner la fecha
            # i_day = driver.find_element_by_xpath('//*[@id="day"]')
            # i_day = driver.find_element_by_name("day")
            i_day = driver.find_element(By.NAME, "day")
            i_day.clear()
            i_day.send_keys(fecha)
            sleep(1)
            i_day.send_keys(Keys.RETURN)
            sleep(1)
            i_day.send_keys(Keys.RETURN)
            sleep(5)

            # Hacer click en el buscador de fechas
            # i_cal = driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]')
            # i_cal.click()
            # sleep(2)

            #Lista de shows
            # i_show = driver.find_element_by_xpath('//*[@id="shows"]')
            # i_opciones = i_show.find_elements_by_tag_name('option')
            i_show = driver.find_element(By.XPATH, '//*[@id="shows"]')
            i_opciones = i_show.find_elements(By.TAG_NAME, 'option')
            no_data = True

            # Buscar el show de la fecha
            for op in i_opciones:
                # if fecha in op.text:
                if buscar in op.text:
                    no_data = False
                    i_op = op
                    print(op.text + ' encontrado.')
                    break
            if no_data :
                print ('-----------> NO ENCONTRADO.')
                continue
            i_op.click()
            sleep(2)

            # Sacar las estadísticas
            # i_list = driver.find_element_by_xpath('//*[@id="list"]')
            i_list = driver.find_element(By.XPATH, '//*[@id="list"]')
            i_list.click()
            # print("============= le he dado al list.")
            sleep(2)

            # Exportar las estadísticas...
            # están es el <div id="calls">
            # i_export = driver.find_element_by_xpath('//*[@id="export_to_excel"]')
            i_export = driver.find_element(By.XPATH, '//*[@id="export_to_excel"]')
            # print("============= encontré 'export_to_excel'.")
            i_export.click()
            # ...y Dar tiempo para cerrar la ventana emergente (En Chrome, y en Firefox dar a guardar el fichero)
            sleep(3)

            mover_a_almacen(dir_servicio, fch, salida)

    sleep(8)
    # driver.close()  # No hace falta, está incluido en el context manager 'WITH'



def carga_punto_env()->None:
    """Cargar como globales variables de environment definidas en el fichero .env"""
    from dotenv import load_dotenv

    load_dotenv()

    # Database global variables
    global DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_TABLE_LLAMADAS, DB_TABLE_PROGRAMAS

    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    DB_TABLE_LLAMADAS = os.getenv('DB_TABLE_LLAMADAS')
    DB_TABLE_PROGRAMAS = os.getenv('DB_TABLE_PROGRAMAS')

    # Firefox
    global FIREFOX_PROFILE, FIREFOX_BINARY_LOCATION, FIREFOX_GECKODRIVER

    FIREFOX_PROFILE = os.getenv('FIREFOX_PROFILE')
    FIREFOX_BINARY_LOCATION = os.getenv('FIREFOX_BINARY_LOCATION')
    FIREFOX_GECKODRIVER = os.getenv('FIREFOX_GECKODRIVER')
    
    # Web
    global STATS_WEB

    STATS_WEB = os.getenv('STATS_WEB')
    
    # Directorios
    global DIR_RELATIVE, DIR_XLSX, DIR_CSV

    DIR_RELATIVE = os.getenv('DIR_RELATIVE')
    DIR_XLSX = os.getenv('DIR_XLSX')
    DIR_CSV = os.getenv('DIR_CSV')


def otras_globales()->None:
    """Cargar otras variables globales necesarias"""
    global DIR_ABS_XLSX, DIR_ABS_STATS, DIR_ABS_CSV, DIR_ABS_DOWNLOADS

    global DIR_ABS_ONEDRIVE
    DIR_ABS_ONEDRIVE = Path(os.getenv('OneDrive'))
    DIR_ABS_XLSX, DIR_ABS_STATS, DIR_ABS_CSV, DIR_ABS_DOWNLOADS = directorios()


def main()->None:
    carga_punto_env()
    otras_globales()

    """
    print(DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_TABLE_LLAMADAS, DB_TABLE_PROGRAMAS)
    print(FIREFOX_PROFILE, FIREFOX_BINARY_LOCATION, FIREFOX_GECKODRIVER)
    print(STATS_WEB, DIR_RELATIVE, DIR_XLSX, DIR_CSV)
    print(DIR_RELATIVE, DIR_XLSX, DIR_CSV)
    print(DIR_ABS_XLSX, DIR_ABS_STATS, DIR_ABS_CSV, DIR_ABS_DOWNLOADS)
    print(DIR_ABS_ONEDRIVE)
    """

    conn, cursor = db_connect()

    # pruebas_ficheros()

    while True:
        print('\n\nMenú')
        print(50*'=')
        print('1 - Para sacar datos de la web.')
        print('2 - Para procesar xls a csv.')
        print('3 - Para introducir datos csv en la base de datos.')
        print()
        print('5 - Para número de días por agente por mes y año.')
        print('6 - Para tiempo medio de atención por agente por mes y año.')
        # print('9 - TEST: PRUEBAS DE FICHEROS.')
        print()
        print('0 - Para terminar.')
        hacer = input('\n¿Qué desea hacer?: ')
        if hacer == '0':
            return()
        elif hacer == '1':
            sacar_datos_web(cursor)
        elif hacer == '2':
            crear_csv(cursor)
        elif hacer == '3':
            introducir_datos(conn, cursor)
        elif hacer == '5':
            dias_por_agente(cursor)
        elif hacer == '6':
            media_por_agente(cursor)
        elif hacer == '9':
            pruebas_ficheros()


if __name__ == "__main__":
    main()