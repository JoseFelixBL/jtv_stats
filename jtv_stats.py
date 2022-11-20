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
    _SELECT = """SELECT id, nombre_excel, nombre_monitor, nombre_salida, directorio 
                FROM programas WHERE activo = 1"""

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

        prog = input('Por favor seleccione un ID de programa de la lista:')
        if prog in programas:
            sal = salida[prog]
            if salida[prog] == None:
                sal = ''
            dir = directorio[prog]
            if directorio[prog] == None:
                dir = ''
            return(prog, monitor[prog], sal, dir)


def directorios():
    # Directorios
    anchor = Path(os.getenv('OneDrive'))
    relative = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats')
    xls_dir = Path( r'automation\JoyasSQL\PruPandas')
    csv_dir = Path(r'automation\JoyasSQL\DatosCSV')

    abs_xls_dir = anchor.joinpath(relative, xls_dir)
    abs_stats_dir = anchor.joinpath(relative)
    abs_csv_dir = anchor.joinpath(relative, csv_dir)
    abs_downloads_dir = anchor.parent.joinpath('Downloads')

    return(abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir)


def obtener_datos_de_csv(prog, salida):
    """Lee el fichero CSV y devuelve la lista de Tuplas (valores) a insertar en la DB"""
    # El o los ficheros CSV los debemos obtener de un directorio específico
    # comprobando que exista el directorio y el fichero

    # Directorios
    abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()

    # csv_file = '202209 to_access.csv'
    # csv_file = anchor.joinpath(relative, csv_dir, f'{aaaa}{mm} {salida}to_access.csv')
    # csv_file = abs_csv_dir.joinpath(f'{aaaa}{mm} {salida}to_access.csv')

    if salida != '':
        salida = f'{salida} '
    patron = f'?????? {salida}to_access.csv'

    valores = []
    for csv_file in abs_csv_dir.glob(patron):
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
                # prog
                valores.append((ff[0], hh[0], cc[0], dd[0], ss[0], vv[0], ll[0], rr[0], prog))
    return(valores)


def db_insert(conn, cursor, datos):
    """Ejecuta el INSERT en la DB"""
    # Corregir el INSERT poniendo la tabla adecuada: llamadas vs. llamadas_copy
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
    programa_id, _, salida, _ = select_programa(cursor)
    datos_a_insertar = obtener_datos_de_csv(programa_id, salida)
    db_insert(conn, cursor, datos_a_insertar)


def filtros_dias_agente(programa_id):
    """Preparar los parámetros para filtrar los datos de la consulta de días por agente"""
    aaaa, mm = ano_mes()
    return(aaaa, mm, programa_id)


def dias_por_agente(cursor):
    """Consulta de días trabajados por agente"""
    _SELECT =   """SELECT log_name AS nombre, COUNT(fecha) AS n_dias
                        FROM ( 
                            SELECT DISTINCT log_name, fecha
                                FROM llamadas
                            WHERE YEAR(fecha) = ? AND MONTH(fecha) = ? 
                            AND programa_id = ?
                        ) table_alias
                        GROUP BY nombre
                """
    programa_id, _, _, _ = select_programa(cursor)
    filtros = filtros_dias_agente(programa_id)

    print('%-16s %s' % ('Agente', 'Días'))
    print('='*16, '='*4)
    for row in db_select(cursor, _SELECT, filtros):
        print(f'{row[0]:16} {row[1]:4}')
    print()

    
def check_filename(file_name):
    """Check if file name is correct, i.e. starts with numbers
    and is >= 20180927, files before that date have different format"""
    # Check if file is csv
    if not file_name.lower().endswith('.xlsx'):
        return False

    # Check if filename starts with numbers.
    inicio = file_name.split(sep=' ')[0]
    if not inicio.isnumeric():
        return False

    return True


def ano_mes():
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


def crear_csv(cursor):
    """Crea el fichero CSV que será usado para actualizar la DB.
    El nombre del fichero se compone de '{aaaa}{mm} {salida} to_access.csv'"""
    # para obtener el PATH al OneDrive en Windows: os.getenv('OneDrive')
    # print('OneDrive: ' + os.getenv('OneDrive'))

    # Para listar un directorio os.listdir()
    programa_id, monitor, salida, _ = select_programa(cursor)
    aaaa, mm = ano_mes()

    # Directorios
    abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()
    # anchor = Path(os.getenv('OneDrive'))
    # relative = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats')
    # xls_dir = Path( r'automation\JoyasSQL\PruPandas')
    # csv_dir = Path(r'automation\JoyasSQL\DatosCSV')

    lista_df = list()
    # for file in os.listdir(anchor.joinpath(relative, xls_dir)):
    for file in os.listdir(abs_xls_dir):
        if not check_filename(file):
            continue
        # lista_df.append(pd.read_excel(os.path.join(full_xlsx_dir, file)))
        # Concatenar en lista_df todos los Excel
        lista_df.append(pd.read_excel(file))

    # Crea un DataFrame con todos los registros
    new_df = pd.concat(lista_df, ignore_index=True)

    cols = new_df.columns
    if 'CC' in cols:
        new_df = new_df.drop("CC", axis=1)

    # new_df.to_csv(os.path.join(base_dir, csv_dir, csv_file), sep=';', index=False)
     # csv_file = 'to_access.csv'
    # new_df.to_csv(f'{aaaa}{mm} {salida}to_access.csv', sep=';', index=False)
    if salida != '':
        salida = f'{salida} '
    # csv_file = anchor.joinpath(relative, csv_dir, f'{aaaa}{mm} {salida}to_access.csv')
    csv_file = abs_csv_dir.joinpath(f'{aaaa}{mm} {salida}to_access.csv')
    # Guardar el DataFrame como un CSV
    new_df.to_csv(csv_file, sep=';', index=False)


def fin_de_mes(aaaa, mm):
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


def d_ini_d_fin(aaaa, mm):
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


def create_dir(name):
    """Comprobar que existe el directorio, y si no, crearlo."""
    dir = Path(name)
    if not dir.is_dir():
        dir.mkdir()

def check_directorios(stat_dir, servicio, salida):
    """Comprobar que existe el directorio de servicio donde se guardan las STATS."""
    # anchor = Path(os.getenv('OneDrive'))
    # relative = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats')
    
    # Directorios
    abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()

    # Directorio de almacén de los .XLSX por servicio
    create_dir(abs_stats_dir)

    # Directorio de procesado de .XLSX para crear CSV
    # dir_procesado = anchor.joinpath(relative, r'automation\JoyasSQL\PruPandas')
    # create_dir(dir_procesado)
    create_dir(abs_xls_dir)
    # Limpiar directorio de procesado de ficheros viejos del 'servicio'
    # no sé si será bueno... puede que quiera acumular ficheros y procesarlos otro día...
    patron = f'???????? {salida} multioption_monitor_*.xlsx'
    if salida == '':
        patron = f'???????? multioption_monitor_*.xlsx'
    for fichero in abs_xls_dir.glob(patron):
        # print(fichero)
        os.remove(fichero)

    # Directorio de CSV
    # create_dir(anchor.joinpath(relative, r'automation\JoyasSQL\DatosCSV'))
    create_dir(abs_csv_dir)

    # Directorio de OLD_CSV_NO_BORRAR
    # create_dir(anchor.joinpath(relative, r'automation\JoyasSQL\OLD_CSV_NO_BORRAR', f'CSV {servicio}'))
    create_dir(abs_stats_dir.joinpath(r'automation\JoyasSQL\OLD_CSV_NO_BORRAR', f'CSV {servicio}'))

    # Directorio de descargas _OLD_multioption_monitor
    #create_dir(anchor.parent.joinpath('Downloads', '_OLD_multioption_monitor'))
    create_dir(abs_downloads_dir.joinpath('_OLD_multioption_monitor'))

    # Borrar ficheros "multioption_monitor_*" de Downloads antes de empezar a bajar de la web
    # for mul_mon in Path(anchor.parent.joinpath('Downloads')).glob('multioption_monitor_*.xls'):
    for mul_mon in abs_downloads_dir.glob('multioption_monitor_*.xls'):
        print(mul_mon)
        os.remove(mul_mon)


def mover_a_almacen(dir_name, fch):
    """Comprobar que existe el directorio de servicio donde se guardan las STATS."""
    anchor = Path(os.getenv('OneDrive'))
    relative = Path(r'Documentos\Multiopción\TelemediaHU\Multioption Stats')
    suffix = f'.xlsx'

    # Directorios
    abs_xls_dir, abs_stats_dir, abs_csv_dir, abs_downloads_dir = directorios()

    # Directorio de almacén de los .XLSX por servicio
    # dir_almacen = anchor.joinpath(relative, dir_name)
    dir_almacen = abs_stats_dir.joinpath(dir_name)

    # Fichero "multioption_monitor_*"
    # for mul_mon in Path(anchor.parent.joinpath('Downloads')).glob('multioption_monitor_*.xls'):
    for mul_mon in abs_downloads_dir.glob('multioption_monitor_*.xls'):
        final_name = f'{fch} {mul_mon.stem}{suffix}'
        final_path = mul_mon.rename(dir_almacen.joinpath(final_name))
        # shutil.copy2(final_path, anchor.joinpath(relative, r'automation\JoyasSQL\PruPandas'))
        shutil.copy2(final_path, abs_xls_dir)


def pruebas_ficheros():
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
    create_dir()


def sacar_datos_web(cursor):
    """Saca los datos de la web para procesarlos."""
    aaaa, mm = ano_mes()
    d_ini, d_fin = d_ini_d_fin(aaaa, mm)
    programa_id, monitor, salida, dir_servicio = select_programa(cursor)

    check_directorios(dir_servicio, monitor, salida)

    # https://developer.mozilla.org/en-US/docs/Web/WebDriver
    # https://github.com/mozilla/geckodriver/releases/
    # usar geckodriver-v0.32.0-win32.zip 
    # Asignar el driver de Chrome y abrir la pg. web requerida
    # driver = webdriver.Chrome()

    # options = Options()
    # options.binary_location = r"C:\Program Files (x86)\Mozilla Firefox\firefox.exe"
    # driver = webdriver.Firefox(options=options, executable_path="C:/location/to/geckodriver.exe")
    # with webdriver.Firefox(options=options, executable_path="G:/Workspace/jtv_stats/geckodriver.exe") as driver:

    profile_path = r'C:\Users\José\AppData\Roaming\Mozilla\Firefox\Profiles\jwbt8302.default-1596351137250'
    # default_profile = FirefoxProfile(profile_path)
    options=Options()
    options.set_preference('profile', profile_path)
    options.binary_location = r"C:\Program Files (x86)\Mozilla Firefox\firefox.exe"
    service = Service(r'G:/Workspace/jtv_stats/geckodriver.exe')
    with Firefox(service=service, options=options) as driver:
        wait = WebDriverWait(driver, 10)
        driver.get('https://nstat.telemedia.hu/jeweladmin/multioption/monitor/')

        # Dar tiempo para poner usuario y password
        sleep(15)

        for dia in range(int(d_ini), int(d_fin) + 1):
            # Preparar fecha a procesar
            fecha = '{:s}-{:02d}-{:02d}'.format(aaaa, int(mm), dia)
            fch = '{:s}{:02d}{:02d}'.format(aaaa, int(mm), dia)
            buscar = monitor + ' - ' + fecha
            print('La fecha es: ' + fecha)

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
                print ('---------- ' + buscar + ' ----no encontrado.')
                continue
            i_op.click()
            sleep(2)

            # Sacar las estadísticas
            # i_list = driver.find_element_by_xpath('//*[@id="list"]')
            i_list = driver.find_element(By.XPATH, '//*[@id="list"]')
            i_list.click()
            print("============= le he dado al list.")
            sleep(2)

            # Exportar las estadísticas...
            # están es el <div id="calls">
            # i_export = driver.find_element_by_xpath('//*[@id="export_to_excel"]')
            i_export = driver.find_element(By.XPATH, '//*[@id="export_to_excel"]')
            print("============= encontré 'export_to_excel'.")
            i_export.click()
            # ...y Dar tiempo para cerrar la ventana emergente (En Chrome, y en Firefox dar a guardar el fichero)
            sleep(3)

            mover_a_almacen(dir_servicio, fch)

    sleep(8)
    driver.close()



def main():
    conn, cursor = db_connect()

    # pruebas_ficheros()

    while True:
        print('1 - Para sacar datos de la web\n')
        print('2 - Para procesar xls a csv')
        print('3 - Para introducir datos')
        print('5 - Para número de días por agente por mes y año')
        hacer = input('¿Qué desea hacer?: ')
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


if __name__ == "__main__":
    main()