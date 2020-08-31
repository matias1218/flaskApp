# -*- coding: utf-8 -*-
"""
Created on Tue Aug 18 13:14:57 2020

@author: matias
"""
import pandas as pd
from datetime import date
#from sqlalchemy import create_engine
#import numpy as np
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os.path
import csv
import sys
import math
#import tkinter as tk
#from tkinter import ttk,Frame
#from tkcalendar import Calendar, DateEntry
import pyodbc
flag = False



    


def descargar_documentos_afp(anio_inicio, anio_final, file_path,driver):

    # Selección de Año
    anio_ini = driver.find_elements_by_name("aaaaini")
    anio_ini[0].click()
    anio_ini[0].send_keys(anio_inicio)
    anio_ini[0].click()
    
    # Selección de Año
    anio_ter = driver.find_elements_by_name("aaaafin")
    anio_ter[0].click()
    anio_ter[0].send_keys(anio_final)
    anio_ter[0].click()
        
    # Generar archivo CSV
    elem=WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH,"//a[contains(@href,'javascript:generaXLS();')]/img[contains(@src,'/apps/images/xls.gif')]")))
    elem.click()
    
    #time.sleep(2)
    # Ciclo para esperar a que el archivo se descargue
    #path sys.path.append(os.path.abspath("D:\Downloads"))
    while not os.path.exists(file_path):
        time.sleep(1)
        print("Archivo en descarga..")


def cargar_archivos_afp(file_name,data):
    with open(file_name, 'r') as csvfile:
        csvReader = csv.reader(csvfile, delimiter = ';')
        for row in csvReader:
            # chequear si una fila esta vacía
            if not (row):    
                continue
            else:
                # Antes de la AFP UNO
                if(len(row[0]) == 10 and len(row) == 13):
                    data = data.append({'fecha': row[0],'VC_cap':row[1],'VF_cap':row[2],'VC_cup':row[3],'VF_cup':row[4],'VC_hab':row[5],'VF_hab':row[6],'VC_mod':row[7],'VF_mod':row[8],'VC_pla':row[9],'VF_pla':row[10],'VC_pro':row[11],'VF_pro':row[12]},ignore_index=True)
                # Despues de la AFP UNO
                if(len(row[0]) == 10 and len(row) == 15):
                    data = data.append({'fecha': row[0],'VC_cap':row[1],'VF_cap':row[2],'VC_cup':row[3],'VF_cup':row[4],'VC_hab':row[5],'VF_hab':row[6],'VC_mod':row[7],'VF_mod':row[8],'VC_pla':row[9],'VF_pla':row[10],'VC_pro':row[11],'VF_pro':row[12],'VC_uno':row[13],'VF_uno':row[14]},ignore_index=True)
    return data
    
def conectar_BD_afp():
    # Connect to MariaDB Platform
    try:
        server = 'LAPTOP-DKPU01T8' 
        database = 'Extracciones' 
        username = 'matt' 
        password = 'root' 
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    except pyodbc.Error as e:
        print(f"Error connecting to SQLServer Platform: {e}")
        sys.exit(1)
    
    # Get Cursor
    #cur = conn.cursor()
    return conn


# Entradas:  cur -> Conexion BD 
#            data -> dataframe que posee los registros de un fondo 
#            afp -> lista con las afps
#            fondo -> el fondo al que pertenece el dataframe
def insertar_BD_afp(cnxn,data,afp,fondo,sql,sql_2,rango_fechas):
    
    cur = cnxn.cursor()
    
    ## Se inserta el registro en la BD
    for fecha in rango_fechas:
        today = date.today()
        tupla_reg = (fondo,fecha,today.strftime("%d-%m-%Y"))
        cur.execute(sql_2,tupla_reg)
        cnxn.commit()
    
    
    
    ## Se inserta el dataframe en la BD
    for index,row in data.iterrows():
        #print(index)
        #print(row['fecha'])
        fecha = row['fecha']
        VC_cap = row['VC_cap']
        VF_cap = row['VF_cap']
        VC_cup = row['VC_cup']
        VF_cup = row['VF_cup']
        VC_hab = row['VC_hab']
        VF_hab = row['VF_hab']
        VC_mod = row['VC_mod']
        VF_mod = row['VF_mod']
        VC_pla = row['VC_pla']
        VF_pla = row['VF_pla']
        VC_pro = row['VC_pro']
        VF_pro = row['VF_pro']
        VC_uno = row['VC_uno']
        VF_uno = row['VF_uno']
        
        
        # Durante el 2019 las cifras de AFP UNO vienen como string
        
        if(isinstance(VC_uno, str) or isinstance(VF_uno, str)):
            VF_uno = str(VF_uno)
        elif(math.isnan(VC_uno) or math.isnan(VF_uno)):
            VC_uno = 0
            VF_uno = 0
    
        try:
            # Capital
            tupla1 = (afp[0], str(VC_cap).replace(".","").replace(",","."), str(VF_cap).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla1)
            cnxn.commit()
            # Cuprum
            tupla2 = (afp[1], str(VC_cup).replace(".","").replace(",","."), str(VF_cup).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla2)
            cnxn.commit()
            # Habitat
            tupla3 = (afp[2], str(VC_hab).replace(".","").replace(",","."), str(VF_hab).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla3)
            cnxn.commit()
            # Modelo
            tupla4 = (afp[3], str(VC_mod).replace(".","").replace(",","."), str(VF_mod).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla4)
            cnxn.commit()
            # PlanVital
            tupla5 = (afp[4], str(VC_pla).replace(".","").replace(",","."), str(VF_pla).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla5)
            cnxn.commit()
            # Provida
            tupla6 = (afp[5], str(VC_pro).replace(".","").replace(",","."), str(VF_pro).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla6)
            cnxn.commit()
            # Uno
            tupla7 = (afp[6], str(VC_uno).replace(".","").replace(",","."), str(VF_uno).replace(".","").replace(",","."), fondo,fecha)
            cur.execute(sql,tupla7)
            cnxn.commit()
        except:
            print("no se pudo insertar la fila",row)


def concatena_nombre_archivo_afp(fondo_letra,file_path_fondo,periodo):
    if(fondo_letra == 'A'):
        file = file_path_fondo[0] + periodo
    elif(fondo_letra == 'B'):
        file = file_path_fondo[1] + periodo
    elif(fondo_letra == 'C'):
        file = file_path_fondo[2] + periodo
    elif(fondo_letra == 'D'):
        file = file_path_fondo[3] + periodo
    else:
        file = file_path_fondo[4] + periodo
    return file

def revisar_periodo_afp(cnxn,fondo,periodo):

    cursor = cnxn.cursor()
    sql = """ SELECT id_extraccion,periodo_extraccion
      
      FROM TA_AFP_extracciones
      WHERE fondo = ? AND periodo_extraccion = ? """
    for anio in periodo:
        tupla = (fondo,anio)
        cursor.execute(sql,tupla)
        fetch_result = cursor.fetchall()
        if(fetch_result):
            print("el periodo ",anio," existe para el fondo ", fondo)
            return True
            
        else:
            print("el periodo ",anio," no existe para el fondo ", fondo)
    return False
            

def generarFechas_afp(ini,fin):
    lista = []
    ini_int = int(ini)
    fin_int = int(fin)
    
    aux = ini_int
    while(aux <= fin_int):
        lista.append(str(aux))
        aux+=1
    print(lista)
    
    return lista

def obtenerExtraccionesAFP():
    conn = conectar_BD_afp()
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM TA_AFP_extracciones """)
    data = cursor.fetchall()
    return data
    
    

############################# MAIN #############################################
######### Inicialización WebDriver #############


def MAIN_afp(ini,fin,fondo_letra,opcion):
    #driver = webdriver.Chrome('C:/webdriver/chromedriver.exe') # El path varía en cada maquina
    # https://www.spensiones.cl/apps/valoresCuotaFondo/vcfAFP.php?tf=A
    
    #link = 'https://www.spensiones.cl/apps/valoresCuotaFondo/vcfAFP.php?tf='+fondo_letra
    #driver.get(link);
    
    ############ Acceso a la BD #############
    # Parámetros dependen del motor de BD
    conn = conectar_BD_afp()
    
    # FORMATO: create_engine({dialect+driver}://{username}:{password}@{host}:{port}/{database})
    # engine = create_engine('mysql+mysqlconnector://root:root@localhost/extracciones')
    
    ## Variables
    
    file_path_fondo = ["vcfA","vcfB","vcfC","vcfD","vcfE"]
    
    #header = pd.Series(["fecha", "VC_cap", "VF_cap","VC_cup", "VF_cup","VC_hab", "VF_hab","VC_mod", "VF_mod","VC_pla", "VF_pla","VC_pro", "VF_pro",])
    header = pd.DataFrame(columns=["fecha", "VC_cap", "VF_cap","VC_cup", "VF_cup","VC_hab", "VF_hab","VC_mod", "VF_mod","VC_pla", "VF_pla","VC_pro", "VF_pro","VC_uno", "VF_uno",])
    
    fondos = ['A','B','C','D','E'] 
    afp = ["Capital","Cuprum","Habitat","Modelo","Planvital","Provida","Uno"]
    sql_1 = """INSERT INTO TA_AFP_CuotayFondo (afyc_afp, afyc_valorcuota, afyc_valorfondo, afyc_fondo, afyc_fecha) VALUES (?,?,?,?,?);"""
    sql_2 = """INSERT INTO TA_AFP_extracciones (fondo,periodo_extraccion,extraido_en) VALUES (?,?,?);"""
    
    ########### -- PROCESAMIENTO -- ############
    
    ###### MODO 1: Extraer datos entre periodos (hasta el último mes confirmado)
    
    # Extracción
    
    fecha_inicio = ini
    fecha_termino = fin
    rango_fechas = ini + fin
    periodo = fecha_inicio + '-' + fecha_termino + ".csv"
    
    # Generacion del listado de fechas
    listado_fechas = generarFechas_afp(ini,fin);
    
    
    if(fondo_letra != 'Todos'):
        
        
        # Primero se revisa si el periodo ya está cargado 
        existe = revisar_periodo_afp(conn,fondo_letra,listado_fechas)
        if(existe):
            return True
        else:
        
            driver = webdriver.Chrome('C:/webdriver/chromedriver.exe')
            link = 'https://www.spensiones.cl/apps/valoresCuotaFondo/vcfAFP.php?tf='+fondo_letra
            driver.get(link);
            
            file = concatena_nombre_archivo_afp(fondo_letra,file_path_fondo,periodo)
            descargar_documentos_afp(fecha_inicio, fecha_termino, file,driver)
            
            # Carga de Archivo en memoria.
            # Opcion 1: obtener datos
            # Opcion 2: obtener e insertar
            if(opcion == 1):
                # Carga de Archivo en memoria
                fondoA_df = cargar_archivos_afp(file,header)
                return fondoA_df
            elif(opcion == 2):
                # Carga de Archivo en memoria
                fondoA_df = cargar_archivos_afp(file,header)
               # Insertar datos en BD
                insertar_BD_afp(conn,fondoA_df,afp,fondo_letra,sql_1,sql_2,listado_fechas)
            driver.quit() 
        
        
    else:
        for letra in fondos:
            # Primero se revisa si el periodo ya está cargado 
            existe = revisar_periodo_afp(conn,letra,listado_fechas)
            if(existe):
                return True
        
        for letra in fondos:
            
            
            driver = webdriver.Chrome('C:/webdriver/chromedriver.exe')
            link = 'https://www.spensiones.cl/apps/valoresCuotaFondo/vcfAFP.php?tf='+letra
            driver.get(link);
            file = concatena_nombre_archivo_afp(letra,file_path_fondo,periodo)
        
            descargar_documentos_afp(fecha_inicio, fecha_termino, file,driver)
            
            # Carga de Archivo en memoria
            fondoA_df = cargar_archivos_afp(file,header)
            
            # Insertar datos en BD
            insertar_BD_afp(conn,fondoA_df,afp,letra,sql_1,sql_2,listado_fechas)
            print("Fondo ",letra, " insertado")
            driver.quit()
    # MODO 2: Extraer datos del penultimo dia disponible (el último esta sujeto a actualizacion)






"""
def example2():
    top = tk.Toplevel(window)

    ttk.Label(top, text='Choose date').pack(padx=10, pady=10)

    cal = DateEntry(top, width=12, background='darkblue',
                    foreground='white', borderwidth=2)
    cal.pack(padx=10, pady=10)



window = tk.Tk()
window.geometry("400x250")

s = ttk.Style(window)
s.theme_use('clam')

#greeting = tk.Label(window, text="Hello, Tkinter", anchor = 'n')
#greeting.pack()

# genero frames
frame0 = Frame(window)
frame0.config(width=400,height=200)

frame1 = Frame(frame0)
frame1.config(width=200,height=100)

frame2 = Frame(frame0)
frame2.config(width=200,height=100)

frame3 = Frame(window)
frame3.config(width=400,height=200)


# creo elementos en frames
ttk.Label(frame1, text='Periodo inicial').pack(padx=10, pady=10)
#cal = DateEntry(frame1, width=12, background='darkblue',foreground='white', borderwidth=2)
ini = ttk.Combobox(frame1, values=["2013", "2014","2015","2016","2017","2018","2019","2020"])
ini.current(7)
ini.pack(padx=20, pady=20)

# creo elementos en frames
ttk.Label(frame2, text='Periodo final').pack(padx=10, pady=10)

#cal = DateEntry(frame2, width=12, background='darkblue',foreground='white', borderwidth=2)
fin = ttk.Combobox(frame2, values=["2013", "2014","2015","2016","2017","2018","2019","2020"])
fin.current(7)
fin.pack(padx=20, pady=20)

# Box de Entradas
labelTop = ttk.Label(frame3,text = "Fondo")
labelTop.pack()
comboExample = ttk.Combobox(frame3, values=["A", "B","C","D","E","Todos"])
comboExample.current(0)
comboExample.pack()


button_calc = tk.Button(frame3, text="Calculate", command=profit_calculator)
button_calc.pack()

# frames separados horizontalmente
frame1.grid(row=1, column=0)
frame2.grid(row=1, column=1)
frame0.pack()
frame3.pack()


    

window.mainloop()

"""





