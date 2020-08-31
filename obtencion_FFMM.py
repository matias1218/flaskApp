# -*- coding: utf-8 -*-
"""
Created on Mon Aug 17 14:44:43 2020

@author: matias
"""
import pandas as pd
from datetime import date
#from sqlalchemy import create_engine
import numpy as np
import time
import sys
from selenium import webdriver
import os.path
#import tkinter as tk
#from tkinter import ttk,Frame,messagebox
#from tkcalendar import Calendar, DateEntry
import pyodbc
################## FUNCIONES ################



    
    

def descargar_documentos_ffmm(cartera, mes, anio, file_name, periodo_actual, file_path, driver):

    # Selección de Mes  
    mes_elem = driver.find_elements_by_name("mm")
    mes_elem[0].click()
    mes_elem[0].send_keys(mes)
    mes_elem[0].click()
    
    # Selección de Año
    anio_elem = driver.find_elements_by_name("aa")
    anio_elem[0].click()
    anio_elem[0].send_keys(anio)
    anio_elem[0].click()
    
    # Selección de Tipo Cartera
    cartera_elem = driver.find_elements_by_name("cartera")
    cartera_elem[0].click()
    cartera_elem[0].send_keys(cartera)
    cartera_elem[0].click()
    
    # Generar archivo txt
    generar_btn = driver.find_elements_by_name("btnConsulta")
    generar_btn[0].click()
    
    time.sleep(2)
    
    # Ciclo para esperar a que el archivo se descargue
    while not os.path.exists(file_path):
        print("archivo aun en descarga..")
        time.sleep(1)


def cargar_archivos_ffmm(file_name):
    
    ########## Carga del archivo en BD ########
    # El nombre del archivo viene con formato: FFM_INV_NAC + AÑO + MES  -> Periodo de carga?
    data = pd.read_csv(file_name,sep=';') 
    
    return data

def agregar_periodo_carga_ffmm(data,periodo_actual):
    data["periodocarga"] = np.nan
    for i in range(0,data.shape[0]):
        data.iloc[i,len(data.columns)-1] = int(periodo_actual)
    return data
    
def insertar_BD_ffmm(datos,col,tabla,cnxn,val):
    # Cambio de nombres de las columnas
    newData = datos.rename(columns=col)
    #newData = data.rename(columns={"Run Fondo":"runfondo","Nombre Fondo":"nomfondo","FFM_6010100":"nemo","FFM_6010211":"rut_emisor","FFM_6010212":"dv_emisor","FFM_6010300":"pais_emisor", "FFM_6010400":"tipo_instrumento", "FFM_6010500":"fecha_vencimiento", "FFM_6010600":"status_instrumento","FFM_6010700":"clasif_riesgo", "FFM_6010800":"codgrupo_empresa", "FFM_6010900":"cant_unidades", "FFM_6011000":"moneda", "FFM_TIR_6011111":"tir_instrum_deuda", "FFM_PAR_6011111":"valor_par_tasa_flotante", "FFM_REL_6011111":"valor_relev_inst_capital", "FFM_6011112":"cod_valorizacion", "FFM_6011113":"base_tasa", "FFM_6011114":"tipo_interes", "FFM_6011200":"valor_cierre", "FFM_6011300":"cod_moneda_liquid", "FFM_6011400":"cod_pais_transaccion", "FFM_6011511":"porcentaje_capital_emisor", "FFM_6011512":"porcentaje_activos_emisor", "FFM_6011513":"porcentaje_activos_fondo"})
    
    ## Con el dataframe en memoria, se pasan los datos a la BD
    #newData.to_sql(name=tabla, con=engine, if_exists='append', index=False)
    sql_1 = """INSERT INTO TA_FFMM_CartInv_Nacionales
           (runFondo
           ,nomFondo
           ,nemo
           ,rut_emisor
           ,dv_emisor
           ,pais_emisor
           ,tipo_instrumento
           ,fecha_vencimiento
           ,status_instrumento
           ,clasif_riesgo
           ,codgrupo_empresa
           ,cant_unidades
           ,moneda
           ,TIR_instrum_deuda
           ,valor_par_tasa_flotante
           ,valor_relev_inst_capital
           ,cod_valorizacion
           ,base_tasa
           ,tipo_interes
           ,valor_cierre
           ,cod_moneda_liquid
           ,cod_pais_transaccion
           ,porcentaje_capital_emisor
           ,porcentaje_activos_emisor
           ,porcentaje_activos_fondo
           ,periodoCarga)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    sql_2 = """INSERT INTO TA_FFMM_CartInv_Internacionales
           (runFondo
           ,nomFondo
           ,nemo
           ,nomEmisor
           ,codPais
           ,tipo_instrumento
           ,fecha_vencimiento
           ,status_instrumento
           ,clasif_riesgo
           ,codgrupo_empresa
           ,cant_unidades
           ,moneda
           ,TIR_instrum_deuda
           ,valor_par_tasa_flotante
           ,valor_relev_inst_capital
           ,cod_valorizacion
           ,base_tasa
           ,tipo_interes
           ,valor_cierre
           ,cod_moneda_liquid
           ,cod_pais_transaccion
           ,porcentaje_capital_emisor
           ,porcentaje_activos_emisor
           ,porcentaje_activos_fondo
           ,periodoCarga)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    a = newData.where(pd.notnull(newData), None)
    cursor = cnxn.cursor()
    if(val==1):
        for index,row in a.iterrows():
            tupla1 = (row['runFondo'],row['nomFondo'],row['nemo'],row['rut_emisor'],row['dv_emisor'],row['pais_emisor'],row['tipo_instrumento'],row['fecha_vencimiento'],row['status_instrumento'],row['clasif_riesgo'],row['codgrupo_empresa'],row['cant_unidades'],row['moneda'],row['tir_instrum_deuda'],row['valor_par_tasa_flotante'],row['valor_relev_inst_capital'],row['cod_valorizacion'],row['base_tasa'],row['tipo_interes'],row['valor_cierre'],row['cod_moneda_liquid'],row['cod_pais_transaccion'],row['porcentaje_capital_emisor'],row['porcentaje_activos_emisor'],row['porcentaje_activos_fondo'],row['periodocarga'])
            cursor.execute(sql_1,tupla1)
            cnxn.commit()
        
    else:
        for index,row in a.iterrows():
            tupla2 = (row['runFondo'],row['nomFondo'],row['nemo'],row['nomEmisor'],row['codPais'],row['tipo_instrumento'],row['fecha_vencimiento'],row['status_instrumento'],row['clasif_riesgo'],row['codgrupo_empresa'],row['cant_unidades'],row['moneda'],row['tir_instrum_deuda'],row['valor_par_tasa_flotante'],row['valor_relev_inst_capital'],row['cod_valorizacion'],row['base_tasa'],row['tipo_interes'],row['valor_cierre'],row['cod_moneda_liquid'],row['cod_pais_transaccion'],row['porcentaje_capital_emisor'],row['porcentaje_activos_emisor'],row['porcentaje_activos_fondo'],row['periodocarga'])
            cursor.execute(sql_2,tupla2)
            cnxn.commit()
    
def insertar_registro_ffmm(cartera,periodo,cnxn):
    cursor = cnxn.cursor()
    today = date.today()
    sql = """INSERT INTO TA_FFMM_extracciones
       (cartera
       ,periodo_extraccion
       ,extraido_en)
       VALUES (?,?,?);"""
    tupla = (cartera,periodo,today.strftime("%d-%m-%Y"))
    cursor.execute(sql,tupla)
    cnxn.commit()
    
def conectar_BD_ffmm():
    # Connect to MariaDB Platform
    try:
        server = 'LAPTOP-DKPU01T8' 
        database = 'Extracciones' 
        username = 'matt' 
        password = 'root' 
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    except pyodbc.Error as e:
        print(f"Error connecting to SQLserver Platform: {e}")
        sys.exit(1)
    
    # Get Cursor
    #cur = conn.cursor()
    return conn


def MAIN_ffmm(mes,anio,cnxn):
    ############################# MAIN #############################################
    ######### Inicialización WebDriver #############
    
    driver = webdriver.Chrome('C:/webdriver/chromedriver.exe') # El path varía en cada maquina
    driver.get('http://www.cmfchile.cl/institucional/estadisticas/ffm_cartera.php');
    
    periodo_actual = anio+mes
    
    col_nac = {"Run Fondo":"runFondo","Nombre Fondo":"nomFondo","FFM_6010100":"nemo","FFM_6010211":"rut_emisor","FFM_6010212":"dv_emisor","FFM_6010300":"pais_emisor", "FFM_6010400":"tipo_instrumento", "FFM_6010500":"fecha_vencimiento", "FFM_6010600":"status_instrumento","FFM_6010700":"clasif_riesgo", "FFM_6010800":"codgrupo_empresa", "FFM_6010900":"cant_unidades", "FFM_6011000":"moneda", "FFM_TIR_6011111":"tir_instrum_deuda", "FFM_PAR_6011111":"valor_par_tasa_flotante", "FFM_REL_6011111":"valor_relev_inst_capital", "FFM_6011112":"cod_valorizacion", "FFM_6011113":"base_tasa", "FFM_6011114":"tipo_interes", "FFM_6011200":"valor_cierre", "FFM_6011300":"cod_moneda_liquid", "FFM_6011400":"cod_pais_transaccion", "FFM_6011511":"porcentaje_capital_emisor", "FFM_6011512":"porcentaje_activos_emisor", "FFM_6011513":"porcentaje_activos_fondo"}
    col_inter = {"Run Fondo":"runFondo","Nombre Fondo":"nomFondo","FFM_6020100":"nemo","FFM_6020200":"nomEmisor","FFM_6020300":"codPais", "FFM_6020400":"tipo_instrumento", "FFM_6020500":"fecha_vencimiento", "FFM_6020600":"status_instrumento","FFM_6020700":"clasif_riesgo", "FFM_6020800":"codgrupo_empresa", "FFM_6020900":"cant_unidades", "FFM_6021000":"moneda", "FFM_tir_6021111":"tir_instrum_deuda", "FFM_par_6021111":"valor_par_tasa_flotante", "FFM_rel_6021111":"valor_relev_inst_capital", "FFM_6021112":"cod_valorizacion", "FFM_6021113":"base_tasa", "FFM_6021114":"tipo_interes", "FFM_6021200":"valor_cierre", "FFM_6021300":"cod_moneda_liquid", "FFM_6021400":"cod_pais_transaccion", "FFM_6021511":"porcentaje_capital_emisor", "FFM_6021512":"porcentaje_activos_emisor", "FFM_6021513":"porcentaje_activos_fondo"}
    cartera = ["Cartera de Inversiones Nacionales","Cartera de Inversiones Extranjeras"]
    tablas = ['ta_ffmm_cartinv_nacionales','ta_ffmm_cartinv_internacionales']
    
    file_name_nac = "FFM_INV_NACI_"+periodo_actual+".txt"
    file_name_ext = "FFM_INV_EXTR_"+periodo_actual+".txt"
    
    file_path_nac = "D:/Downloads/"+file_name_nac
    file_path_ext = "D:/Downloads/"+file_name_ext
    
    
    ## PRIMERA EXTRACCION ##
    descargar_documentos_ffmm(cartera[0], mes, anio, file_name_nac, periodo_actual, file_path_nac, driver)
    print("primera extraccion completa")
    
    ## PRIMERA CARGA ###
    data = cargar_archivos_ffmm(file_name_nac)
    data_periodo = agregar_periodo_carga_ffmm(data,periodo_actual)
    insertar_BD_ffmm(data_periodo,col_nac,tablas[0],cnxn,1)
    insertar_registro_ffmm(tablas[0],periodo_actual,cnxn)
    print("primera carga completa")
    
    ## SEGUNDA EXTRACCION ##
    descargar_documentos_ffmm(cartera[1], mes, anio, file_name_ext, periodo_actual, file_path_ext, driver)
    print("Segunda extraccion completa")
    
    ## SEGUNDA CARGA ###
    data = cargar_archivos_ffmm(file_name_ext)
    data_periodo = agregar_periodo_carga_ffmm(data,periodo_actual)
    insertar_BD_ffmm(data_periodo,col_inter,tablas[1],cnxn,2)
    insertar_registro_ffmm(tablas[1],periodo_actual,cnxn)
    print("Segunda carga completa")
    
    driver.quit()



"""
    today = date.today()
    periodo_actual = ""
    if today.month < 10:
        periodo_actual = str(today.year) + str(0)+ str(today.month-1)
    else:
        periodo_actual = str(today.year) + str(today.month)   
        
    mes = periodo_actual[4:]  # Elimina los primeros 4 valores
    anio = periodo_actual[:4] # Deja los primeros 4 valores
    return mes,anio,periodo_actual
    """
def revisar_periodo_ffmm(cnxn,mes,anio):

    cursor = cnxn.cursor()
    sql = """ SELECT id_extraccion
      ,cartera
      ,periodo_extraccion
      ,extraido_en
      FROM TA_FFMM_extracciones
      WHERE periodo_extraccion = ?"""
    tupla = (anio+mes)
    print(tupla)
    cursor.execute(sql,tupla)
    fetch_result = cursor.fetchall()
    if(fetch_result):
        return True
    else:
        return False
    
def obtenerExtraccionesFFMM():
    conn = conectar_BD_ffmm()
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM TA_FFMM_extracciones """)
    data = cursor.fetchall()
    return data

def iniciar_ffmm(mes_str,anio_str):
    
    cnxn = conectar_BD_ffmm()
    
    # Revisar si el periodo ya existe en la BD
    flag = revisar_periodo_ffmm(cnxn,mes_str,anio_str)        
    if(flag):
        print("El periodo ya está cargado")
        return True
    else:
        print("periodo no cargado aun")
    
    MAIN_ffmm(mes_str,anio_str,cnxn)
    
##########################################################################################################################









