# -*- coding: utf-8 -*-
"""
Created on Tue Aug 25 10:59:54 2020

@author: matias
"""
from flask import Flask, render_template, request, redirect, url_for, flash
import sys 
import os
#sys.path.append(os.path.abspath("D:\Downloads"))
from obtencion_FFMM import *
from nuevoETLAFP import *
import pandas as pd
import time

#data = pd.DataFrame(MAIN('2016','2020','Todos',2))


app = Flask(__name__)
app.secret_key = 'keysecret'


@app.route('/')
def Index():
    return render_template('index.html')

@app.route('/extraccionesAFP')
def extraccion_afp():
    extract = obtenerExtraccionesAFP()
    return render_template('extraccionesAFP.html', extracciones = extract)

@app.route('/extraccionesFFMM')
def extraccion_ffmm():
    extract = obtenerExtraccionesFFMM()
    return render_template('extraccionesFFMM.html', extracciones_ffmm = extract)

@app.route("/test" , methods=['POST'])
def test():
    time.sleep(1)
    ini = request.form.get('prim1')
    fin = request.form.get('prim2')
    fondo = request.form.get('prim3') 
    estado = MAIN_afp(ini,fin,fondo,2)
    if(estado):
        flash('El periodo que intentas cargar ya existe')
        return redirect(url_for('Index'))
    else:
        flash('Periodo cargado exitosamente')
        return redirect(url_for('Index'))
    
    
    
@app.route("/ffmm" , methods=['POST'])
def test2():
    time.sleep(1)
    mes_str = request.form.get('prim5')
    anio_str = request.form.get('prim4')
    print(anio_str)
    print(mes_str)
    estado = iniciar_ffmm(str(mes_str),str(anio_str))
    if(estado):
        flash('El periodo que intentas cargar ya existe')
        return redirect(url_for('Index'))
    else:
        flash('Periodo cargado exitosamente')
        return redirect(url_for('Index'))
    




