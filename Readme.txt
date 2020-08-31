Notas:

Te recomiendo que descomprimas los archivos .py en la carpeta de descargas (o donde tu chrome descargue los archivos) ya que por código se buscan los .csv en la misma carpeta en donde están los .py

Tanto en el script nuevoETLAFP.py como en obtencion_FFMM.py hay dos funciones que son las que se conectan a la base de datos, ahí se tienen que cambiar las credenciales con las tuyas
Las funciones son:
conectar_BD_afp() -> para nuevoETLAFP.py
conectar_BD_ffmm() -> para obtencion_FFMM.py

- En ambas funciones Main de los archivos se tiene que cambiar la ruta del Chrome WebDriver
MAIN_afp -> nuevoETLAFP.py
MAIN_ffmm -> obtencion_FFMM.py

--------------------------------------------
El servicio se ejecuta con los siguientes comandos.  Te recomiendo que estos pasos los hagas en la terminal de anaconda en la misma ruta donde están los archivos, y no en el spyder.

Dependencias necesarias:

- $ pip install Flask

El primero se ejecuta solo una vez

1- $ set FLASK_APP=testFlask.py
2- $ python -m flask run
