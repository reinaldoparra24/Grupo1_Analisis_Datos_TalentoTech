import pandas as pd
from pandas import *

dataset = pd.read_csv("Fuentes_No_Convencionales_de_Energ_a_Renovable.csv")

"""
Los datos contienen 158 registros y 13 columnas, con las siguientes características:

* Proyecto: Nombre del proyecto.
* Tipo: Tipo de fuente (Eólica, Solar).
* Capacidad: Capacidad del proyecto en MW.
* Departamento: Departamento donde se localiza el proyecto.
* Municipio: Municipio del proyecto.
* Código Departamento y Código Municipio: Códigos administrativos.
* Fecha estimada FPO: Fecha estimada de puesta en operación.
* Energía [kWh/día]: Energía generada por día.
* Usuarios: Número de usuarios beneficiados.
* Inversión estimada [COP]: Monto de inversión en pesos colombianos.
* Empleos estimados: Empleos que se espera generar.
* Emisiones CO2 [Ton/año]: Emisiones anuales de CO2 reducidas.

"""

# Convertir la columna 'Fecha estimada FPO' a tipo datetime
dataset['Fecha estimada FPO'] = pd.to_datetime(dataset['Fecha estimada FPO'], errors='coerce')

# Verificar si hay duplicados

# Contar el número de filas duplicadas
num_duplicated_rows = dataset.duplicated().sum()

# Dataset con las filas duplicadas (si existen)

duplicated_rows = dataset[dataset.duplicated()]

# Verificar si hay valores nulos en el dataset
missing_values = dataset.isnull().sum()

"""
El análisis de la limpieza arrojó los siguientes resultados:

1. Duplicados: Se encontraron 2 filas duplicadas.
2. Valores nulos: Existen 3 valores nulos en la columna "Fecha estimada FPO" (Esos valores Nulos fueron reemplazados por fechas reales según información verificada)
"""

# Eliminar duplicados
df_clean = dataset.drop_duplicates()

# Reemplazar valores en filas con fechas nulas

df_clean.loc[:, "Fecha estimada FPO"] = df_clean["Fecha estimada FPO"].fillna(pd.Timestamp("2021-12-31"))

# Verificar si hay valores nulos en el dataset
missing_values_2 = df_clean.isnull().sum()

#----------------------------------------------------------------------------------------------------------

"""
Se creará la Base de Datos con las siguientes Tablas:

* Proyectos: Contendrá la información principal sobre cada proyecto.
* Ubicación: Contendrá los datos de ubicación (departamento y municipio) para normalizar los códigos.
* Energía y Emisiones: Almacenará la capacidad energética, emisiones de CO2 y datos relacionados.
"""

import mysql.connector

# Configuración de la conexión
config = {
    'user': 'root',  # Cambia esto por tu usuario de MySQL
    'password': '',  # Cambia por tu contraseña de MySQL
    'host': '127.0.0.1',  # Cambia por la IP o dominio de tu servidor
    'port': 3306,  # Puerto MySQL
    'raise_on_warnings': True
}

# Crear la conexión a MySQL
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Eliminar la base de datos si existe
try:
    cursor.execute("DROP DATABASE IF EXISTS energias_renovables")
    print("Base de datos eliminada (si existía).")
except mysql.connector.Error as err:
    print(f"Error al intentar eliminar la base de datos: {err}")

# Crear una base de datos
cursor.execute("CREATE DATABASE IF NOT EXISTS energias_renovables")
cursor.execute("USE energias_renovables")

# Verificar si la base de datos fue creada
cursor.execute("SHOW DATABASES")
for db in cursor:
    print(db)

# Crear la tabla de Ubicacion
cursor.execute("""
CREATE TABLE IF NOT EXISTS Ubicacion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    departamento VARCHAR(100),
    municipio VARCHAR(100),
    codigo_departamento INT,
    codigo_municipio INT
)
""")

# Crear la tabla de Proyectos
cursor.execute("""
CREATE TABLE IF NOT EXISTS Proyectos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    proyecto VARCHAR(255),
    tipo VARCHAR(50),
    capacidad FLOAT,
    fecha_fpo DATE,
    usuarios INT,
    inversion_cop BIGINT,
    empleos_estimados INT,
    ubicacion_id INT,
    FOREIGN KEY (ubicacion_id) REFERENCES Ubicacion(id)
)
""")

# Crear la tabla de Energía y Emisiones
cursor.execute("""
CREATE TABLE IF NOT EXISTS Energia_Emisiones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    proyecto_id INT,
    energia_kwh_dia BIGINT,
    emisiones_co2_ton FLOAT,
    capacidad_mw FLOAT,
    FOREIGN KEY (proyecto_id) REFERENCES Proyectos(id)
)
""")

# Confirmar los cambios
connection.commit()

# Cerrar la conexión
cursor.close()
connection.close()

print("Tablas creadas con éxito: Ubicacion, Proyectos y Energia_Emisiones")

#-------------------------------------------------------------------------------------------

"""
Insertar datos en la tabla Ubicación
"""

# Crear conexión y cursor
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Usar la base de datos
cursor.execute("USE energias_renovables")

# Insertar datos en la tabla Ubicacion (evitar duplicados)
for _, row in df_clean[['Departamento', 'Municipio', 'Código Departamento', 'Código Municipio']].drop_duplicates().iterrows():
    cursor.execute("""
    INSERT INTO Ubicacion (departamento, municipio, codigo_departamento, codigo_municipio)
    VALUES (%s, %s, %s, %s)
    """, (row['Departamento'], row['Municipio'], row['Código Departamento'], row['Código Municipio']))

# Confirmar los cambios
connection.commit()

# Cerrar la conexión
cursor.close()
connection.close()

print("Datos insertados en la tabla Ubicacion")

#----------------------------------------------------------------------------------------------------------

"""
Insertar datos en la tabla Proyectos
"""

# Crear conexión y cursor
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Usar la base de datos
cursor.execute("USE energias_renovables")

# Insertar datos en la tabla Proyectos
for _, row in df_clean.iterrows():
    # Obtener el ID de la Ubicacion relacionada
    cursor.execute("""
    SELECT id FROM Ubicacion 
    WHERE departamento = %s AND municipio = %s
    """, (row['Departamento'], row['Municipio']))
    ubicacion_id = cursor.fetchone()[0]
    
    # Insertar datos del proyecto
    fecha_fpo_str = row['Fecha estimada FPO'].strftime('%Y-%m-%d %H:%M:%S')  # Convert to string

    # Insert data with the converted string
    cursor.execute("""
    INSERT INTO Proyectos (proyecto, tipo, capacidad, fecha_fpo, usuarios, inversion_cop, empleos_estimados, ubicacion_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['Proyecto'], row['Tipo'], row['Capacidad'], fecha_fpo_str, row['Usuarios'], 
          row['Inversión estimada [COP]'], row['Empleos estimados'], ubicacion_id))

# Confirmar los cambios
connection.commit()

# Cerrar la conexión
cursor.close()
connection.close()

print("Datos insertados en la tabla Proyectos")

#----------------------------------------------------------------------------------------------------------

"""
Insertar datos en la tabla Energía y Emisiones
"""

# Crear conexión y cursor
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Usar la base de datos
cursor.execute("USE energias_renovables")

# Insertar datos en la tabla Energia_Emisiones
for _, row in df_clean.iterrows():
    # Obtener el ID del Proyecto relacionado
    cursor.execute("""
    SELECT id FROM Proyectos 
    WHERE proyecto = %s
    """, (row['Proyecto'],))
    proyecto_id = cursor.fetchone()[0]
    
    # Insertar datos de energía y emisiones
    cursor.execute("""
    INSERT INTO Energia_Emisiones (proyecto_id, energia_kwh_dia, emisiones_co2_ton, capacidad_mw)
    VALUES (%s, %s, %s, %s)
    """, (proyecto_id, row['Energía [kWh/día]'], row['Emisiones CO2 [Ton/año]'], row['Capacidad']))

# Confirmar los cambios
connection.commit()

# Cerrar la conexión
cursor.close()
connection.close()

print("Datos insertados en la tabla Energia_Emisiones")