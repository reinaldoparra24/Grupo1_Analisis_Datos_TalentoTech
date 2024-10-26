import pandas as pd
from pandas import *

dataset = pd.read_csv("Fuentes_No_Convencionales_de_Energ_a_Renovable.csv")

# Convertir la columna 'Fecha estimada FPO' a tipo datetime
dataset['Fecha estimada FPO'] = pd.to_datetime(dataset['Fecha estimada FPO'], errors='coerce')

# Contar el número de filas duplicadas
num_duplicated_rows = dataset.duplicated().sum()

# Dataset con las filas duplicadas (si existen)

duplicated_rows = dataset[dataset.duplicated()]

# Verificar si hay valores nulos en el dataset
missing_values = dataset.isnull().sum()

# Eliminar duplicados
df_clean = dataset.drop_duplicates()

# Reemplazar valores en filas con fechas nulas

df_clean.loc[:, "Fecha estimada FPO"] = df_clean["Fecha estimada FPO"].fillna(pd.Timestamp("2021-12-31"))

# Verificar si hay valores nulos en el dataset
missing_values_2 = df_clean.isnull().sum()

#---------------------------------Crear la base de datos SQLite desde el archivo SQL -------------------------------------------------------------------------

import sqlite3

# Crear la conexión a la base de datos SQLite
conn = sqlite3.connect('energias_renovables.sqlite')
cursor = conn.cursor()

# Cerrar la conexión
conn.commit()
conn.close()

# Consultas con Sqlite3

# Conexión a la base de datos SQLite
connection = sqlite3.connect('energias_renovables.sqlite')

#---------------------------------CONSULTAS-------------------------------------------------------------------------

# Proyectos con mayor capacidad instalada mayor a 50 MW y sus ubicaciones

query = """
SELECT 
    p.proyecto, 
    p.capacidad, 
    u.departamento, 
    u.municipio
FROM 
    Proyectos p
JOIN 
    Ubicacion u ON p.ubicacion_id = u.id
WHERE 
    p.capacidad > 50
ORDER BY p.capacidad DESC;
"""
df_capacidadinstalada = pd.read_sql(query, con=connection)

# Mostrar los primeros registros del DataFrame
#df_capacidadinstalada.head()

#----------------------------------------------------------------------------------------------------------

# Agrupa los proyectos por departamento, calcula la capacidad total instalada en cada uno

query = """
SELECT 
    u.departamento, 
    SUM(p.capacidad) AS capacidad_total
FROM 
    Proyectos p
JOIN 
    Ubicacion u ON p.ubicacion_id = u.id
GROUP BY 
    u.departamento
ORDER BY 
    capacidad_total DESC;
"""

df_departamentos = pd.read_sql(query, con=connection)

# Análisis adicional: mostrar los departamentos con capacidad mayor a 200 MW
#df_departamentos[df_departamentos['capacidad_total'] > 200]

#----------------------------------------------------------------------------------------------------------

# Proyectos que tienen una mayor generación de energía y calcula su impacto en la reducción de emisiones de CO₂

query = """
SELECT 
    p.proyecto, 
    e.energia_kwh_dia, 
    e.emisiones_co2_ton
FROM 
    Proyectos p
JOIN 
    Energia_Emisiones e ON p.id = e.proyecto_id
WHERE 
    e.energia_kwh_dia > 100000
ORDER BY 
    e.energia_kwh_dia DESC;
"""

df_energia = pd.read_sql(query, con=connection)

# Análisis adicional: calcular la relación entre la energía generada y las emisiones evitadas
df_energia['ratio_energia_emisiones'] = df_energia['energia_kwh_dia'] / df_energia['emisiones_co2_ton']
#df_energia.sort_values(by='ratio_energia_emisiones', ascending=False).head()

#----------------------------------------------------------------------------------------------------------

# Inversión total en proyectos por municipio y departamento

query = """
SELECT 
    u.departamento, 
    u.municipio, 
    SUM(p.inversion_cop) AS inversion_total
FROM 
    Proyectos p
JOIN 
    Ubicacion u ON p.ubicacion_id = u.id
GROUP BY 
    u.departamento, u.municipio
ORDER BY 
    inversion_total DESC;
"""

df_inversion = pd.read_sql(query, con=connection)

# Análisis adicional: mostrar los municipios con una inversión total mayor a 1 billón de COP
#df_inversion[df_inversion['inversion_total'] > 1e12]

df_inversion_filtrado = df_inversion[df_inversion['inversion_total'] > 1e12]

#----------------------------------------------------------------------------------------------------------

# Proyectos por tipo de energía y calcula cuántos empleos se han generado en cada uno.

query = """
SELECT 
    p.tipo, 
    COUNT(p.id) AS numero_proyectos, 
    SUM(p.empleos_estimados) AS empleos_total
FROM 
    Proyectos p
GROUP BY 
    p.tipo
ORDER BY 
    empleos_total DESC;
"""

df_empleos = pd.read_sql(query, con=connection)

# Análisis adicional: filtrar los tipos de energía con más de 5000 empleos generados
#df_empleos[df_empleos['empleos_total'] > 3000]

#----------------------------------------------------------------------------------------------------------

# Eficiencia de los proyectos en términos de energía generada por cada peso invertido
query = """
SELECT 
    p.proyecto, 
    e.energia_kwh_dia, 
    p.inversion_cop, 
    (e.energia_kwh_dia / p.inversion_cop) AS energia_por_peso
FROM 
    Proyectos p
JOIN 
    Energia_Emisiones e ON p.id = e.proyecto_id
ORDER BY 
    energia_por_peso DESC
LIMIT 20;
"""

df_eficiencia = pd.read_sql(query, con=connection)

# Análisis adicional: mostrar los 5 proyectos más eficientes
#df_eficiencia.nlargest(5, 'energia_por_peso')

#----------------------------------------------------------------------------------------------------------

# Calcula el crecimiento anual de la capacidad instalada en todos los proyectos

query = """
SELECT 
    strftime('%Y', p.fecha_fpo) AS anio, 
    SUM(p.capacidad) AS capacidad_total_anual
FROM 
    Proyectos p
GROUP BY 
    anio
ORDER BY 
    anio;
"""

df_crecimiento = pd.read_sql(query, con=connection)

# Análisis adicional: mostrar el crecimiento de capacidad en los últimos 5 años
#df_crecimiento.tail(5)

#----------------------------------------------------------------------------------------------------------

#Ubicación de Proyectos en Colombia

# Cargar el archivo CSV
df_municipios = pd.read_csv('Municipios.csv',delimiter=";")
df_municipios = df_municipios.drop('Unnamed: 4', axis=1)

# Realizamos el merge basado en las columnas 'Departamento' y 'Municipio'
df_merged = pd.merge(df_clean, df_municipios, on=['Departamento', 'Municipio'], how='left')

# Verificar si hay valores nulos en el dataset
missing_values = df_merged.isnull().sum()

# Mostrar los resultados
#print("Valores nulos en el dataset:")
#print(missing_values)

import pandas as pd

# Organizando el dataset a columnas de interés
df_ubicacion = df_merged[['Proyecto', 'Departamento', 'Municipio', 'latitud', 'longitud', 'Capacidad']]

# Nuevo orden de columnas
nuevo_orden = ['Proyecto', 'Capacidad', 'Departamento', 'Municipio', 'latitud', 'longitud' ]

# Reordenar el DataFrame
df_ubicacion = df_ubicacion[nuevo_orden]

# Convertir la columna 'latitud' y 'longitud' a float, reemplazando comas por puntos

df_ubicacion.loc[:, 'latitud'] = df_ubicacion['latitud'].str.replace(',', '.').astype(float)
df_ubicacion.loc[:, 'longitud'] = df_ubicacion['longitud'].str.replace(',', '.').astype(float)

#-------------------------------------------Creación del Dashboard--------------------------------------------------------------

import streamlit as st 
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# Título del Dashboard
st.title('Dashboard de Energía Renovable en Colombia')

# Crear tabs para organizar los gráficos
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Datos","Gráficos de Líneas", "Gráficos de Barras", "Mapas", "Gráficos Circulares", "Otros Gráficos"])

# Botón para actualizar los gráficos
if st.button('Actualizar Gráficos'):
    st.write('Gráficos actualizados!')

# Tab 1: Datos
with tab1:

    st.header("Datos Representativos")

    df_merged2 = df_merged.drop(columns=["Código Municipio", "Código Departamento"])

    ver_df = st.toggle('Ver DataFrame', value=True)
    if ver_df:
        st.write(df_merged2)
    
    # Calcular las métricas
    capacidad_total = df_merged2['Capacidad'].sum()
    energia_total = df_merged2['Energía [kWh/día]'].sum()
    usuarios_total = df_merged2['Usuarios'].sum()
    inversion_total = df_merged2['Inversión estimada [COP]'].sum()
    empleos_total = df_merged2['Empleos estimados'].sum()
    emisiones_total = df_merged2['Emisiones CO2 [Ton/año]'].sum()

    # Crear columnas para mostrar las métricas
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)

    # Mostrar las métricas usando st.metric
    col1.metric("Capacidad Total Instalada (MW)", f"{capacidad_total:.2f}")
    col2.metric("Energía Total Generada por Día (kWh)", f"{energia_total:,.0f}")
    col3.metric("Total de Usuarios Beneficiados", f"{usuarios_total:,}")

    # Convertir a millones para una mejor presentación
    inversion_en_millones = inversion_total / 1_000_000

    col4.metric( label="Inversión Total Estimada (COP)", 
    value=f"${inversion_en_millones:,.2f} M COP",  
    delta=f"${inversion_total:,.0f} COP")

    col5.metric("Empleos Generados", f"{empleos_total:,}")
    col6.metric("Emisiones de CO2 Evitadas (Ton/año)", f"{emisiones_total:,.0f}")

# Tab 2: Gráficos de Líneas
with tab2:
    st.header("Gráficos de Líneas")

    # Slider para seleccionar un rango de años en el gráfico de crecimiento
    anio_seleccionado = st.slider('Seleccionar Rango de Años', 
                                  min_value=int(df_crecimiento['anio'].min()), 
                                  max_value=int(df_crecimiento['anio'].max()), 
                                  value=(2010, 2023))
    df_crecimiento['anio'] = df_crecimiento['anio'].astype(int)
    df_crecimiento_filtrado = df_crecimiento[(df_crecimiento['anio'] >= anio_seleccionado[0]) & (df_crecimiento['anio'] <= anio_seleccionado[1])]

    # Gráfico de crecimiento anual de la capacidad filtrado por años
    fig6 = go.Figure(data=go.Scatter(x=df_crecimiento_filtrado['anio'], 
                                       y=df_crecimiento_filtrado['capacidad_total_anual'],
                                       mode='lines+markers', name='Crecimiento Anual de la Capacidad'))
    fig6.update_layout(title='Crecimiento Anual de la Capacidad Total (Filtrado)')
    st.plotly_chart(fig6)

# Tab 3: Gráficos de Barras
with tab3:
    st.header("Gráficos de Barras")

    # Multiselect para elegir múltiples departamentos en gráfico de capacidad por departamento
    departamentos_seleccionados = st.multiselect('Seleccionar Departamentos', 
                                                 df_departamentos['departamento'].unique(),
                                                 default=df_departamentos['departamento'].unique())

    # Filtrar los datos por los departamentos seleccionados
    df_departamento_filtrado = df_departamentos[df_departamentos['departamento'].isin(departamentos_seleccionados)]

    # Gráfico de capacidad por departamento filtrado
    fig1 = px.bar(df_departamento_filtrado, x='departamento', y='capacidad_total',
                   title=f'Capacidad Total por Departamentos Seleccionados')
    st.plotly_chart(fig1)

    # Color Picker para personalizar el color del gráfico de inversión
    color_elegido = st.color_picker('Seleccionar color para el gráfico de inversión', '#00f900')

    # Gráfico de distribución de capacidad por municipio, personalizado por color
    fig9 = px.bar(df_inversion_filtrado,
                 x='municipio', 
                 y='inversion_total',
                 color='departamento',
                 title='Inversión Total en Proyectos por Municipio',
                 labels={'inversion_total': 'Inversión Total (COP)', 'municipio': 'Municipio'},
                 text='inversion_total',
                 color_discrete_sequence=px.colors.qualitative.Set2)

    # Personalizar el diseño
    fig9.update_traces(texttemplate='%{text:.3s}', textposition='outside', cliponaxis=False, width=0.3, marker_color=color_elegido)
    fig9.update_layout(xaxis_title='Municipios', yaxis_title='Inversión Total (COP)',
                      xaxis_tickangle=-90, barmode='group', xaxis=dict(automargin=True), bargap=1)
    st.plotly_chart(fig9)

# Tab 4: Mapas
with tab4:
    st.header("Mapas")

    # Mapa de ubicación de proyectos en Colombia
    fig2 = px.scatter_geo(df_ubicacion, lat='latitud', lon='longitud',
                          hover_name='Municipio', hover_data=['Departamento', 'Capacidad'],
                          size='Capacidad', color='Departamento',
                          title='Ubicación de Proyectos en Colombia').update_geos(
                              scope='south america',
                              lataxis_range=[-5, 15],
                              lonaxis_range=[-80, -65],
                              showland=True, landcolor="LightGray",
                              showcountries=True, countrycolor="Black",
                              showsubunits=True, subunitcolor="Blue")
    st.plotly_chart(fig2)

# Tab 5: Gráficos Circulares
with tab5:
    st.header("Gráficos Circulares")

    # Obtener los valores únicos de la columna 'tipo' para el select_slider
    tipos_disponibles = sorted(df_empleos['tipo'].unique())  # Asegurarse de que los tipos estén ordenados

    # Usar un select_slider para seleccionar un rango de tipos de energía
    tipos_seleccionados = st.select_slider('Seleccionar Rango de Tipos de Energía', 
                                           options=tipos_disponibles, 
                                           value=(tipos_disponibles[0], tipos_disponibles[-1]))

    # Filtrar los datos de empleos por el rango de tipos seleccionados
    df_empleos_filtrado = df_empleos[df_empleos['tipo'].between(*tipos_seleccionados)]

    # Gráfico de distribución de empleos generados filtrado
    fig4 = px.pie(df_empleos_filtrado, names='tipo', values='empleos_total',
                   title=f'Distribución de Empleos Generados para {tipos_seleccionados[0]} a {tipos_seleccionados[1]}')
    st.plotly_chart(fig4)

    # Filtrar los datos de capacidad por tecnología según los tipos seleccionados
    df_capacidad_filtrado = df_clean[df_clean['Tipo'].between(*tipos_seleccionados)]

    # Gráfico de distribución de capacidad por tecnología filtrado
    fig8 = px.pie(df_capacidad_filtrado, names='Tipo', values='Capacidad',
                   title=f'Distribución de Capacidad por Tecnología para {tipos_seleccionados[0]} a {tipos_seleccionados[1]}')
    st.plotly_chart(fig8)

    # Gráfico Sunburst para capacidad por tecnología y proyecto filtrado
    fig7 = px.sunburst(df_capacidad_filtrado, path=['Tipo', 'Proyecto'], values='Capacidad',
                       title=f'Capacidad por Tecnología y Proyecto para {tipos_seleccionados[0]} a {tipos_seleccionados[1]}')
    st.plotly_chart(fig7)

# Tab 6: Otros Gráficos
with tab6:
    st.header("Otros Gráficos")

    # Multiselect para seleccionar múltiples proyectos en gráfico de energía vs emisiones
    proyectos_seleccionados = st.multiselect('Seleccionar Proyectos', df_energia['proyecto'].unique(), 
                                             default=df_energia['proyecto'].unique())

    # Filtrar los datos de energía por los proyectos seleccionados
    df_energia_filtrado = df_energia[df_energia['proyecto'].isin(proyectos_seleccionados)]

    # Gráfico de energía vs emisiones filtrado por proyectos
    fig3 = px.scatter(df_energia_filtrado, x='energia_kwh_dia', y='emisiones_co2_ton', color='proyecto',
                      size='energia_kwh_dia', title='Energía Generada vs. Emisiones Evitadas por Proyecto')
    st.plotly_chart(fig3)

    # Gráfico de eficiencia energética por proyecto
    fig5 = px.scatter(df_eficiencia, x='proyecto', y='energia_por_peso',
                     size='energia_kwh_dia', color='proyecto', 
                     title='Eficiencia Energética por Proyecto (kWh / COP)',
                     labels={'energia_por_peso': 'Eficiencia Energética (kWh/COP)', 'proyecto': 'Proyecto'})
    st.plotly_chart(fig5)
