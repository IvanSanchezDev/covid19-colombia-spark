# Análisis batch de casos de COVID-19 en Colombia con Apache Spark

Proyecto del curso Big Data (UNAD). Procesa con PySpark más de 6,3 millones de casos
positivos de COVID-19 en Colombia: carga, limpieza, análisis exploratorio y gráficas.

## Problema

Los casos de COVID-19 se reportaron individualmente, con edad, sexo, departamento, fechas y
desenlace. El volumen hace costoso analizarlos con herramientas tradicionales. Este proyecto
responde con procesamiento distribuido:

- ¿Cómo evolucionaron los casos en el tiempo y cómo se distribuyen por departamento?
- ¿Cómo cambia la letalidad según la edad y el sexo?
- ¿Cuánto tardaba el reporte desde el inicio de síntomas?

## Fuente de datos

"Casos positivos de COVID-19 en Colombia", Instituto Nacional de Salud, Portal de Datos
Abiertos de Colombia (datos.gov.co), ID `gt2j-8ykr`. Se descargó el 17 de octubre de 2026.

https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr

## Qué hace `covid_batch.py`

1. **Carga** el CSV con Spark (DataFrames) y normaliza los nombres de columnas.
2. **Diagnóstico previo**: conteos de `estado` y `recuperado`, nulos frente a texto `N/A`, y
   rango de edades por unidad de medida.
3. **Limpieza**: unifica mayúsculas (`Leve`, `leve`, `LEVE`), convierte `N/A` en nulos,
   elimina duplicados por ID, convierte la edad a años según su unidad (años, meses, días),
   descarta edades fuera de 0 a 120 y convierte las fechas.
4. **EDA**: casos por mes, top de departamentos, letalidad por grupo de edad y por sexo, y
   retraso entre síntomas y reporte.
5. **Visualización**: gráficas y tablas CSV en `output/`.

## Resultados principales

- 6.390.971 casos únicos analizados.
- Problemas de calidad corregidos: `estado` con tres escrituras de la misma categoría,
  41.268 valores `N/A` en `estado` y 36.327 en `recuperado` (texto, no nulos), y edades con
  unidades distintas.
- Letalidad sobre casos confirmados: 0,10 % en menores de 10 años y 22,25 % en mayores de 80.
- Letalidad por sexo: 2,92 % en hombres y 1,65 % en mujeres.
- Mediana de 10 días entre inicio de síntomas y reporte (percentil 90: 20 días).

La letalidad se calcula sobre casos confirmados, no sobre infecciones reales; depende de la
cantidad de pruebas realizadas, por lo que no debe leerse como riesgo real de morir.

## Cómo ejecutarlo

Requisitos: Python 3.10 o superior, Java 11 o 17 y unos 6 GB de RAM libres.

```bash
git clone <URL-DEL-REPOSITORIO>
cd <carpeta-del-repositorio>

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

mkdir -p data
wget -O data/covid_casos.csv "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"

python covid_batch.py
```

La descarga pesa cerca de 1,2 GB. Si el ajuste de memoria hace falta:
`spark-submit --driver-memory 4g covid_batch.py`.



