import os, re, unicodedata
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

BASE = os.path.dirname(os.path.abspath(__file__))
RUTA = os.path.join(BASE, "data", "covid_casos.csv")
SALIDA = os.path.join(BASE, "output")
os.makedirs(SALIDA, exist_ok=True)

spark = (SparkSession.builder.appName("CovidBatch")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- 1. CARGA ----------
def norm(n):
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", n.lower()).strip("_")

raw = spark.read.option("header", True).csv(RUTA)
raw = raw.toDF(*[norm(c) for c in raw.columns])
print("Columnas del archivo:", raw.columns)

def pick(*cands):
    for c in cands:
        if c in raw.columns:
            return c
    raise SystemExit(f"No encuentro ninguna de {cands}. Columnas: {raw.columns}")

df = raw.select(
    F.col(pick("id_de_caso")).alias("id_caso"),
    F.col(pick("fecha_reporte_web")).alias("f_reporte"),
    F.col(pick("fecha_inicio_sintomas", "fecha_de_inicio_de_sintomas")).alias("f_sintomas"),
    F.col(pick("departamento_nom", "nombre_departamento")).alias("departamento"),
    F.col(pick("edad")).alias("edad"),
    F.col(pick("unidad_medida", "unidad_de_medida_de_edad")).alias("unidad_medida"),
    F.col(pick("sexo")).alias("sexo"),
    F.col(pick("estado")).alias("estado"),
    F.col(pick("recuperado")).alias("recuperado"),
)
print("Filas cargadas:", df.count())

# ---------- 2. ANTES DE LIMPIAR ----------
print("\n=== ANTES: valores de estado ===")
df.groupBy("estado").count().orderBy(F.desc("count")).show(truncate=False)
print("=== ANTES: valores de recuperado ===")
df.groupBy("recuperado").count().orderBy(F.desc("count")).show(truncate=False)

cols = ["estado", "recuperado", "sexo", "edad", "departamento"]
fila = df.select(
    *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c + "_null") for c in cols],
    *[F.sum(F.when(F.upper(F.trim(F.col(c))) == "N/A", 1).otherwise(0)).alias(c + "_na") for c in cols],
).first().asDict()
print("=== ANTES: nulos reales vs texto 'N/A' ===")
for c in cols:
    print(f"{c:13s} nulos={fila[c + '_null']:>9}   'N/A'={fila[c + '_na']:>9}")

print("\n=== ANTES: edad por unidad_medida (verifica 1=años, 2=meses, 3=días) ===")
(df.withColumn("e", F.col("edad").cast("int"))
   .groupBy("unidad_medida")
   .agg(F.count("*").alias("casos"), F.min("e").alias("edad_min"), F.max("e").alias("edad_max"))
   .show())

# ---------- 3. LIMPIEZA ----------
def txt(c):
    x = F.lower(F.trim(F.col(c)))
    return F.when(x.isin("n/a", "na", ""), None).otherwise(x)

def a_fecha(c):
    return F.to_date(F.coalesce(
        F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(c), "MM/dd/yyyy hh:mm:ss a"),
        F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss.SSS")))

u = F.col("unidad_medida").cast("int")
e = F.col("edad").cast("double")
limpio = (df.dropDuplicates(["id_caso"])
    .withColumn("estado", txt("estado"))
    .withColumn("recuperado", txt("recuperado"))
    .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
    .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
    .withColumn("edad_anios", F.when(u == 1, e).when(u == 2, e / 12).when(u == 3, e / 365))
    .withColumn("edad_anios", F.when((F.col("edad_anios") >= 0) & (F.col("edad_anios") <= 120), F.col("edad_anios")))
    .withColumn("f_reporte", a_fecha("f_reporte"))
    .withColumn("f_sintomas", a_fecha("f_sintomas"))
    .withColumn("fallecido", F.when((F.col("estado") == "fallecido") | (F.col("recuperado") == "fallecido"), 1).otherwise(0))
    .drop("edad", "unidad_medida"))
limpio.cache()
total = limpio.count()

print(f"\n=== DESPUÉS DE LIMPIAR ({total} casos únicos) ===")
limpio.groupBy("estado").count().orderBy(F.desc("count")).show(truncate=False)
limpio.groupBy("recuperado").count().orderBy(F.desc("count")).show(truncate=False)
limpio.select(F.round(F.avg("edad_anios"), 1).alias("edad_prom"),
              F.round(F.min("edad_anios"), 2).alias("min"),
              F.round(F.max("edad_anios"), 1).alias("max")).show()

# ---------- 4. EDA ----------
def guardar(nombre):
    plt.tight_layout()
    plt.savefig(f"{SALIDA}/{nombre}.png", dpi=130)
    plt.close()

# 4.1 Casos por mes (olas)
mes = (limpio.filter(F.col("f_reporte").isNotNull())
       .groupBy(F.date_format("f_reporte", "yyyy-MM").alias("mes"))
       .agg(F.count("*").alias("casos")).orderBy("mes").toPandas())
mes.to_csv(f"{SALIDA}/casos_por_mes.csv", index=False)
plt.figure(figsize=(11, 4))
plt.plot(mes["mes"], mes["casos"], marker="o", markersize=3)
plt.xticks(mes["mes"][::3], rotation=60)
plt.title("Casos de COVID-19 reportados por mes"); plt.ylabel("Casos")
guardar("1_casos_por_mes")

# 4.2 Casos y letalidad por departamento
dep = (limpio.groupBy("departamento")
       .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
       .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
       .orderBy(F.desc("casos")))
print("=== Top 15 departamentos por casos ===")
dep.show(15, truncate=False)
d = dep.limit(15).toPandas().iloc[::-1]
d.to_csv(f"{SALIDA}/top_departamentos.csv", index=False)
plt.figure(figsize=(9, 6))
plt.barh(d["departamento"], d["casos"])
plt.title("Top 15 departamentos por número de casos"); plt.xlabel("Casos")
guardar("2_top_departamentos")

# 4.3 Letalidad por grupo de edad
dec = F.least(F.floor(F.col("edad_anios") / 10), F.lit(8))
ed = (limpio.filter(F.col("edad_anios").isNotNull())
      .groupBy(dec.alias("dec"))
      .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
      .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
      .orderBy("dec").toPandas())
ed["grupo"] = ed["dec"].apply(lambda x: "80+" if x >= 8 else f"{int(x)*10}-{int(x)*10+9}")
print("=== Letalidad por grupo de edad ===")
print(ed[["grupo", "casos", "fallecidos", "letalidad_pct"]].to_string(index=False))
ed.to_csv(f"{SALIDA}/letalidad_por_edad.csv", index=False)
plt.figure(figsize=(9, 4))
plt.bar(ed["grupo"], ed["letalidad_pct"])
plt.title("Letalidad (%) por grupo de edad"); plt.ylabel("% de casos que fallecieron")
guardar("3_letalidad_por_edad")

# 4.4 Sexo
sx = (limpio.filter(F.col("sexo").isin("F", "M"))
      .groupBy("sexo").agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
      .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2)))
print("=== Por sexo ===")
sx.show()

# 4.5 Retraso entre inicio de síntomas y reporte
ret = (limpio.filter(F.col("f_sintomas").isNotNull() & F.col("f_reporte").isNotNull())
       .withColumn("retraso", F.datediff("f_reporte", "f_sintomas"))
       .filter((F.col("retraso") >= 0) & (F.col("retraso") <= 60)))
q = ret.approxQuantile("retraso", [0.5, 0.9], 0.01)
print(f"=== Retraso síntomas→reporte: mediana {q[0]} días, percentil 90 {q[1]} días ===")
h = ret.groupBy("retraso").count().orderBy("retraso").toPandas()
plt.figure(figsize=(9, 4))
plt.bar(h["retraso"], h["count"])
plt.title("Días entre inicio de síntomas y reporte"); plt.xlabel("Días"); plt.ylabel("Casos")
guardar("4_retraso_reporte")

print(f"\nListo. Gráficas y CSV en: {SALIDA}")
spark.stop()
