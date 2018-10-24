#Code here

>>> spark = SparkSession.builder.getOrCreate()
>>> escuelasdf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/escuelasPR.csv")
>>> estudiantesdf = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/geraldo_vera/estudiantesPR.csv")
>>> escuelasdf.join(estudiantesdf, 'IdEscuela').show()
>>> joindf = estudiantesdf.join(escuelasdf.select(escuelasdf.Ciudad, escuelasdf.IdEscuela), 'IdEscuela').
>>> resulta = joindf.filter((joindf.Sexo == 'M') & (joindf.Nivel == 'SPR') & ((joindf.Ciudad == 'Ponce') | (joindf.Ciudad == 'San Juan')))
