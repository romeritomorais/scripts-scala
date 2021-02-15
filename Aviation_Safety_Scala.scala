// agora carregamos em Dataframe o csv selecionado pelo metodo do object `datasets`
val pathfile = "/home/romerito/Dropbox/tecnology/bigdata_analytics/projects/python3/Aviation/data/dataset_aviationsafety.csv"
val aviation_df = spark.read.option("inferSchema", true)
                           .option("header", "true")
                           .csv(pathfile)

val rename_df = aviation_df.withColumnRenamed("acc. date", "data")
                           .withColumnRenamed("reg.", "registro")
                           .withColumnRenamed("fat.", "vitimas")
                           .drop("_c6", "dmg")

val null_df = rename_df.na.drop()

null_df.createOrReplaceTempView("Aviation")

val clean_df = spark.sql(
"""
SELECT LOWER(REPLACE(data, '-', ''))     AS data,
       LTRIM(LOWER(type))                AS tipo,
       registro,
       LTRIM(LOWER(operator))            AS operador,
       COALESCE(CAST(vitimas AS INT), 0) AS vitimas,
       LOWER(location)                   AS localizacao
FROM   aviation 
""").na.drop()

val csvexportspark = "/home/romerito/Dropbox/tecnology/bigdata_analytics/projects/python3/Aviation/"
clean_df.write.option("header","true").option("sep",";").mode("overwrite").csv("csvexportspark")


