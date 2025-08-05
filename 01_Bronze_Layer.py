# Databricks notebook source
# MAGIC %md
# MAGIC #Ingestão de Dados Brutos de Estatísticas do Brasileirão
# MAGIC
# MAGIC Este notebook é responsável por ler o arquivo CSV bruto de estatísticas por clube/partida do Campeonato Brasileiro e persistí-lo como uma tabela Delta Lake na camada Bronze.
# MAGIC
# MAGIC **Fonte dos Dados:** Arquivo `campeonato-brasileiro-estatisticas-full.csv` fornecido pelo usuário.
# MAGIC
# MAGIC **Objetivo:** Ingerir os dados "as-is" (como estão), sem grandes transformações, garantindo a fidelidade dos dados de origem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial
# MAGIC ##### Define o caminho para o arquivo CSV de origem e para a camada Bronze.

# COMMAND ----------

# Caminho completo para o arquivo CSV de estatísticas do Brasileirão
csv_file_path = "/FileStore/dados/soccer/campeonato_brasileiro_estatisticas_full.csv" 

# Caminho para a camada Bronze no DBFS
bronze_path = "dbfs:/user/hive/warehouse/brasileirao_estatisticas_data/bronze/"

# Nome da tabela Bronze
bronze_table_name = "bronze_brasileirao_estatisticas_full"
output_path = f"{bronze_path}{bronze_table_name}/"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestão e Persistência na Camada Bronze
# MAGIC ###### Lê o arquivo CSV, adiciona uma coluna para identificar a origem do arquivo e salva em formato Delta Lake.

# COMMAND ----------

from pyspark.sql.functions import input_file_name
import os

print(f"Processando arquivo: {csv_file_path}")

try:
    # Lê o arquivo CSV com cabeçalho e inferência de esquema
    # Use delimiter=',' se seu CSV usa vírgula como separador (padrão para o arquivo fornecido)
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .load(csv_file_path)
    
    # Adiciona uma coluna 'source_file' para rastrear a origem
    df = df.withColumn("source_file", input_file_name())
    
    # Salva o DataFrame como uma tabela Delta Lake
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(output_path)
        
    print(f"Dados de estatísticas do Brasileirão salvos com sucesso na camada Bronze em: {output_path}")
    
    # Cria uma tabela no metastore para facilitar a consulta SQL
    spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table_name} USING DELTA LOCATION '{output_path}'")
    print(f"Tabela '{bronze_table_name}' criada/atualizada no metastore.")

except Exception as e:
    print(f"Erro ao processar o arquivo {csv_file_path}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificação da Camada Bronze
# MAGIC ##### Mostra algumas linhas da tabela criada na camada Bronze para verificação.

# COMMAND ----------

try:
    df_bronze_sample = spark.read.format("delta").load(output_path)
    print(f"Amostra de dados da tabela {bronze_table_name}:")
    df_bronze_sample.limit(5).display()
    df_bronze_sample.printSchema()
except Exception as e:
    print(f"Não foi possível ler a tabela {bronze_table_name}. Erro: {e}")