# Databricks notebook source
# MAGIC %md
# MAGIC # Limpeza, Padronização e Enriquecimento de Dados de Estatísticas do Brasileirão
# MAGIC
# MAGIC ##### Este notebook processa os dados brutos da camada Bronze, aplicando transformações para limpar, padronizar e enriquecer as informações estatísticas por clube/partida do Campeonato Brasileiro, tornando-as mais adequadas para análise.
# MAGIC
# MAGIC **Objetivo:** Refinar os dados estatísticos para melhorar a qualidade e usabilidade.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial
# MAGIC ##### Define os caminhos para as camadas Bronze e Silver.

# COMMAND ----------

# Caminho para a camada Bronze
bronze_path = "dbfs:/user/hive/warehouse/brasileirao_estatisticas_data/bronze/"
bronze_table_name = "bronze_brasileirao_estatisticas_full"
bronze_input_path = f"{bronze_path}{bronze_table_name}/"

# Caminho para a camada Silver
silver_path = "dbfs:/user/hive/warehouse/brasileirao_estatisticas_data/silver/"
silver_table_name = "silver_brasileirao_estatisticas"
silver_output_path = f"{silver_path}{silver_table_name}/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura dos Dados da Camada Bronze
# MAGIC ##### Carrega os dados brutos de estatísticas do Brasileirão.

# COMMAND ----------

from pyspark.sql.functions import col, year, regexp_extract, trim

try:
    df_bronze = spark.read.format("delta").load(bronze_input_path)
    print(f"Dados da tabela {bronze_table_name} carregados com sucesso.")
    df_bronze.printSchema()
except Exception as e:
    print(f"Erro ao carregar a tabela Bronze '{bronze_table_name}': {e}")
    dbutils.notebook.exit("Erro: Não foi possível carregar os dados da camada Bronze. Verifique o notebook '01_Bronze_Layer.ipynb'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza, Padronização e Enriquecimento
# MAGIC
# MAGIC Adaptando as transformações para as colunas do arquivo `campeonato-brasileiro-estatisticas-full.csv`.
# MAGIC
# MAGIC **Colunas da Tabela:** `partida_id`, `rodata`, `clube`, `chutes`, `chutes_no_alvo`, `posse_de_bola`, `passes`, `precisao_passes`, `faltas`, `cartao_amarelo`, `cartao_vermelho`, `impedimentos`, `escanteios`.

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.functions import coalesce, regexp_replace, cast, col, lit

# Renomear colunas para snake_case
column_mapping = {
    'partida_id': 'id_partida',
    'rodata': 'rodada_texto', # Manter como texto para extração do ano, se necessário
    'clube': 'clube',
    'chutes': 'chutes',
    'chutes_no_alvo': 'chutes_no_alvo',
    'posse_de_bola': 'posse_de_bola',
    'passes': 'passes',
    'precisao_passes': 'precisao_passes',
    'faltas': 'faltas',
    'cartao_amarelo': 'cartao_amarelo',
    'cartao_vermelho': 'cartao_vermelho',
    'impedimentos': 'impedimentos',
    'escanteios': 'escanteios'
}

df_silver = df_bronze
for old_name, new_name in column_mapping.items():
    if old_name in df_silver.columns:
        df_silver = df_silver.withColumnRenamed(old_name, new_name)

# Selecionar e tipar colunas
# `posse_de_bola` e `precisao_passes` podem vir como string com '%' ou vazias
df_silver = df_silver.select(
    col("id_partida").cast(IntegerType()).alias("id_partida"),
    col("rodada_texto").cast(StringType()).alias("rodada_texto"), # Manter como string por enquanto
    col("clube").cast(StringType()).alias("clube"),
    
    # Limpeza e tipagem para colunas numéricas (preencher nulos com 0)
    coalesce(regexp_replace(col("chutes"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("chutes"),
    coalesce(regexp_replace(col("chutes_no_alvo"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("chutes_no_alvo"),
    
    # Para posse_de_bola e precisao_passes, remover '%' e converter para Double
    coalesce(regexp_replace(col("posse_de_bola"), "[%]", "").cast(DoubleType()), lit(0.0)).alias("posse_de_bola"),
    coalesce(regexp_replace(col("passes"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("passes"),
    coalesce(regexp_replace(col("precisao_passes"), "[%]", "").cast(DoubleType()), lit(0.0)).alias("precisao_passes"),
    
    coalesce(regexp_replace(col("faltas"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("faltas"),
    coalesce(regexp_replace(col("cartao_amarelo"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("cartao_amarelo"),
    coalesce(regexp_replace(col("cartao_vermelho"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("cartao_vermelho"),
    coalesce(regexp_replace(col("impedimentos"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("impedimentos"),
    coalesce(regexp_replace(col("escanteios"), "[^0-9]", "").cast(IntegerType()), lit(0)).alias("escanteios"),
    
    col("source_file").alias("origem_arquivo")
)

# Limpeza de strings (remover espaços extras dos nomes dos clubes)
df_silver = df_silver.withColumn("clube", trim(col("clube")))

# Enriquecimento de Dados
# Extrair o ano da temporada se 'rodada_texto' contiver o ano (ex: "2023-1", "2023-38")
# Se a coluna 'rodata' for apenas o número da rodada (ex: '1', '38'), não é possível extrair o ano.
# Assumindo que o dataset é de uma única temporada, ou que o ano está implícito.
# Se necessário, você pode adicionar uma coluna de ano manualmente aqui:
# df_silver = df_silver.withColumn("ano_temporada", lit(2023)) # Exemplo: se soubermos que é 2023

# Tentativa de extrair ano da rodada, se no formato 'YYYY-Rodada'
# Caso contrário, definir um ano padrão ou extrair da `data_jogo` se o arquivo tivesse essa coluna.
# Pelo snippet, 'rodata' é apenas o número. Vou adicionar um ano default e uma 'rodada_numero'.
df_silver = df_silver.withColumn("ano_temporada", lit(2023)) # DEFININDO ANO DA TEMPORADA COMO 2023 POR PADRÃO
df_silver = df_silver.withColumn("rodada_numero", col("rodada_texto").cast(IntegerType()))

# Salva o DataFrame processado como uma tabela Delta Lake na camada Silver
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_output_path)
    
print(f"Dados de estatísticas do Brasileirão salvos com sucesso na camada Silver em: {silver_output_path}")

# Cria uma tabela no metastore para facilitar a consulta SQL
spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_table_name} USING DELTA LOCATION '{silver_output_path}'")
print(f"Tabela '{silver_table_name}' criada/atualizada no metastore.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificação da Camada Silver
# MAGIC ##### Mostra algumas linhas da tabela criada na camada Silver para verificação.

# COMMAND ----------

try:
    df_silver_sample = spark.read.format("delta").load(silver_output_path)
    print(f"Amostra de dados da tabela {silver_table_name}:")
    df_silver_sample.limit(5).display()
    df_silver_sample.printSchema()
except Exception as e:
    print(f"Não foi possível ler a tabela {silver_table_name}. Verifique se a tabela foi criada. Erro: {e}")