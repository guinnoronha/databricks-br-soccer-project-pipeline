# Databricks notebook source
# MAGIC %md
# MAGIC # Agregação e Modelagem de Dados de Estatísticas do Brasileirão para Análise
# MAGIC
# MAGIC Este notebook agrega e modela os dados refinados da camada Silver para criar tabelas prontas para consumo em análises e dashboards, focando nas estatísticas de jogo por clube.
# MAGIC
# MAGIC **Objetivo:** Fornecer dados de alta qualidade e sumarizados para consumo direto, baseados nas estatísticas de desempenho.
# MAGIC
# MAGIC **Importante:** Este arquivo de dados (`campeonato-brasileiro-estatisticas-full.csv`) contém apenas estatísticas por clube/partida e não inclui informações de placar final, vencedor, público ou renda. Portanto, as agregações da camada Gold foram adaptadas para o tipo de dado disponível.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial
# MAGIC ##### Define os caminhos para as camadas Silver e Gold.

# COMMAND ----------

# Caminho para a camada Silver
silver_path = "dbfs:/user/hive/warehouse/brasileirao_estatisticas_data/silver/"
silver_table_name = "silver_brasileirao_estatisticas"
silver_input_path = f"{silver_path}{silver_table_name}/"

# Caminho para a camada Gold
gold_path = "dbfs:/user/hive/warehouse/brasileirao_estatisticas_data/gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura dos Dados da Camada Silver
# MAGIC ##### Carrega a tabela principal da camada Silver.

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, avg, round

try:
    df_silver_estatisticas = spark.read.format("delta").load(silver_input_path)
    print(f"Dados da tabela {silver_table_name} carregados com sucesso.")
    df_silver_estatisticas.cache() # Cache para otimizar operações subsequentes
except Exception as e:
    print(f"Erro ao carregar a tabela Silver '{silver_table_name}': {e}")
    dbutils.notebook.exit("Erro: Não foi possível carregar os dados da camada Silver. Verifique o notebook '02_Silver_Layer.ipynb'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agregações e Criação de Tabelas Gold
# MAGIC
# MAGIC Vamos criar algumas tabelas analíticas baseadas nas estatísticas:
# MAGIC 1.  **`gold_clube_estatisticas_temporada`**: Média de estatísticas (chutes, posse de bola, cartões, etc.) por clube por temporada.
# MAGIC 2.  **`gold_resumo_estatisticas_rodada`**: Média de estatísticas agregadas por rodada.
# MAGIC 3.  **`gold_clube_cartoes_totais`**: Total de cartões amarelos e vermelhos por clube por temporada.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1. Criar Tabela: `gold_clube_estatisticas_temporada`
# MAGIC #####Média de estatísticas por clube por temporada, útil para comparar o desempenho geral dos times.

# COMMAND ----------

df_clube_stats_temporada = df_silver_estatisticas.groupBy("ano_temporada", "clube").agg(
    count(col("id_partida")).alias("jogos_disputados"),
    round(avg("chutes"), 2).alias("media_chutes_por_jogo"),
    round(avg("chutes_no_alvo"), 2).alias("media_chutes_no_alvo_por_jogo"),
    round(avg("posse_de_bola"), 2).alias("media_posse_de_bola"),
    round(avg("passes"), 2).alias("media_passes_por_jogo"),
    round(avg("precisao_passes"), 2).alias("media_precisao_passes"),
    round(avg("faltas"), 2).alias("media_faltas_por_jogo"),
    round(avg("cartao_amarelo"), 2).alias("media_cartao_amarelo_por_jogo"),
    round(avg("cartao_vermelho"), 2).alias("media_cartao_vermelho_por_jogo"),
    round(avg("impedimentos"), 2).alias("media_impedimentos_por_jogo"),
    round(avg("escanteios"), 2).alias("media_escanteios_por_jogo")
).orderBy("ano_temporada", col("clube").asc())

gold_clube_estatisticas_path = f"{gold_path}gold_clube_estatisticas_temporada/"

df_clube_stats_temporada.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_clube_estatisticas_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_clube_estatisticas_temporada USING DELTA LOCATION '{gold_clube_estatisticas_path}'")
print(f"Tabela 'gold_clube_estatisticas_temporada' criada com sucesso na camada Gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2. Criar Tabela: `gold_resumo_estatisticas_rodada`
# MAGIC ##### Média de estatísticas por rodada, útil para entender o comportamento geral do campeonato ao longo do tempo.

# COMMAND ----------

df_resumo_rodadas_stats = df_silver_estatisticas.groupBy("ano_temporada", "rodada_numero").agg(
    count(col("id_partida")).alias("total_entradas_rodada"), # Uma entrada por time por partida
    round(avg("chutes"), 2).alias("media_chutes_rodada"),
    round(avg("chutes_no_alvo"), 2).alias("media_chutes_no_alvo_rodada"),
    round(avg("posse_de_bola"), 2).alias("media_posse_de_bola_rodada"),
    round(avg("passes"), 2).alias("media_passes_rodada"),
    round(avg("precisao_passes"), 2).alias("media_precisao_passes_rodada"),
    round(avg("faltas"), 2).alias("media_faltas_rodada"),
    round(avg("cartao_amarelo"), 2).alias("media_cartao_amarelo_rodada"),
    round(avg("cartao_vermelho"), 2).alias("media_cartao_vermelho_rodada"),
    round(avg("impedimentos"), 2).alias("media_impedimentos_rodada"),
    round(avg("escanteios"), 2).alias("media_escanteios_rodada")
).orderBy("ano_temporada", "rodada_numero")

gold_resumo_estatisticas_rodada_path = f"{gold_path}gold_resumo_estatisticas_rodada/"

df_resumo_rodadas_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_resumo_estatisticas_rodada_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_resumo_estatisticas_rodada USING DELTA LOCATION '{gold_resumo_estatisticas_rodada_path}'")
print(f"Tabela 'gold_resumo_estatisticas_rodada' criada com sucesso na camada Gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3. Criar Tabela: `gold_clube_cartoes_totais`
# MAGIC ##### Total de cartões amarelos e vermelhos por clube por temporada, para identificar times mais ou menos disciplinados.

# COMMAND ----------

df_clube_cartoes = df_silver_estatisticas.groupBy("ano_temporada", "clube").agg(
    sum("cartao_amarelo").alias("total_cartoes_amarelos"),
    sum("cartao_vermelho").alias("total_cartoes_vermelhos")
).orderBy("ano_temporada", "clube")

gold_clube_cartoes_path = f"{gold_path}gold_clube_cartoes_totais/"

df_clube_cartoes.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_clube_cartoes_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_clube_cartoes_totais USING DELTA LOCATION '{gold_clube_cartoes_path}'")
print(f"Tabela 'gold_clube_cartoes_totais' criada com sucesso na camada Gold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificação e Exemplos de Consultas (Camada Gold)
# MAGIC ##### Vamos consultar as tabelas Gold para ver os resultados e demonstrar o potencial analítico das estatísticas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1. Consultar `gold_clube_estatisticas_temporada`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo: Média de chutes no alvo por jogo dos clubes na temporada 2023 (ou última disponível)
# MAGIC SELECT
# MAGIC   ano_temporada,
# MAGIC   clube,
# MAGIC   jogos_disputados,
# MAGIC   media_chutes_por_jogo,
# MAGIC   media_chutes_no_alvo_por_jogo,
# MAGIC   media_posse_de_bola
# MAGIC FROM gold_clube_estatisticas_temporada
# MAGIC WHERE ano_temporada = (SELECT MAX(ano_temporada) FROM gold_clube_estatisticas_temporada)
# MAGIC ORDER BY media_chutes_no_alvo_por_jogo DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2. Consultar `gold_resumo_estatisticas_rodada`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo: Média de faltas e cartões amarelos por rodada na temporada 2023 (ou última disponível)
# MAGIC SELECT
# MAGIC   ano_temporada,
# MAGIC   rodada_numero,
# MAGIC   media_faltas_rodada,
# MAGIC   media_cartao_amarelo_rodada,
# MAGIC   media_cartao_vermelho_rodada
# MAGIC FROM gold_resumo_estatisticas_rodada
# MAGIC WHERE ano_temporada = (SELECT MAX(ano_temporada) FROM gold_resumo_estatisticas_rodada)
# MAGIC ORDER BY rodada_numero ASC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3. Consultar `gold_clube_cartoes_totais`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exemplo: Clubes com mais cartões vermelhos na temporada 2023 (ou última disponível)
# MAGIC SELECT
# MAGIC   ano_temporada,
# MAGIC   clube,
# MAGIC   total_cartoes_amarelos,
# MAGIC   total_cartoes_vermelhos
# MAGIC FROM gold_clube_cartoes_totais
# MAGIC WHERE ano_temporada = (SELECT MAX(ano_temporada) FROM gold_clube_cartoes_totais)
# MAGIC ORDER BY total_cartoes_vermelhos DESC, total_cartoes_amarelos DESC
# MAGIC LIMIT 10;