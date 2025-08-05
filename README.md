# Projeto de Engenharia de Dados de Estatísticas de Futebol Brasileiro no Databricks

## ⚽ Visão Geral do Projeto

Este projeto demonstra uma arquitetura de engenharia de dados em camadas (Bronze, Silver e Gold) utilizando o **Databricks Community Edition** e **Apache Spark (PySpark)**, com foco em **estatísticas detalhadas por clube em cada partida do Campeonato Brasileiro de Futebol**. O objetivo é transformar dados brutos de desempenho individual dos clubes em informações analíticas valiosas, permitindo análises sobre posse de bola, chutes, faltas, cartões, e outros indicadores de jogo.

Este projeto é ideal para quem deseja explorar métricas de desempenho técnico e tático no futebol brasileiro através da engenharia de dados.

**Importante:** Este dataset (`campeonato-brasileiro-estatisticas-full.csv`) contém apenas estatísticas por clube/partida (ex: chutes, posse de bola, cartões). Ele **não inclui** informações de placar final, vencedor do jogo, se o time era mandante ou visitante, público ou renda. Portanto, as análises de classificação de pontos ou desempenho financeiro de arenas não são possíveis apenas com este arquivo. Se essas análises forem desejadas, este projeto precisará ser complementado com outro dataset que contenha os resultados completos das partidas.

## 🚀 Tecnologias Utilizadas

* **Databricks Community Edition:** Plataforma de Data & AI unificada para desenvolvimento.
* **Apache Spark (PySpark):** Framework de processamento de dados distribuído para lidar com grandes volumes de dados.
* **Delta Lake:** Formato de armazenamento de dados open-source que traz confiabilidade, performance e transações ACID para data lakes.
* **Python:** Linguagem de programação para script dos notebooks.

## 📊 Fonte de Dados

Os dados utilizados neste projeto são obtidos do arquivo **`campeonato-brasileiro-estatisticas-full.csv`**, que é um dataset público de estatísticas do Campeonato Brasileiro de Futebol. Este arquivo oferece informações detalhadas sobre o desempenho de cada clube em cada partida, como:
* `partida_id`: Identificador único da partida.
* `rodata`: Número da rodada.
* `clube`: Nome do clube.
* `chutes`, `chutes_no_alvo`: Tentativas de gol.
* `posse_de_bola`: Porcentagem de posse de bola.
* `passes`, `precisao_passes`: Passes realizados e sua precisão.
* `faltas`, `cartao_amarelo`, `cartao_vermelho`: Aspectos disciplinares.
* `impedimentos`, `escanteios`: Outras estatísticas de jogo.

## 🏗️ Arquitetura de Camadas

O projeto segue a arquitetura de Data Lakehouse com três camadas principais:

![Arquitetura Data Lakehouse](https://www.databricks.com/wp-content/uploads/2020/02/blog-data-lakehouse-architecture.png)
_Fonte da imagem: Databricks Blog_

### 1. 🥉 Camada Bronze (Raw Data)

* **Objetivo:** Ingestão dos dados brutos "as-is" (como estão) do arquivo CSV.
* **Formato:** O CSV original é lido e armazenado em uma tabela **Delta Lake**.
* **Localização:** `dbfs:/user/<seu_email_ou_id>/brasileirao_estatisticas_data/bronze/`
* **Características:** Dados com esquema inferido, mínima ou nenhuma transformação, garantindo a fidelidade dos dados de origem. A tabela principal nesta camada é `bronze_brasileirao_estatisticas_full`.

### 2. 🥈 Camada Silver (Cleaned & Conformed Data)

* **Objetivo:** Limpeza, padronização e enriquecimento dos dados da camada Bronze.
* **Transformações:**
    * Renomeação de colunas para um padrão `snake_case` e nomes mais amigáveis (ex: `rodata` para `rodada_texto`, `partida_id` para `id_partida`).
    * Tratamento de valores nulos, preenchendo zeros em campos numéricos essenciais (chutes, cartões, etc.).
    * Remoção de caracteres especiais e conversão de tipos de dados (ex: `posse_de_bola` e `precisao_passes` de string para `DoubleType`).
    * Criação de novas colunas derivadas para análise (ex: `ano_temporada` (definido como 2023 por padrão para este dataset), `rodada_numero`).
    * Limpeza de strings (remoção de espaços extras dos nomes dos clubes).
* **Formato:** Tabela **Delta Lake** com esquema definido e dados mais estruturados.
* **Localização:** `dbfs:/user/<seu_email_ou_id>/brasileirao_estatisticas_data/silver/`
* **Características:** Dados prontos para agregações e análises mais complexas, com maior qualidade e consistência. A tabela principal é `silver_brasileirao_estatisticas`.

### 3. 🥇 Camada Gold (Aggregated & Curated Data)

* **Objetivo:** Agregação e modelagem dos dados da camada Silver para casos de uso analíticos específicos, baseados nas estatísticas de jogo.
* **Tabelas Criadas:**
    * `gold_clube_estatisticas_temporada`: Média de estatísticas (chutes, posse de bola, cartões, etc.) por clube por temporada.
    * `gold_resumo_estatisticas_rodada`: Média de estatísticas agregadas por rodada, mostrando tendências do campeonato.
    * `gold_clube_cartoes_totais`: Total de cartões amarelos e vermelhos por clube por temporada, para análise de disciplina.
* **Formato:** Tabelas **Delta Lake** otimizadas para consulta.
* **Localização:** `dbfs:/user/<seu_email_ou_id>/brasileirao_estatisticas_data/gold/`
* **Características:** Dados sumarizados, pré-calculados e prontos para relatórios e dashboards focados em desempenho estatístico.

## ⚙️ Como Executar o Projeto

Siga os passos abaixo para replicar e executar este projeto em seu Databricks Community Edition:

### Pré-requisitos:
* Conta no [Databricks Community Edition](https://community.cloud.databricks.com/).
* Navegador web.
* O arquivo `campeonato-brasileiro-estatisticas-full.csv`.

### Passos:

1.  **Acesse o Databricks Community Edition:** Faça login na sua conta.
2.  **Crie um Cluster:**
    * No menu lateral, vá para **Compute**.
    * Clique em **Create Cluster**.
    * Dê um nome ao cluster (ex: `brasileirao-stats-eng`).
    * Selecione a versão mais recente do **Databricks Runtime (LTS)**.
    * Defina um tempo de inatividade para terminação (ex: 30 minutos).
    * Clique em **Create Cluster**.
3.  **Crie a Estrutura de Pastas no DBFS:**
    * No menu lateral, vá para **Workspace**.
    * Clique com o botão direito na pasta `Users` (ou em sua pasta de usuário individual).
    * Selecione **Create** > **Folder**. Crie uma pasta chamada `brasileirao_estatisticas_data`.
    * Dentro de `brasileirao_estatisticas_data`, crie uma subpasta chamada `raw`.
4.  **Faça Upload do Arquivo de Dados (CSV de Estatísticas):**
    * No Databricks, clique com o botão direito na pasta `raw` que você criou (`dbfs:/user/<seu_email_ou_id>/brasileirao_estatisticas_data/raw/`).
    * Selecione **Upload Data**.
    * Arraste e solte o arquivo `campeonato-brasileiro-estatisticas-full.csv` para a área de upload.
5.  **Importe os Notebooks:**
    * Crie novos notebooks no seu Workspace. Você pode fazer isso clicando com o botão direito em uma pasta (ex: `brasileirao_estatisticas_data`) e selecionando **Create** > **Notebook**.
    * Nomeie os notebooks como:
        * `01_Bronze_Layer`
        * `02_Silver_Layer`
        * `03_Gold_Layer`
    * Copie e cole o código Python de cada seção (Bronze, Silver, Gold) deste README para o notebook correspondente.
    * **Atenção:** Lembre-se de **substituir `sua_pasta_de_usuario`** nos caminhos dos notebooks pelo seu e-mail/ID de usuário no Databricks (ex: `/user/seu.email@example.com/brasileirao_estatisticas_data/`).
6.  **Execute os Notebooks em Sequência:**
    * Certifique-se de que seu cluster está **ligado**.
    * Abra o notebook `01_Bronze_Layer`. Anexe-o ao seu cluster e execute todas as células.
    * Após a conclusão bem-sucedida, abra o notebook `02_Silver_Layer`. Anexe-o ao mesmo cluster e execute todas as células.
    * Finalmente, abra o notebook `03_Gold_Layer`. Anexe-o ao cluster e execute todas as células.

## ✨ Exemplos de Insights e Análises (Camada Gold)

As tabelas na camada Gold estão prontas para serem consultadas e visualizadas, oferecendo insights valiosos sobre as estatísticas dos jogos do Brasileirão. Você pode usar a interface SQL do Databricks ou integrar com ferramentas de BI.

### Estatísticas Médias por Clube por Temporada

```sql
SELECT
  ano_temporada,
  clube,
  jogos_disputados,
  media_chutes_por_jogo,
  media_chutes_no_alvo_por_jogo,
  media_posse_de_bola,
  media_faltas_por_jogo,
  media_cartao_amarelo_por_jogo,
  media_cartao_vermelho_por_jogo
FROM gold_clube_estatisticas_temporada
WHERE ano_temporada = (SELECT MAX(ano_temporada) FROM gold_clube_estatisticas_temporada) -- Pega a última temporada disponível
ORDER BY media_chutes_no_alvo_por_jogo DESC
LIMIT 10;
