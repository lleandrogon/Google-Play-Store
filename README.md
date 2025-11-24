# Projeto de Pipeline no Databricks

Este repositório contém o código e a estrutura de um pipeline de dados desenvolvido para rodar **exclusivamente no Databricks**, utilizando **PySpark**, **Delta Lake** e **Jobs do Databricks** para orquestração e validações entre as camadas do lake.

## 🚀 Visão Geral do Projeto

O objetivo deste projeto é processar e transformar dados brutos provenientes da base "Google Play Store", realizando a evolução das camadas:

* **Bronze** → ingestão dos dados brutos
* **Silver** → limpeza, padronização e validações
* **Gold** → agregações e tabelas finais para consumo

Todo o fluxo é executado dentro do Databricks e **não roda localmente**.

---

## 🧱 Arquitetura do Pipeline

O pipeline segue a abordagem de medallion architecture:

### **1. Bronze**

* Ingestão direta dos arquivos originais
* Armazenamento em formato Delta
* Sem transformações complexas

### **2. Silver**

* Padronização dos tipos de dados
* Remoção e tratamento de inconsistências
* Regras de qualidade de dados
* Escrita em Delta

### **3. Gold**

* Métricas, agregações e indicadores finais
* Tabelas otimizadas para análise

---

## 🔧 Tecnologias Utilizadas

* **Databricks (Community ou Enterprise)**
* **PySpark**
* **Spark SQL**
* **Delta Lake**
* **Databricks Jobs** (para orquestração)
* **Databricks Notebooks**

---

## 🧭 Orquestração com Databricks Jobs

A orquestração do pipeline foi feita com a ferramenta **Databricks Jobs**, utilizando múltiplas tarefas executadas em sequência:

1. **Job Bronze** – Faz a ingestão dos dados.
2. **Job Silver** – Executa as validações da camada Silver.
3. **Job Gold** – Monta as métricas finais e tabelas de consumo.

Cada job possui dependências configuradas para garantir a ordem correta da execução.

Além disso, foram implementadas **validações automáticas**, como:

* contagem de registros
* verificações de schema
* checagem de duplicidade
* validação de colunas obrigatórias

## ▶️ Como Executar

Como o projeto **não roda localmente**, a execução ocorre **exclusivamente no Databricks**.

### Passo a passo:

1. Importe o código/notebooks para o Workspace do Databricks.
2. Configure os caminhos de leitura e escrita no DBFS.
3. Crie os Jobs no Databricks.
4. Configure as dependências:

   * Silver depende do Bronze
   * Gold depende do Silver
5. Execute o job principal.

---

## 🧪 Validações Implementadas

Durante a evolução das camadas, o pipeline aplica diversas validações:

* Schema validation
* Verificação de nulls
* Duplicidade de chaves
* Normalização de colunas
* Verificação de consistência de tipos

Em caso de falha, o job é interrompido para garantir integridade.
