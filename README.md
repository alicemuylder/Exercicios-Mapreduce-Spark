# Atividade MapReduce e Spark — Alice De Muylder

Este repositório contém as implementações práticas dos exercícios de **MapReduce** e **PySpark**, desenvolvidos para a disciplina **Big Data e Cloud Computing**, ministrada pelo professor **Cristiano Neto** no **Ibmec BH**.

## COMO EXECUTAR OS EXERCÍCIOS 

### 1. Instalar dependências
Antes de começar, instale as bibliotecas necessárias:

```bash
pip install -r requirements.txt

```
### 2. Executar via Docker 
O ambiente esteja configurado com Docker, rode:

docker run -it --rm -v "$(pwd):/app" atividade_mapreduce python pipeline.py


### EXERCÍCIOS 

📝 Exercício 1 — WordCount
Implementa a contagem de palavras em Python puro, processando o arquivo test_input.txt e gerando results.txt com as ocorrências.

📊 Exercício 2 — Log Analysis
Realiza a análise de logs de servidor web (padrão Apache), extraindo:
IPs únicos
Métodos HTTP (GET, POST, etc.)
URLs mais acessadas
Distribuição de códigos de status

💬 Exercício 3 — Sentiment Analysis
Processa o arquivo reviews.csv, classifica sentimentos (positivo, neutro, negativo) e gera estatísticas agregadas de feedbacks.

💵 Exercício 4 — Sales Aggregation
Analisa transações de vendas em transactions.csv, produzindo totais de vendas por produto, cliente e categoria.

⚡ Exercício 5 — Implementações em PySpark
Versões otimizadas com PySpark, para análise de grandes volumes de dados e comparação de desempenho com Python puro.
5.1	WordCount em Spark
5.2	Análise de logs em Spark
5.3	Agregação de vendas em Spark

### RESULTADOS 
Cada exercício salva sua saída na pasta results/, podendo conter:
Arquivos .txt ou .csv
Relatórios agregados
Visualizações (quando aplicável)
Dentro de cada exercício tem uma aba respostas que mostra as conclusões tiradas após a realização dos códigos.

### TECNOLOGIAS UTILIZADAS 
Python 3.11
PySpark
Docker
Jupyter Notebook (para análise interativa opcional)
MapReduce (conceitos e implementações manuais)
