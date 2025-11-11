import os
import subprocess
import sys

# ==================== CONFIGURAÇÃO ====================
BASE_DIR = os.path.join(os.getcwd(), "atividade_mapreduce_alice")

# Define o executável do Python
PYTHON_EXEC = "python" if sys.platform != "win32" else "python"

# ==================== MENU DE EXERCÍCIOS ====================
EXERCICIOS = {
    "1": "exercicio1/ex1_wordcount.py",
    "2": "exercicio2/ex2_log_analysis.py",
    "3": "exercicio3/ex3_sentiment_analysis.py",
    "4.1": "exercicio4/ex4_sales_aggregation.py sem",
    "4.2": "exercicio4/ex4_sales_aggregation.py com",
    "5.1": "exercicio5/ex5_1_spark_wordcount.py",
    "5.2": "exercicio5/ex5_2_spark_logs.py",
    "5.3": "exercicio5/ex5_3_spark_sales.py",
}

# ==================== FUNÇÃO PARA EXECUTAR SCRIPT ====================
def executar_script(caminho_script):
    partes = caminho_script.split()
    script = partes[0]
    args = partes[1:]  # argumentos extras, se houver

    caminho_completo = os.path.join(BASE_DIR, script)

    if not os.path.exists(caminho_completo):
        print(f"❌ Arquivo {script} não encontrado em {BASE_DIR}.")
        return

    print(f"\n🚀 Executando {script} {' '.join(args)}...\n")

    process = subprocess.Popen(
        [PYTHON_EXEC, "-u", caminho_completo, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        print(line, end='')

    process.wait()
    if process.returncode != 0:
        print(f"\n⚠️ O script {script} terminou com erro (código {process.returncode})")

# ==================== MENU INTERATIVO ====================
def menu():
    while True:
        print("\n==============================")
        print("📚 MENU DE EXERCÍCIOS MAPREDUCE")
        print("==============================")
        print("1   - Exercício 1 (WordCount)")
        print("2   - Exercício 2 (Log Analysis)")
        print("3   - Exercício 3 (Sentiment Analysis)")
        print("4.1 - Exercício 4 (Sales Aggregation - sem Combiner)")
        print("4.2 - Exercício 4 (Sales Aggregation - com Combiner)")
        print("5.1 - Exercício 5_1 (Spark WordCount)")
        print("5.2 - Exercício 5_2 (Spark Log Analysis)")
        print("5.3 - Exercício 5_3 (Spark Sales Aggregation)")
        print("0   - Sair")
        print("==============================")

        opcao = input("👉 Escolha o exercício: ").strip()

        if opcao == "0":
            print("👋 Encerrando o menu... Até logo!")
            break
        elif opcao in EXERCICIOS:
            executar_script(EXERCICIOS[opcao])
        else:
            print("⚠️ Opção inválida! Tente novamente.")

# ==================== EXECUÇÃO PRINCIPAL ====================
if __name__ == "__main__":
    menu()
