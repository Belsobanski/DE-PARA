from pyspark.sql import SparkSession
import difflib  # Para comparar a semelhança entre os nomes das colunas


def initialize_spark(app_name="De/Para DataFrames Customizado"):
    """Inicializa a SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_header_as_dataframe(spark, path):
    """
    Lê o arquivo JSON no caminho especificado e retorna as colunas como uma lista.
    Exibe as colunas em um DataFrame.
    """
    try:
        df = spark.read.json(path)
        columns = df.columns
        header_df = spark.createDataFrame([(col,) for col in columns], ["Column Name do JSON"])
        print(f"\nColunas do JSON ({path}):")
        display(header_df)
        return columns
    except Exception as e:
        print(f"\nErro ao ler o arquivo JSON: {path}")
        print(f"Detalhes do erro: {e}")
        return None


def get_columns_from_input(spark):
    """
    Solicita os nomes das colunas ao usuário via input e retorna uma lista.
    Exibe as colunas fornecidas em um DataFrame.
    """
    try:
        columns = input("Digite os nomes das colunas separados por vírgula: ").split(",")
        columns = [col.strip() for col in columns]
        header_df = spark.createDataFrame([(col,) for col in columns], ["Column Name do Excel"])
        print("\nColunas fornecidas pelo usuário:")
        display(header_df)
        return columns
    except Exception as e:
        print("Erro ao processar entrada das colunas.")
        print(f"Detalhes do erro: {e}")
        return None


def compare_columns(spark, columns_from_json, columns_from_excel):
    """
    Compara as colunas do JSON com as do Excel e cria um DataFrame de mapeamento de/para.
    """
    mapping = []
    for json_col in columns_from_json:
        best_match, highest_ratio = None, 0
        for excel_col in columns_from_excel:
            ratio = difflib.SequenceMatcher(None, json_col.lower(), excel_col.lower()).ratio()
            if ratio > highest_ratio:
                best_match, highest_ratio = excel_col, ratio

        status = "Contém" if highest_ratio > 0.7 else "Não contém"
        mapping.append((json_col, best_match if best_match else "Nenhuma correspondência", status))

    de_para_df = spark.createDataFrame(mapping, ["Coluna JSON", "Coluna Excel", "Status"])
    print("\nMapeamento De/Para:")
    display(de_para_df)
    return de_para_df


def main():
    """
    Função principal para executar o fluxo completo.
    """
    spark = initialize_spark()

    # Receber colunas do Excel e caminho do JSON
    columns_from_excel = get_columns_from_input(spark)
    if not columns_from_excel:
        print("Nenhuma coluna fornecida. Encerrando o programa.")
        return

    json_path = input("Digite o caminho completo do arquivo JSON (usando dbfs:/): ")
    columns_from_json = get_header_as_dataframe(spark, json_path)
    if not columns_from_json:
        print("Erro ao obter colunas do JSON. Encerrando o programa.")
        return

    # Comparar e criar o DataFrame de mapeamento
    compare_columns(spark, columns_from_json, columns_from_excel)


if __name__ == "__main__":
    main()
