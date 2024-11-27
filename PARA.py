from pyspark.sql import SparkSession
import difflib  # Para comparar a semelhança entre os nomes das colunas

# Inicializar SparkSession
spark = SparkSession.builder.appName("De/Para DataFrames Customizado").getOrCreate()

# Função para obter e exibir cabeçalho de um JSON
def get_header_as_dataframe(path):
    try:
        # Lê o arquivo JSON no caminho especificado
        df = spark.read.json(path)
        
        # Obtém os nomes das colunas (cabeçalho)
        columns = df.columns
        
        # Cria um DataFrame com os nomes das colunas
        header_df = spark.createDataFrame([(col,) for col in columns], ["Column Name do Json"])
        
        # Exibe o DataFrame com os nomes das colunas
        print(f"\n{'='*80}")
        print(f"DataFrame com as colunas do arquivo JSON: {path}")
        print(f"{'='*80}")
        header_df.show(100, truncate=False)
        
        return columns  # Retorna a lista de colunas para reutilização
        
    except Exception as e:
        print(f"\nErro ao ler {path}")
        print(f"Erro: {str(e)}")
        return None

# Função para criar DataFrame com nomes de colunas fornecidos pelo usuário
def get_columns_and_create_df():
    # Receber os nomes das colunas como entrada do usuário
    columns = input("Digite os nomes das colunas separados por virgula: ").split(",")
    
    # Limpar espaços extras
    columns = [col.strip() for col in columns]
    
    # Criar um DataFrame com os nomes das colunas
    header_df = spark.createDataFrame([(col,) for col in columns], ["Column Name do excel"])
    
    # Exibir o DataFrame
    header_df.show(100, truncate=False)
    return columns  # Retorna as colunas fornecidas

# Função para comparar colunas e criar um DataFrame de mapeamento de/para
def compare_columns(columns_from_json, columns_from_excel):
    """
    Compara as colunas dos dois conjuntos e cria um DataFrame de mapeamento de/para.
    - Usa difflib para comparar a semelhança dos nomes das colunas.
    """
    # Para cada coluna do JSON, buscamos uma correspondência no Excel
    mapping = []
    for json_col in columns_from_json:
        best_match = None
        highest_ratio = 0
        
        for excel_col in columns_from_excel:
            ratio = difflib.SequenceMatcher(None, json_col.lower(), excel_col.lower()).ratio()
            if ratio > highest_ratio:
                best_match = excel_col
                highest_ratio = ratio
        
        # Determina o status com base na correspondência encontrada
        if highest_ratio > 0.7:
            mapping.append((json_col, best_match, "Contém"))
        else:
            mapping.append((json_col, "Nenhuma correspodência", "Não contém"))
    
    # Cria o DataFrame de mapeamento
    de_para_df = spark.createDataFrame(mapping, ["Coluna JSON", "Coluna Excel", "Status"])
    
    # Exibe o DataFrame de/para
    print("\nDataFrame De/Para com Status:")
    de_para_df.show(100, truncate=False)
    return de_para_df

# Chamar a função para criar DataFrame com nomes fornecidos pelo usuário
columns_from_excel = get_columns_and_create_df()

# Solicitar ao usuário o caminho do arquivo
json_path = input("Digite o caminho completo do arquivo JSON (usando dbfs:/): ")

# Processar o caminho inserido e obter o cabeçalho
columns_from_json = get_header_as_dataframe(json_path)

# Criar o DataFrame de/para com comparação de similaridade entre as colunas
if columns_from_json and columns_from_excel:
    de_para_df = compare_columns(columns_from_json, columns_from_excel)