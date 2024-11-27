# DE-PARA
realizar o mapeamento (De/Para) entre as colunas
Utilizando PySpark e o Databricks, consegui automatizar essa tarefa e alcançar resultados precisos. O fluxo envolveu as seguintes etapas:

1️⃣ Leitura do JSON: Usamos o Databricks para processar o arquivo JSON, identificando suas colunas e estruturando as informações em um DataFrame.

2️⃣ Recebimento do Modelo de Referência: Coletamos os nomes das colunas esperadas, fornecidas pelo usuário.

3️⃣ Comparação Automatizada: Com a biblioteca difflib, comparamos os nomes das colunas do JSON com o modelo de referência, identificando correspondências com base na similaridade.

4️⃣ Classificação: Cada coluna foi marcada como "Contém" (se houve correspondência) ou "Não contém" (se não houve).

5️⃣ Geração do DataFrame De/Para: Criamos um DataFrame com o resultado do mapeamento e exportamos o resultado para Excel.

