# Projeto de Extração e Análise de Dados
## Contexto Inicial
Este projeto visa a extração de dados por meio de web scraping, inserção dos dados no BigQuery e análise dos mesmos.
## Bibliotecas Utilizadas
Para web scraping e manipulação de dados:
```python
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
```

Para inserção dos dados no BigQuery usando Apache Beam:
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import DataflowRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from google.cloud import bigquery
from apache_beam.transforms.sql import SqlTransform
```

## Modelagem de dados
Os dados foram extraídos e salvos em formato Parquet no Google Cloud Storage. As bibliotecas requests, BeautifulSoup e pandas foram utilizadas para requisições, scraping e criação de dataframes, respectivamente. A biblioteca os foi usada para a criação de variáveis de ambiente e acesso ao cloud.

Os dados resultantes do scraping foram salvos no storage e utilizados para a criação das tabelas no BigQuery. Não foi necessário tratamento adicional dos dados, pois os nomes das colunas foram definidos durante a extração, e o dataframe não necessitava de limpeza para valores nulos ou NaN, tampouco formatação de datas.

A criação da tabela no BigQuery foi feita utilizando o Apache Beam, através do módulo beam.io.WriteToBigQuery e write_truncate como forma de inscrição no BQ.
A análise foi realizada por meio de query utilizando sql por meio do módulo SqlTransform do Apache Beam, disponível na query do pipeline web_scrap_storage_to_bq na função category_analise.
Estrutura das Tabelas
**Tabela `books_to_scrap`**:

| Coluna           | Descrição                                             |
|------------------|-------------------------------------------------------|
| `id`             | Identificador único do livro                          |
| `product_name`   | Nome do produto                                       |
| `bar_code`       | Código de barras do produto                           |
| `category`       | Categoria do livro                                    |
| `price`          | Preço do livro                                        |
| `qty_stock`      | Quantidade em estoque                                 |
| `star_rating`    | Avaliação do livro em estrelas                        |
| `extraction_date`| Data de extração dos dados                            |


*A descrição de cada coluna está disponível no BigQuery para facilitar a compreensão do dataset.

Tabela category_analytics: foram analisadas informações úteis para o time de negócios, que podem auxiliar na tomada de decisões, como médias, valores mínimos e máximos entre as avaliações existentes, preço médio e mínimo por categorias.
#
### Agregação de Dados

Para a agregação dos dados, foi utilizada uma query (disponível no pipeline web_scrap_storage_to_bq na função category_analise) que agrupa os dados por categorias e calcula médias, valores mínimos e máximos das avaliações e preços. Com esses valores, é possível identificar quais livros têm maior avaliação entre o público e quais categorias possuem os livros melhor avaliados.
#
### Teste dos Dados

Para testar o primeiro pipeline, é possível alterar a saída de dados para 'content/nomedoarquivo.parquet', disponibilizado pelo próprio Colab, e verificar a estrutura final do processamento. O resultado do segundo pipeline pode ser acessado através do link disponibilizado pelo BigQuery:
https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1smywebscrap-423316!2sestudos_gcp

