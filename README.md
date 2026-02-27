# Desafio de Data Engineer - IPLANRIO

---

## Objetivo: 
O objetivo desse desafio é resolver o problema proposta pela equipe do IPLANRIO para a vaga de engenheiro de Dados, criando uma arquitetura simples, resiliente e escalável. Para maiores informações, leia: `https://github.com/prefeitura-rio/iplanrio-desafio-data-eng`


## Decisões Arquiteturais
---

- Orquestração com Prefect
- Bronze recriada a cada execução devido ao baixo volume
- Silver incremental
- Gold materializada como tabela
- Orquestração de containers com Docker Compose
- Exposição dos dados via API REST com FastAPI

Seguindo o desafio proposto, ao final do processo de ETL, o bucket ficou da seguinte forma:

```
br-cgu-terceirizados/
├── terceirizados/
│   ├── terceirizados_2025-01.parquet
│   ├── terceirizados_2025-05.parquet
│   ├── terceirizados_2025-07.parquet
│   └── ...
├── bronze/
│   └── terceirizados-bronze.duckdb
├── silver/
│   └── terceirizados-silver.duckdb
└── gold/
    └── terceirizados-gold.duckdb
```


## Arquiteutra do Projeto

![Texto Alternativo](images/arquitetura.png)

## Configurando o projeto

1. Clone o projeto

```bash
https://github.com/tricktx/iplanrio-desafio-data-eng.git
```

2. Navegue até o repositório:

```bash
iplanrio-desafio-data-eng
```

3. Crie um arquivo chamado `.env`, com a seguinte variável: 
```
export GOOGLE_APPLICATION_CREDENTIALS=</path/service/account.json>
```

3. Rode o docker-compose

```bash
docker compose up -d --build
```

4. Após todos os container estiverem ativos, acesse `http://127.0.0.1:4200/dashboard` para acessar a UI do Prefect e também acesse o `http://localhost:8000/` para rodar o FAST API.

5. Acesse a aba `Blocks` na UI do Prefect e adicione o nome do seu bucket no GCP em `cgu-bucket` e o local que se encontra a sua service-account em `cgu-service-account`.

   5.1. Caso queira criar um novo bucket, aconselho que utilize o Terraform no arquivo `terraform/main.tf` e altere o nome do bucket dentro do blocks.
---

## Fluxo dos Dados

O Prefect executa a pipeline TODOS os dias às 19:00hrs de Brasília. O Flow se chama `CGU Data Pipeline` e está configurado no arquivo `src.pipelines.flows.py` que posteriormente é feito o deploy pelo arquivo `deploy.py` que se chama `deploy-cgu`.

A pipeline possui algumas tasks que fazem todo o fluxo rodar. No arquivo chamado `src.pipelines.tasks`, eu criei três grandes tasks: 

1. check_for_updates: Basicamente, essa task construi a URL, a partir de uma verificação na data máxima da camada bronze, adicionando 4 meses, pois os dados são disponibilizados dentro desse período. e verifica se ela retorna um `200` . Se retornar 200, verificamos a requisição foi bem sucedida e podemos baixar os dados. É basicamente uma task de verificação e download, se ela retornar False, a pipeline é encerrada e só roda no outro dia. Visando resolver o problema do desafio, uma lógica foi criada para impossibilitar o carregamento de dados duplicados no banco. 

3. ingest_and_partition: Essa task faz todo o processo de ingestão, após algumas validações. Algumas tabelas como `201901`, tinha um grande problema que não vinha com as colunas, dessa forma, precisei validar e se não tivesse, cria-la. Posteriormente, salvo os arquivos em .parquet particionados com os seus anos e meses de carga. `terceirizados_201901`, `terceirizados_201902`

No arquivo chamado `src.utils.setup`, criei os arquivos que podem ser replicados.
1. upload_files_in_directory: Sobe os arquivos de um diretório local para uma folder no GCS.
2. Executa um dbt run em cada camada de dados específica (bronze, silver e gold) e também fazer o testes na camada Silver.

Por fim, a camada gold expõe os dados via API REST com FastAPI e retornar dados com paginação com o seguinte código: `http://localhost:8000/terceirizados/pages/{page}` e um where na base com o id do terceirizado `http://localhost:8000/terceirizados/{id}`

Percebe-se na imagem abaixo que o fluxo de Dados rodou perfeitamente no Prefect 3.
![alt text](images/image.png)

Também podemos validar a página de page e de id no FastAPI retornando os dados:
![alt text](images/image-1.png)

![alt text](images/image-2.png)