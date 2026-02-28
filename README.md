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

## Configurando o Projeto

### 1. Clone o repositório
```bash
git clone https://github.com/tricktx/iplanrio-desafio-data-eng.git
```

### 2. Navegue até o diretório do projeto
```bash
cd iplanrio-desafio-data-eng
```

### 3. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com a seguinte variável:
```env
export GOOGLE_APPLICATION_CREDENTIALS=</path/service/account.json>
```

> Substitua `</path/service/account.json>` pelo caminho real da sua service account do GCP.

### 4. Suba os containers com Docker Compose
```bash
docker compose up -d --build
```

### 5. Acesse as interfaces

Após todos os containers estarem ativos, acesse:

- **Prefect UI** → [http://127.0.0.1:4200/dashboard](http://127.0.0.1:4200/dashboard)
- **FastAPI** → [http://localhost:8000/](http://localhost:8000/)

### 6. Configure os Blocks no Prefect

Na UI do Prefect, acesse a aba **Blocks** e configure:

- `cgu-bucket` → nome do seu bucket no GCP
- `cgu-service-account` → caminho da sua service account

> **💡 Dica:** Caso queira criar um novo bucket, utilize o Terraform disponível em `terraform/main.tf` e atualize o nome do bucket dentro dos blocks após a criação.
---

## Fluxo dos Dados

## Orquestração

O **Prefect** executa a pipeline diariamente às **19:00 (horário de Brasília)**.

- **Flow:** `CGU Data Pipeline`  
- **Definição:** `src.pipelines.flows.py`  
- **Deploy:** `deploy.py`  
- **Nome do deployment:** `deploy-cgu`  

> [!NOTE]  
> Se for a primeira execução do projeto, recomenda-se fortemente rodar a pipeline com o parâmetro `load_to_data=True`.  
>  
> Nesse modo, o bucket será populado com todos os dados históricos disponíveis, executando integralmente o processo de ingestão, validação e particionamento.

---

## Estrutura das Tasks (`src.pipelines.tasks`)

A pipeline é composta por tasks responsáveis por controlar disponibilidade, ingestão e consistência dos dados.

### 1. `check_for_updates`

Responsável por:

- Consultar a **data máxima** disponível na camada Bronze.
- Adicionar **4 meses** à data encontrada (janela em que os dados costumam ser publicados).
- Construir dinamicamente a URL de verificação.
- Validar se a requisição HTTP retorna **status 200**.

**Comportamento:**

- Se retornar `200`, os dados são considerados disponíveis e o download é iniciado.
- Caso contrário, a execução é encerrada de forma controlada e o flow será reexecutado no próximo agendamento.

Foi implementada ainda uma lógica para impedir carga duplicada no banco, garantindo **idempotência** no processo.

---

### 2. `ingest_and_partition`

Responsável pela ingestão e padronização dos dados.

Principais etapas:

- Validação estrutural dos arquivos.
- Correção de inconsistências históricas (exemplo: `201901`, que não continha cabeçalho de colunas).
- Garantia de padronização do schema antes da persistência.

Após validação, os dados são salvos em formato **`.parquet`**, particionados por ano e mês:

```
terceirizados_201901
terceirizados_201902
```

Essa estratégia melhora organização, rastreabilidade e performance de leitura.

---

## Utilitários (`src.utils.setup`)

### 1. `upload_files_in_directory`

Realiza o upload de todos os arquivos de um diretório local para uma pasta específica no **GCS**.

### 2. Execução do dbt

Executa:

- `dbt run` para as camadas:
  - Bronze
  - Silver
  - Gold
- `dbt test` na camada Silver

A camada **Silver** utiliza materialização **incremental com chave técnica**, evitando reprocessamento completo e garantindo eficiência.

---

> [!NOTE]  
> Caso deseje executar a pipeline para anos anteriores, consulte a documentação da função `check_for_update_and_download`.  
>  
> Não há previsão oficial de atualização retroativa na fonte. Portanto:
>  
> - A camada **Silver** não será reprocessada integralmente (modelo incremental).  
> - A camada **Gold**, derivada da Silver, também não sofrerá alterações.  
>  
> Execuções retroativas possuem finalidade demonstrativa, evidenciando a consistência e reprodutibilidade da arquitetura.

---

## Exposição da Camada Gold via API

A camada **Gold** é exposta por meio de uma API REST construída com **FastAPI**.

### Endpoints disponíveis

**Paginação:**
```bash
http://localhost:8000/terceirizados/pages/{page}
```


**Consulta por ID:**
```
http://localhost:8000/terceirizados/{id}
```

A API implementa paginação e filtros diretamente na base, garantindo eficiência no consumo dos dados.

Percebe-se na imagem abaixo que o fluxo de Dados rodou perfeitamente no Prefect 3.
![alt text](images/image.png)

Também podemos validar a página de page e de id no FastAPI retornando os dados:
![alt text](images/image-1.png)

![alt text](images/image-2.png)