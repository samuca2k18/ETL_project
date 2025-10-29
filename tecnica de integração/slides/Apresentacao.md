---
title: Pipeline ETL – IBGE População (Bronze → Silver → Gold)
author: Equipe
date: 2025
---

## Objetivo

- Demonstrar um pipeline ETL completo usando dados públicos do IBGE
- Armazenamento em camadas: Bronze (Raw), Silver (Refined), Gold (Curado)
- Entrega com código, scripts e documentação para apresentação

---

## Arquitetura (Visão Geral)

```mermaid
flowchart LR
  A[Fonte Pública IBGE\n(Estimativas 2025 ODS)] -->|Extract| B[Bronze\nRaw file .ods]
  B -->|Transform| C[Silver\nCSV limpo/harmonizado]
  C -->|Load\nAgregações| D1[Gold\nPopulação por UF]
  C -->|Load\nRanking| D2[Gold\nTop N Municípios]
```

---

## Bronze (Raw)
- Download direto do IBGE (ODS)
- Sem alterações (fidedigno à fonte)
- Reuso do arquivo para evitar redownload

---

## Silver (Refined)
- Harmonização de colunas IBGE → `cod_municipio`, `nome_municipio`, `uf`, `populacao`
- Normalização de strings e tipos (numéricos)
- Remoção de duplicatas e nulos críticos

---

## Gold (Curado)
- `gold_populacao_por_uf.csv`: soma de população por UF + nº de municípios
- `gold_top_municipios.csv`: Top N por população (N configurável)

---

## Pipeline (Detalhe Técnico)
- Extract: `etl.py > extract_to_bronze`
  - Retries/backoff, cabeçalhos HTTP
- Transform: `etl.py > transform_to_silver` + `src/transforms.py > silver_pipeline`
  - Leitura ODS/CSV robusta, heurística de cabeçalho/aba
- Load: `etl.py > load_to_gold`
  - Agregações e ranking, salvando CSVs

---

## Execução
```bash
cd "tecnica de integração"
python -m venv ..\.venv
..\.venv\Scripts\activate
pip install -r requirements.txt
python etl.py --config config.yaml
```
Windows: `run_etl.bat`

---

## Desafios & Soluções
- Cabeçalhos variáveis → harmonização por termos-chave
- ODS com múltiplas abas → leitura "smart" e pontuação de header
- HTML/404 por URL incorreta → verificação e mensagem guiada
- Intermitência de rede → retries + reaproveitamento de Bronze

---

## Evidências (Saídas)
- Silver: `data/silver/ibge_populacao_2025_clean.csv`
- Gold:
  - `data/gold/gold_populacao_por_uf.csv`
  - `data/gold/gold_top_municipios.csv`

---

## Próximos Passos
- Dashboard (ex.: Power BI/Streamlit)
- Orquestração (ex.: Airflow) e agendamento
- Testes de dados (ex.: Great Expectations)

---

## Contato
- Código e docs: pasta `tecnica de integração/`
- `README.md` (uso) e `RELATORIO.md` (itens 4 e 5)

