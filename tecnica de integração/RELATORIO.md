# Relatório do Projeto – Pipeline ETL (IBGE População)

Resumo executivo: Pipeline ETL completo utilizando dados públicos do IBGE (Estimativas de População por município), com camadas Bronze/Silver/Gold, documentação e script de execução. Este relatório cobre os itens 4 e 5 do enunciado.

## 1. Fonte dos dados e justificativa da escolha
- Fonte: IBGE – Estimativas de População por Município (2025).
- URL: `https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2025/estimativa_dou_2025.ods`
- Justificativa:
  - Base pública, estável e de alta credibilidade.
  - Abrangência nacional com granularidade municipal.
  - Uso recorrente em análises demográficas e planejamento público/privado.

## 2. Estrutura e formato dos dados originais
- Formato principal: ODS (planilha open document); o pipeline também aceita CSV.
- Estrutura típica (pode variar por ano/aba):
  - Colunas com cabeçalhos heterogêneos no IBGE (ex.: "Cód. Município", "Nome do Município", "UF", "População").
  - Pode conter múltiplas abas (UF vs Municípios) e linhas de cabeçalho superiores.
- Consequências:
  - Necessário autodetectar a aba mais relevante (municípios) e a linha de cabeçalho.
  - Necessário harmonizar nomes de colunas para um esquema comum.

## 3. Etapas de transformação aplicadas (Silver)
As transformações são implementadas em `src/transforms.py` dentro da função `silver_pipeline`:
- Normalização de nomes de colunas (`normalize_columns`).
- Harmonização específica IBGE (`harmonize_ibge_columns`) para mapear para:
  - `cod_municipio`, `nome_municipio`, `uf`, `populacao`.
- Padronização do código IBGE com 7 dígitos (`standardize_ibge_code`).
- Limpeza de strings e coersão de tipos numéricos (`clean_strings`, `coerce_types`).
- Validação de colunas obrigatórias e remoção de duplicatas/nulos críticos (`drop_obvious_issues`).

Entrada Silver: ODS/CSV bruto (Bronze).
Saída Silver: `data/silver/ibge_populacao_2025_clean.csv`.

## 4. Modelo de dados final (Gold)
Gerado em `etl.py > load_to_gold`, usando funções em `src/transforms.py`:
- `gold_populacao_por_uf.csv`:
  - Campos: `uf`, `populacao_total`, `municipios`.
  - Lógica: soma de população por UF e contagem de municípios distintos.
- `gold_top_municipios.csv`:
  - Campos: `cod_municipio`, `nome_municipio`, `uf`, `populacao`.
  - Lógica: top N municípios por população (N configurável em `config.yaml: gold.top_n`).

## 5. Desafios encontrados e soluções adotadas
- Variação de cabeçalhos do IBGE entre anos/abas:
  - Solução: heurísticas de harmonização (normalização de texto + mapeamento por termos-chave).
- Formatos múltiplos (ODS/CSV) e arquivos com abas e cabeçalhos em linhas superiores:
  - Solução: leitura "inteligente" (`read_ibge_ods_smart`) que pontua possíveis cabeçalhos e seleciona a aba/linha mais adequada.
- Possíveis HTML/404 salvos como CSV por erro de URL:
  - Solução: verificação do "magic" no início do arquivo e erro claro com instrução para corrigir `config.yaml`.
- Robustez de execução (rate limit/intermitência de rede):
  - Solução: retries com backoff exponencial e reaproveitamento do artefato Bronze existente.

## 6. Organização das camadas
- Bronze: `data/bronze/estimativa_dou_2025.ods` (arquivo bruto, sem alterações).
- Silver: `data/silver/ibge_populacao_2025_clean.csv` (dados padronizados).
- Gold: `data/gold/*.csv` (tabelas analíticas).

## 7. Execução e reprodutibilidade
- Requisitos em `requirements.txt`.
- Execução padrão:
  ```bash
  cd "tecnica de integração"
  python etl.py --config config.yaml
  ```
- Opções:
  - `--skip-extract` para reutilizar Bronze existente;
  - `--skip-transform` para reutilizar Silver existente.

## 8. Entregáveis (cobertura dos itens 4 e 5)
- Código-fonte: `etl.py`, `src/`, `config.yaml`.
- Artefatos de dados: `data/bronze`, `data/silver`, `data/gold`.
- Scripts: `run_etl.bat` (Windows, opcional).
- Documentação: este `RELATORIO.md` (itens 4 e 5) e `README.md` (execução/uso).
