# ETL IBGE População 2022/2025 – Bronze / Silver / Gold

Pipeline de ETL em Python usando um dataset público do IBGE (Estimativas de População – municípios). O resultado é organizado nas três camadas clássicas de dados.

- **Bronze (Raw):** arquivo original baixado do IBGE, sem alterações.
- **Silver (Refined):** dados limpos e padronizados (códigos IBGE, strings, tipos numéricos, remoção de duplicatas/nulos).
- **Gold (Curado/DW):** tabelas finais para consumo analítico:
  - `gold_populacao_por_uf.csv`: soma de população por UF + contagem de municípios;
  - `gold_top_municipios.csv`: TOP N municípios por população (configurável via `config.yaml`).

## Objetivo (resumo)
Entregar um pipeline ETL completo (extração, transformação e carga) com armazenamento em Bronze/Silver/Gold, utilizando base pública (IBGE) e documentado para apresentação.

## Quick Start
```bash
cd "tecnica de integração"
python -m venv ..\.venv
..\.venv\Scripts\activate
pip install -r requirements.txt
python etl.py --config config.yaml
```

No Windows, você pode usar: `run_etl.bat`

## Pré-requisitos
- Python 3.10+
- Pacotes em `requirements.txt` (inclui `pandas`, `requests`, `odfpy`, `pyyaml`).
- Acesso à internet para baixar o Bronze (ou já ter o arquivo em `data/bronze/`).

## 1) Preparação do ambiente
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
```

Opcional (Windows): execute `run_etl.bat` para rodar com defaults.

## 2) Como executar
```bash
cd "tecnica de integração"
python etl.py --config config.yaml
```

Flags úteis:
- `--skip-extract`: pula o download e reutiliza o Bronze existente
- `--skip-transform`: pula a transformação e reutiliza o Silver existente

## 3) Configuração (`config.yaml`)
Principais chaves:
- `paths`: diretórios das camadas `data/bronze`, `data/silver`, `data/gold`;
- `source.source_url`: URL pública do IBGE (ODS/CSV com população municipal);
- `files`: nomes padrão dos artefatos em cada camada;
- `gold.top_n`: número de municípios no ranking da Gold `gold_top_municipios.csv`.

O projeto está apontando para a estimativa `2025` em ODS:
`https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2025/estimativa_dou_2025.ods`.

## 4) Estrutura de diretórios
```
tecnica de integração/
  etl.py
  config.yaml
  requirements.txt
  README.md
  RELATORIO.md
  run_etl.bat
  src/
    utils.py
    transforms.py
  data/
    bronze/
      estimativa_dou_2025.ods
    silver/
      ibge_populacao_2025_clean.csv
    gold/
      gold_populacao_por_uf.csv
      gold_top_municipios.csv
  slides/
    Apresentacao.md
```

## 5) Camadas e modelo de dados
- Bronze: arquivo original (ODS/CSV) exatamente como baixado.
- Silver: colunas padronizadas para `cod_municipio`, `nome_municipio`, `uf`, `populacao`.
- Gold:
  - `gold_populacao_por_uf.csv(uf, populacao_total, municipios)`
  - `gold_top_municipios.csv(cod_municipio, nome_municipio, uf, populacao)`

Detalhes de transformação estão no `RELATORIO.md`.

## 6) Reprodutibilidade e tratamento de erros
- Download com retries/backoff e reaproveitamento do Bronze já existente.
- Autodetecção de formato (ODS/CSV) e limpeza robusta de cabeçalhos do IBGE.
- Validações no Silver: presença de colunas críticas, coersão de tipos e remoção de duplicatas/nulos.

## 7) Relatório do projeto
Consulte o documento `RELATORIO.md` para fonte de dados, justificativa, estrutura dos dados originais, etapas de transformação, modelo final e desafios/soluções.

## 8) Solução de problemas (rápido)
- Erro de URL/HTML salvo como Bronze: ajuste `source_url` e rode sem `--skip-extract`.
- Falha ao ler ODS: confirme `odfpy` instalado (`pip install odfpy`).
- Nomes de colunas diferentes do esperado: ver mapeamento em `src/transforms.py > harmonize_ibge_columns`.

## Slides
- Slide deck em Markdown com diagrama Bronze → Silver → Gold:
  - `tecnica de integração/slides/Apresentacao.md`

---
Licença: uso educacional.
