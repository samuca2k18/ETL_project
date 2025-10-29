# ETL IBGE População 2022 – Bronze / Silver / Gold

Pipeline de ETL em Python usando um dataset público do IBGE (Censo 2022 – população por município).  
Resultado organizado em três camadas:

- **Bronze (Raw):** CSV bruto, exatamente como baixado.
- **Silver (Refined):** dados limpos e padronizados (códigos IBGE, strings, tipos numéricos, remoção de duplicatas/nulos).
- **Gold (DW/Curado):** tabelas finais para análise:
  - `gold_populacao_por_uf.csv`: soma de população por UF + contagem de municípios;
  - `gold_top_municipios.csv`: TOP N municípios por população (configurável).

## 1) Preparação

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
