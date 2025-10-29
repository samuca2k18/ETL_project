import os
import sys
import argparse
import requests
import pandas as pd
import yaml
from pathlib import Path
import time
import re
import unicodedata


from src.utils import ensure_dirs, safe_path
from src.transforms import silver_pipeline, gold_populacao_por_uf, gold_top_municipios


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _score_header(cols_norm):
    """Pontua um conjunto de colunas normalizadas para preferir a tabela de municípios."""
    cols_str = " ".join(cols_norm)
    score = 0
    # Queremos MUNICÍPIO
    if "municip" in cols_str:
        score += 5
    # UF também ajuda
    if " uf " in f" {cols_str} " or cols_norm.count("uf") >= 1:
        score += 2
    # População é essencial
    if "populac" in cols_str:
        score += 5
    # Código ajuda
    if "cod" in cols_str or "codigo" in cols_str:
        score += 2
    return score

def read_ibge_ods_smart(bronze_fp: str) -> pd.DataFrame:
    """Lê ODS/XLSX do IBGE e tenta encontrar:
       - a aba de MUNICÍPIOS (não a de UF),
       - a linha correta de cabeçalho.
    """
    ext = Path(bronze_fp).suffix.lower()
    engine = "odf" if ext == ".ods" else None

    # Lê todas as abas, sem cabeçalho, como texto
    sheets = pd.read_excel(bronze_fp, sheet_name=None, engine=engine, header=None, dtype=str)
    best_df = None
    best_score = -1
    best_rows = -1

    for sheet_name, raw in sheets.items():
        # Limpa linhas completamente vazias no topo
        raw = raw.dropna(how="all").reset_index(drop=True)
        if raw.empty:
            continue

        # Tenta as primeiras 50 linhas como possíveis cabeçalhos
        max_rows = min(50, len(raw))
        for i in range(max_rows):
            header_row = raw.iloc[i].tolist()
            cols = [str(x).strip() for x in header_row]
            cols_norm = [_norm(c) for c in cols]
            # precisa ter algo que pareça cabeçalho (mais de 2 colunas não vazias)
            non_empty = sum(1 for c in cols_norm if c not in ("", "nan"))
            if non_empty < 2:
                continue

            score = _score_header(cols_norm)
            if score <= 0:
                continue  # não parece tabela útil

            # constrói df com esse header
            tmp = raw.copy()
            tmp.columns = cols
            tmp = tmp.iloc[i+1:].reset_index(drop=True)
            # remove colunas totalmente vazias
            tmp = tmp.loc[:, ~tmp.columns.astype(str).str.fullmatch(r"\s*nan\s*", case=False, na=False)]
            tmp = tmp.dropna(how="all")
            # Heurística: preferir tabelas "grandes" (municípios ~5k linhas)
            rows = len(tmp)

            # só aceita se houver alguma coluna sugerindo 'município'
            has_munic_col = any("municip" in _norm(c) for c in tmp.columns)
            if not has_munic_col:
                # provavelmente é a aba de UF; ignore
                continue

            # escolhe o melhor candidato por score e tamanho
            if (score > best_score) or (score == best_score and rows > best_rows):
                best_df = tmp
                best_score = score
                best_rows = rows

    if best_df is not None:
        return best_df

    # Fallback (se nada bateu): usa leitura padrão (pode exigir ajustes depois)
    return pd.read_excel(bronze_fp, engine=engine)

def read_bronze_any(bronze_fp: str) -> pd.DataFrame:
    ext = Path(bronze_fp).suffix.lower()
    if ext in [".ods", ".xls", ".xlsx"]:
        try:
            return read_ibge_ods_smart(bronze_fp)
        except Exception:
            engine = "odf" if ext == ".ods" else None
            return pd.read_excel(bronze_fp, engine=engine)
    if ext in [".csv", ".txt"]:
        try:
            return pd.read_csv(bronze_fp, encoding="utf-8", sep=None, engine="python", on_bad_lines="skip")
        except UnicodeDecodeError:
            try:
                return read_ibge_ods_smart(bronze_fp)
            except Exception:
                return pd.read_excel(bronze_fp)
    # assinatura zip (xlsx/ods) mesmo com extensão trocada
    with open(bronze_fp, "rb") as f:
        sig = f.read(4)
    if sig[:2] == b"PK":
        try:
            return read_ibge_ods_smart(bronze_fp)
        except Exception:
            return pd.read_excel(bronze_fp)
    # último recurso: CSV
    return pd.read_csv(bronze_fp, encoding="utf-8", sep=None, engine="python", on_bad_lines="skip")



def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def extract_to_bronze(cfg: dict) -> str:
    bronze_dir = cfg["paths"]["bronze"]
    ensure_dirs(bronze_dir)

    primary = cfg["source"]["source_url"]
    fallbacks = cfg["source"].get("fallback_urls", [])
    urls_to_try = [primary] + fallbacks

    bronze_fp = safe_path(bronze_dir, cfg["files"]["bronze_filename"])

    # Se já existe, reaproveita (evita bater no rate limit sem necessidade)
    if os.path.exists(bronze_fp):
        print(f"[Extract] Bronze já existe, reaproveitando: {bronze_fp}")
        return bronze_fp

    headers = {
        "User-Agent": "etl-ibge-pipeline/1.0 (+https://example.local)",
        "Accept": "text/csv,application/octet-stream,*/*;q=0.8",
    }

    # Tenta cada URL com retry/backoff exponencial
    last_err = None
    for url in urls_to_try:
        print(f"[Extract] Baixando dados de: {url}")
        for attempt in range(5):  # até 5 tentativas por URL
            try:
                r = requests.get(url, headers=headers, timeout=60)
                status = r.status_code
                if status == 200 and r.content:
                    with open(bronze_fp, "wb") as f:
                        f.write(r.content)
                    print(f"[Extract] Salvo em: {bronze_fp}")
                    return bronze_fp
                else:
                    # 429 ou 5xx → backoff; outros códigos → tenta próximo espelho
                    if status in (429, 500, 502, 503, 504):
                        retry_after = r.headers.get("Retry-After")
                        wait = int(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
                        print(f"[Extract] HTTP {status}. Aguardando {wait}s e tentando de novo…")
                        time.sleep(wait)
                        continue
                    else:
                        print(f"[Extract] HTTP {status} nesta URL, tentando próximo espelho…")
                        break
            except requests.RequestException as e:
                last_err = e
                wait = 2 ** attempt
                print(f"[Extract] Erro de rede ({e}). Aguardando {wait}s e tentando de novo…")
                time.sleep(wait)
                continue

        print("[Extract] Mudando para o próximo espelho…")

    # Se chegou aqui, falhou em todas
    raise SystemError(f"Falha ao baixar a fonte após tentativas em todas as URLs. Último erro: {last_err}")


def transform_to_silver(cfg: dict, bronze_fp: str) -> str:
    silver_dir = cfg["paths"]["silver"]
    ensure_dirs(silver_dir)
    silver_fp = safe_path(silver_dir, cfg["files"]["silver_filename"])

    print(f"[Transform] Lendo Bronze: {bronze_fp}")

    # --- Guarda contra HTML/404 salvado como "CSV"
    with open(bronze_fp, "rb") as f:
        head = f.read(512).lower()
    if b"<html" in head or b"<body" in head:
        raise ValueError(
            "O arquivo Bronze não é um CSV válido (parece HTML/404). "
            "Verifique e corrija a 'source_url' no config.yaml e rode sem --skip-extract."
        )

    # --- Autodetecta separador e ignora linhas claramente ruins
    df = read_bronze_any(bronze_fp)
    print("[Debug] Primeiras colunas lidas:", list(df.columns)[:10])
    print("[Debug] Amostra:", df.head(3).to_dict(orient="records"))

    print("[Transform] Aplicando limpeza e padronização…")
    df = silver_pipeline(df)

    print(f"[Transform] Salvando Silver em: {silver_fp}")
    df.to_csv(silver_fp, index=False, encoding="utf-8")
    return silver_fp


def load_to_gold(cfg: dict, silver_fp: str) -> None:
    gold_dir = cfg["paths"]["gold"]
    ensure_dirs(gold_dir)

    print(f"[Load] Lendo Silver: {silver_fp}")
    df = pd.read_csv(silver_fp, encoding="utf-8")

    # Tabelas Gold
    by_state_fp = safe_path(gold_dir, cfg["files"]["gold_by_state"])
    top_fp      = safe_path(gold_dir, cfg["files"]["gold_topN"])

    print("[Load] Gerando gold_populacao_por_uf…")
    df_uf = gold_populacao_por_uf(df)
    df_uf.to_csv(by_state_fp, index=False, encoding="utf-8")

    print("[Load] Gerando gold_top_municipios…")
    top_n = int(cfg["gold"]["top_n"])
    df_top = gold_top_municipios(df, top_n=top_n)
    df_top.to_csv(top_fp, index=False, encoding="utf-8")

    print(f"[Load] Gold salvo em:\n - {by_state_fp}\n - {top_fp}")

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze→Silver→Gold (IBGE População 2022)")
    parser.add_argument("--config", default="config.yaml", help="Caminho do YAML de configuração")
    parser.add_argument("--skip-extract", action="store_true", help="Pula a etapa de Extract (usa Bronze existente)")
    parser.add_argument("--skip-transform", action="store_true", help="Pula a etapa de Transform (usa Silver existente)")
    args = parser.parse_args()

    cfg = load_config(args.config)

    bronze_fp = safe_path(cfg["paths"]["bronze"], cfg["files"]["bronze_filename"])
    silver_fp = safe_path(cfg["paths"]["silver"], cfg["files"]["silver_filename"])

    # EXTRACT
    if not args.skip_extract:
        bronze_fp = extract_to_bronze(cfg)
    else:
        print("[Extract] Pulado.")

    # TRANSFORM
    if not args.skip_transform:
        silver_fp = transform_to_silver(cfg, bronze_fp)
    else:
        print("[Transform] Pulado.")

    # LOAD
    load_to_gold(cfg, silver_fp)

if __name__ == "__main__":
    main()
