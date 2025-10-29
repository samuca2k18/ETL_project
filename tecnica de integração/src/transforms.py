import pandas as pd
import re
import unicodedata

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Padroniza nomes de colunas (lowercase, sem espaços)
    mapping = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=mapping)
    return df

def standardize_ibge_code(df: pd.DataFrame) -> pd.DataFrame:
    # Garante 7 dígitos no código IBGE do município
    if "cod_municipio" in df.columns:
        df["cod_municipio"] = df["cod_municipio"].astype(str).str.zfill(7)
    return df

def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    # percorre por índice para não reavaliar dtypes a cada loop
    for col in list(df.columns):
        # Seleção robusta: se vier DataFrame por duplicidade, pega a 1ª
        s = df[col]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]

        if pd.api.types.is_object_dtype(s):
            s = s.astype(str).str.strip()
            df[col] = s

    if "nome_municipio" in df.columns:
        df["nome_municipio"] = df["nome_municipio"].astype(str).str.title()
    if "uf" in df.columns:
        df["uf"] = df["uf"].astype(str).str.upper()
    return df

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if "populacao" in df.columns:
        df["populacao"] = pd.to_numeric(df["populacao"], errors="coerce")
    return df

def drop_obvious_issues(df: pd.DataFrame) -> pd.DataFrame:
    # Remove duplicatas, se possível
    subset = [c for c in ["cod_municipio", "nome_municipio", "uf"] if c in df.columns]
    if subset:
        df = df.drop_duplicates(subset=subset)

    # Verifica campos críticos antes do dropna
    required = ["cod_municipio", "nome_municipio", "uf", "populacao"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Colunas obrigatórias ausentes após harmonização: {missing}. "
                       f"Colunas atuais: {list(df.columns)}")

    # Drop de nulos críticos
    df = df.dropna(subset=required, how="any")
    return df


def _norm(s: str) -> str:
    # remove acentos/pontuação e padroniza
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)      # troca pontuação por espaço
    s = re.sub(r"\s+", " ", s).strip()
    return s

def harmonize_ibge_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapeia cabeçalhos variados do IBGE (ODS/XLS/CSV) para:
      cod_municipio, nome_municipio, uf, populacao
    """
    mapping = {}
    for c in df.columns:
        n = _norm(c)
        # código do município
        if ("cod" in n or "codigo" in n) and ("munic" in n or "municip" in n):
            mapping[c] = "cod_municipio"
            continue
        # nome do município
        if ("municipio" in n or "nome do municipio" in n or ("nome" in n and "municip" in n)):
            mapping[c] = "nome_municipio"
            continue
        # UF
        if n == "uf" or "unidade federativa" in n:
            mapping[c] = "uf"
            continue
        # população (pega qualquer coluna que contenha 'populacao')
        if "populacao" in n:
            mapping[c] = "populacao"
            continue
    if mapping:
        df = df.rename(columns=mapping)
    return df


def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    seen = {}
    new_cols = []
    for c in map(str, df.columns):
        base = c.strip()
        if base not in seen:
            seen[base] = 0
            new_cols.append(base)
        else:
            seen[base] += 1
            new_cols.append(f"{base}_{seen[base]}")
    df.columns = new_cols
    return df

def silver_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    # 0) nomes únicos para evitar seleção como DataFrame
    df = ensure_unique_columns(df)

    # 1) harmoniza nomes vindos do IBGE (CÓD. MUNIC, NOME DO MUNICÍPIO, etc.)
    df = harmonize_ibge_columns(df)

    # 2) normaliza nomes (lowercase, underscores)
    df = normalize_columns(df)

    # 3) padroniza código IBGE (7 dígitos)
    df = standardize_ibge_code(df)

    # 4) limpeza de strings e tipos
    df = clean_strings(df)
    df = coerce_types(df)

    # 5) validação e remoção de nulos críticos (levanta KeyError informativo se faltar algo)
    df = drop_obvious_issues(df)
    return df

# ---- Camada Gold ----

def gold_populacao_por_uf(df: pd.DataFrame) -> pd.DataFrame:
    # Agrega população por UF
    group_cols = ["uf"]
    agg_df = (
        df.groupby(group_cols, as_index=False)
          .agg(populacao_total=("populacao", "sum"),
               municipios=("cod_municipio", "nunique"))
    )
    agg_df = agg_df.sort_values("populacao_total", ascending=False)
    return agg_df

def gold_top_municipios(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    cols = [c for c in ["cod_municipio", "nome_municipio", "uf", "populacao"] if c in df.columns]
    top = df[cols].sort_values("populacao", ascending=False).head(top_n)
    return top.reset_index(drop=True)
