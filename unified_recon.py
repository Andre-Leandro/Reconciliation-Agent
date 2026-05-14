import os
import pandas as pd
import json
import io
from dotenv import load_dotenv

# 1. CONFIGURACION Y UTILIDADES

def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

def clean_amount(val):
    if pd.isna(val): return 0.0
    if isinstance(val, (int, float)): return float(val)
    val = str(val).replace('JMD', '').replace('USD', '').replace(',', '').strip()
    try: return float(val)
    except: return 0.0

def clean_date(val):
    if pd.isna(val) or val == "" or str(val).lower() == "nan": return ""
    try:
        return pd.to_datetime(val).strftime('%d/%m/%Y')
    except:
        return str(val)

def build_desc_lookup(df_bank):
    desc_col = 'DESCRIPTION'
    debits_col = 'DEBITS'
    credits_col = 'CREDITS'
    if desc_col not in df_bank.columns or (debits_col not in df_bank.columns and credits_col not in df_bank.columns):
        return pd.Series(dtype='object')
    parts = []
    if debits_col in df_bank.columns:
        deb = df_bank[[debits_col, desc_col]].copy()
        deb['amount_match'] = deb[debits_col].apply(clean_amount).abs()
        parts.append(deb[['amount_match', desc_col]])
    if credits_col in df_bank.columns:
        cred = df_bank[[credits_col, desc_col]].copy()
        cred['amount_match'] = cred[credits_col].apply(clean_amount).abs()
        parts.append(cred[['amount_match', desc_col]])
    if not parts:
        return pd.Series(dtype='object')
    eb_long = pd.concat(parts, ignore_index=True)
    eb_long = eb_long[eb_long['amount_match'] != 0]
    eb_long = eb_long.dropna(subset=[desc_col])
    return eb_long.groupby('amount_match')[desc_col].first()

def apply_part_filter(df, account_id, part_config):
    """
    df: DataFrame con todos los datos de SAP.
    account_id: El ID de la cuenta principal.
    part_config: El diccionario de configuracion de la parte (part1 o part2).
    """
    if not part_config:
        return pd.DataFrame(columns=df.columns)

    # 1. Filtrar por cuenta (si se especifica en la parte, usar esa, sino la principal)
    target_acc = part_config.get("account_id", account_id)
    if isinstance(target_acc, str):
        df_res = df[df['G/L Account'].astype(str).str.contains(target_acc, na=False)].copy()
    elif isinstance(target_acc, list):
        pattern = "|".join(target_acc)
        df_res = df[df['G/L Account'].astype(str).str.contains(pattern, na=False)].copy()
    else:
        df_res = df.copy()

    # 2. Filtrar por Journal Entry Type
    entry_types = part_config.get("entry_types", "")
    if entry_types:
        df_res = df_res[df_res['Journal Entry Type'].str.contains(entry_types, na=False, case=False)]

    # 3. Filtrar por patrones de texto
    patterns = part_config.get("patterns", [])
    search_in = part_config.get("search_in", "item_text") # item_text, description, or both
    contains = part_config.get("contains", True)

    if patterns:
        pattern_str = "|".join(patterns)
        
        mask_item = df_res['Journal Entry Item Text'].str.contains(pattern_str, na=False, regex=True, case=False)
        mask_desc = df_res['Description'].str.contains(pattern_str, na=False, regex=True, case=False)

        if search_in == "item_text":
            mask = mask_item
        elif search_in == "description":
            mask = mask_desc
        elif search_in == "both":
            mask = mask_item | mask_desc
        else:
            mask = mask_item

        if contains:
            df_res = df_res[mask]
        else:
            df_res = df_res[~mask]

    # 4. Filtro opcional por signo de monto
    amt_filter = part_config.get("amount_filter", "none")
    if amt_filter == "positive":
        df_res = df_res[df_res['Amount in Transaction Currency'] > 0]
    elif amt_filter == "negative":
        df_res = df_res[df_res['Amount in Transaction Currency'] < 0]

    return df_res

def match_by_amount(df1, df2):
    """Cruza df1 y df2 por monto absoluto uno a uno."""
    if df1.empty or df2.empty:
        return pd.DataFrame(columns=df1.columns), pd.DataFrame(columns=df2.columns), df1, df2
    
    matched_1 = []
    matched_2 = []
    df1_rem = df1.copy()
    df2_rem = df2.copy()
    
    for idx in df1.index:
        if idx not in df1_rem.index: continue
        row = df1_rem.loc[idx]
        amt = abs(row['Amount in Transaction Currency'])
        mask = df2_rem['Amount in Transaction Currency'].abs().round(2) == round(amt, 2)
        match_idx = df2_rem[mask].index
        if not match_idx.empty:
            m_idx = match_idx[0]
            matched_1.append(row.to_dict())
            matched_2.append(df2_rem.loc[m_idx].to_dict())
            df2_rem = df2_rem.drop(m_idx)
            df1_rem = df1_rem.drop(idx)
            
    return pd.DataFrame(matched_1), pd.DataFrame(matched_2), df1_rem, df2_rem

# 2. PROCESO PRINCIPAL

def run_reconciliation():
    ENV_FILE = os.getenv("RECON_ENV_FILE", "env.txt")
    env_path = ENV_FILE if os.path.isabs(ENV_FILE) else os.path.join(os.getcwd(), ENV_FILE)
    load_dotenv(env_path, override=False)

    BASE_DIR = os.getenv("RECON_ROOT", "")
    OUTPUT_BASE_DIR = os.getenv("RECON_OUTPUT_DIR_V2", "Automatic reconciliation V2")
    RECON_IS_S3 = os.getenv("RECON_IS_S3", "false")
    IS_S3 = RECON_IS_S3.lower() in {"1", "true", "yes"}
    
    S3FS = None
    if IS_S3:
        import s3fs
        S3FS = s3fs.S3FileSystem()

    def path_exists(path):
        return S3FS.exists(path) if IS_S3 else os.path.exists(path)

    def read_excel(path):
        if IS_S3:
            with S3FS.open(path, "rb") as f:
                return pd.read_excel(f)
        return pd.read_excel(path)

    config = load_config()
    
    COLUMNAS_SALIDA = [
        'Company Code', 'G/L Account', 'Journal Entry Type', 'Posting Key', 
        'Journal Entry', 'Journal Entry Item Text', 'Posting Date', 'Value date', 
        'Amount in Transaction Currency', 'Amount in Global Currency', 
        'Clearing Date', 'Clearing Journal Entry', 'Description'
    ]

    # Definimos los meses a procesar directamente en el código (Abril 2026)
    months = [
        {
            "month_name": "November",
            "month_number": "011",
            "year": "2025",
            "sap_file_pattern": "SAP Exports/NOV 2025.xlsx"
        },
        {
            "month_name": "December",
            "month_number": "012",
            "year": "2025",
            "sap_file_pattern": "SAP Exports/DEC 2025.xlsx"
        },
        {
            "month_name": "January",
            "month_number": "001",
            "year": "2026",
            "sap_file_pattern": "SAP Exports/JAN 2026.xlsx"
        },
        {
            "month_name": "February",
            "month_number": "002",
            "year": "2026",
            "sap_file_pattern": "SAP Exports/FEB 2026.xlsx"
        },
        {
            "month_name": "April",
            "month_number": "003",
            "year": "2026",
            "sap_file_pattern": "SAP Exports/APR 2026.xlsx"
        }
    ]

    for acc_config in config["accounts"]:
        acc_id = acc_config["account_id"]
        use_bank = acc_config.get("use_bank_file", True)
        
        for m in months:
            month_name = m["month_name"]
            month_num = m["month_number"]
            year = m["year"]
            sap_path = os.path.join(BASE_DIR, m["sap_file_pattern"])
            
            # El archivo bancario suele ser EB_<account>.xlsx en la carpeta del mes
            month_folder = f"{month_name[:3].upper()} {year}"
            bank_filename = acc_config.get("bank_filename", f"EB_{acc_id}.xlsx")
            bank_path = os.path.join(BASE_DIR, "Bank Statements", month_folder, bank_filename)

            if not path_exists(sap_path):
                print(f"Skipping {acc_id} - {month_name}: SAP file not found: {sap_path}")
                continue

            df_sap = read_excel(sap_path)
            
            # Limpieza inicial de SAP
            for col in ['Amount in Transaction Currency', 'Amount in Global Currency', 'Amount in Company Code Currency']:
                if col in df_sap.columns:
                    df_sap[col] = df_sap[col].apply(clean_amount)
            
            df_sap['G/L Account'] = df_sap['G/L Account'].astype(str)
            df_sap['Journal Entry Type'] = df_sap['Journal Entry Type'].astype(str)
            df_sap['Journal Entry Item Text'] = df_sap['Journal Entry Item Text'].astype(str).fillna("")

            # --- FILTRO POR FECHA (POSTING DATE) ---
            if 'Posting Date' in df_sap.columns:
                df_sap['Posting Date DT'] = pd.to_datetime(df_sap['Posting Date'], errors='coerce')
                target_year = int(year)
                
                # Mapeo simple de nombres de meses en inglés a números
                month_map = {
                    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
                    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12
                }
                target_month = month_map.get(month_name)
                
                if target_month:
                    mask_date = (df_sap['Posting Date DT'].dt.month == target_month) & \
                                (df_sap['Posting Date DT'].dt.year == target_year)
                    df_sap = df_sap[mask_date].copy()

            # Lógica de descripción bancaria
            if use_bank and path_exists(bank_path):
                df_bank_stmt = read_excel(bank_path)
                desc_lookup = build_desc_lookup(df_bank_stmt)
                df_sap['Description'] = df_sap['Amount in Transaction Currency'].abs().map(desc_lookup).fillna("")
            else:
                df_sap['Description'] = ""

            # Preparar salida
            output_dir = os.path.join(OUTPUT_BASE_DIR, acc_id)
            if not IS_S3:
                os.makedirs(output_dir, exist_ok=True)
            
            output_file = os.path.join(output_dir, f"{acc_id}_{month_name}_{year}_V2.xlsx")
            
            if IS_S3:
                output_buffer = io.BytesIO()
                writer = pd.ExcelWriter(output_buffer, engine='xlsxwriter')
            else:
                writer = pd.ExcelWriter(output_file, engine='xlsxwriter')

            workbook = writer.book
            fmt_jam = workbook.add_format({'num_format': '#,##0.00 "JMD"'}) 
            fmt_usd = workbook.add_format({'num_format': '#,##0.00 "USD"'}) 
            fmt_std = workbook.add_format({'num_format': '#,##0.00'})

            # Cache for remainders (used in 111367-111368)
            remainders = {}

            for tab in acc_config["tabs"]:
                df_p1 = apply_part_filter(df_sap, acc_id, tab["part1"])
                df_p2 = apply_part_filter(df_sap, acc_id, tab["part2"])

                # Lógica especial de match/remainder para 111367-111368
                if tab.get("use_match"):
                    df_p1_matched, df_p2_matched, df_p1_rem, df_p2_rem = match_by_amount(df_p1, df_p2)
                    # El usuario quiere que en Cashier 3 SOLO queden los que matchearon, pero separados
                    df_p1 = df_p1_matched
                    df_p2 = df_p2_matched
                    remainders[tab["name"]] = (df_p1_rem, df_p2_rem)
                
                # Cargar remainders si se pide
                if tab.get("load_remainder_from"):
                    source_tab = tab["load_remainder_from"]
                    if source_tab in remainders:
                        # Por defecto cargamos el remainder de la parte 2 (bank side)
                        rem_df = remainders[source_tab][1]
                        df_p2 = pd.concat([df_p2, rem_df], ignore_index=True)

                total_p1 = df_p1['Amount in Transaction Currency'].sum() if not df_p1.empty else 0
                total_p2 = df_p2['Amount in Transaction Currency'].sum() if not df_p2.empty else 0
                diff = total_p1 + total_p2
                
                prefix = tab.get("prefix", "Diff")
                # El formato de nom_text varia un poco entre notebooks, usaremos uno estandarizado pero flexible
                nom_text = f"{prefix} {abs(total_p1):.2f} {abs(total_p2):.2f} - {month_name} {year}"

                filas = df_p1.to_dict('records')
                filas.append({})
                filas.append({'Amount in Transaction Currency': total_p1})
                filas.append({})
                
                if not df_p2.empty:
                    filas.extend(df_p2.to_dict('records'))
                    filas.append({})
                    filas.append({'Amount in Transaction Currency': total_p2})
                    filas.append({})
                    filas.append({
                        'Amount in Transaction Currency': diff,
                        'Amount in Global Currency': nom_text
                    })
                else:
                    # Si no hay parte 2, igual ponemos el total de parte 1 al final
                    filas.append({
                        'Amount in Transaction Currency': total_p1,
                        'Amount in Global Currency': nom_text
                    })

                df_sheet = pd.DataFrame(filas)
                
                # Formatear fechas
                for date_col in ['Posting Date', 'Value date', 'Clearing Date']:
                    if date_col in df_sheet.columns:
                        df_sheet[date_col] = df_sheet[date_col].apply(clean_date)

                # Asegurar columnas
                for col in COLUMNAS_SALIDA:
                    if col not in df_sheet.columns: df_sheet[col] = ""
                df_sheet = df_sheet[COLUMNAS_SALIDA]

                sheet_name = tab["name"][:31]
                df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)

                # Formateo visual
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(df_sheet.columns):
                    col_len = df_sheet[col].map(lambda x: len(str(x)) if pd.notna(x) else 0).max()
                    max_len = max(col_len, len(str(col))) + 2
                    
                    if col in ['Amount in Transaction Currency', 'Amount in Company Code Currency']:
                        worksheet.set_column(i, i, max_len, fmt_jam)
                    elif col == 'Amount in Global Currency':
                        worksheet.set_column(i, i, max_len, fmt_usd)
                    else:
                        worksheet.set_column(i, i, max_len)

            writer.close()
            if IS_S3:
                output_buffer.seek(0)
                with S3FS.open(output_file, "wb") as f:
                    f.write(output_buffer.getvalue())
            
            print(f"Report Generated: {output_file}")

if __name__ == "__main__":
    run_reconciliation()
