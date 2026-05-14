import os
import pandas as pd
from datetime import datetime
import glob
from dotenv import load_dotenv

# 1. CONFIGURACION INICIAL
ENV_FILE = os.getenv("RECON_ENV_FILE", "env.txt")
env_path = ENV_FILE if os.path.isabs(ENV_FILE) else os.path.join(os.getcwd(), ENV_FILE)
load_dotenv(env_path, override=False)

BASE_DIR = os.getenv("RECON_ROOT", "")
OUTPUT_BASE_DIR = os.getenv("RECON_OUTPUT_DIR", "Automatic reconciliation")
RECON_IS_S3 = os.getenv("RECON_IS_S3", "false")
IS_S3 = RECON_IS_S3.lower() in {"1", "true", "yes"}

S3FS = None
if IS_S3:
    import s3fs
    S3FS = s3fs.S3FileSystem()

# HARDCODED CONFIGURATION (Resto hardcodeado como pediste)
ACCOUNT = "11136380"
OFFICE_NO = 1106
SAP_REL_PATH = "SAP Exports/daily.xlsx"
LIBERATE_REL_PATH = "Liberate Reports"

# Helper para unir rutas (S3 o Local)
def join_path(base, *parts):
    if IS_S3:
        base_clean = base.rstrip("/")
        tail = "/".join(p.strip("/") for p in parts if p is not None)
        return f"{base_clean}/{tail}" if tail else base_clean
    if base:
        return os.path.join(base, *parts)
    return os.path.join(*parts)

def path_exists(path):
    return S3FS.exists(path) if IS_S3 else os.path.exists(path)

def read_excel_path(path):
    if IS_S3:
        with S3FS.open(path, "rb") as f:
            return pd.read_excel(f)
    return pd.read_excel(path)

def read_csv_path(path):
    if IS_S3:
        with S3FS.open(path, "rb") as f:
            return pd.read_csv(f, on_bad_lines='skip', low_memory=False)
    return pd.read_csv(path, on_bad_lines='skip', low_memory=False)

def list_files_path(pattern):
    if IS_S3:
        # s3fs.glob works with s3 paths
        return S3FS.glob(pattern)
    return glob.glob(pattern)

# Built paths
SAP_FILE = join_path(BASE_DIR, SAP_REL_PATH)
LIBERATE_BASE_DIR = join_path(BASE_DIR, LIBERATE_REL_PATH)
OUTPUT_DIR = join_path(OUTPUT_BASE_DIR, ACCOUNT, "Daily")

# Columns for output
COLUMNAS_SALIDA = [
    'G/L Account', 'Journal Entry', 'Company Code', 'Journal Entry Type', 'Posting Date', 
    'Amount in Company Code Currency', 'Journal Entry Item Text',
    'Value date', 'Amount in Transaction Currency', 
    'Offsetting Account', 'Amount in Global Currency', 'Assignment Reference', 
    'Clearing Date', 'Journal Entry Item', 'Fiscal Period', 'Clearing Journal Entry',
    'G/L Account Name', 'Match Info'
]

def clean_amount(val):
    if pd.isna(val): return 0.0
    if isinstance(val, (int, float)): return float(val)
    val = str(val).replace('JMD', '').replace('USD', '').replace(',', '').strip()
    try: return float(val)
    except: return 0.0

def get_liberate_path(date):
    # date is a datetime object
    month_year = date.strftime("%b %Y").upper() # e.g. JAN 2026
    day_str = date.strftime("%d-%b-%Y").upper() # e.g. 05-JAN-2026
    
    folder = join_path(LIBERATE_BASE_DIR, month_year)
    if not path_exists(folder):
        return None
    
    # Search for file containing the date
    pattern = join_path(folder, f"*_{day_str}*.CSV")
    files = list_files_path(pattern)
    if files:
        # S3FS returns paths without s3:// prefix usually, or it depends on config
        # but read_csv_path expects what s3fs.open needs.
        return files[0]
    return None

def process_daily_reconciliation():
    if not IS_S3 and not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Reading SAP file: {SAP_FILE}")
    df_sap = read_excel_path(SAP_FILE)
    
    # Clean amounts
    for col in ['Amount in Transaction Currency', 'Amount in Global Currency']:
        if col in df_sap.columns:
            df_sap[col] = df_sap[col].apply(clean_amount)
            
    # Filter by account
    df_sap = df_sap[df_sap['G/L Account'].astype(str).str.contains(ACCOUNT, na=False)].copy()
    
    # Find XQ rows for Office 1106
    xq_condition = (df_sap['Journal Entry Type'].str.contains('XQ', na=False, case=False)) & \
                   (df_sap['Journal Entry Item Text'].str.contains(str(OFFICE_NO), na=False)) & \
                   (pd.isna(df_sap['Clearing Journal Entry']) | (df_sap['Clearing Journal Entry'].astype(str).str.strip() == ""))
    
    df_xq_relevant = df_sap[xq_condition].copy()
    unique_dates = df_xq_relevant['Posting Date'].unique()
    
    # 2. Pool all UNCLEARED bank movements (ZB/ZR/BR)
    bank_condition = (df_sap['Journal Entry Type'].str.contains('ZB|ZR|BR', na=False, case=False)) & \
                     (pd.isna(df_sap['Clearing Journal Entry']) | (df_sap['Clearing Journal Entry'].astype(str).str.strip() == ""))
    df_bank_pool = df_sap[bank_condition].copy()
    df_bank_pool['Match Info'] = ""
    df_bank_pool['matched_date'] = None
    
    all_output_blocks = []
    
    for pdate in sorted(unique_dates):
        pdate_dt = pd.to_datetime(pdate)
        pdate_str = pdate_dt.strftime('%d.%m.%Y')
        print(f"Processing date: {pdate_str}")
        
        # 1. Get XQ rows for this date
        df_xq_day = df_sap[
            (df_sap['Posting Date'] == pdate) & 
            (df_sap['Journal Entry Type'].str.contains('XQ', na=False, case=False)) & 
            (df_sap['Journal Entry Item Text'].str.contains(str(OFFICE_NO), na=False)) &
            (pd.isna(df_sap['Clearing Journal Entry']) | (df_sap['Clearing Journal Entry'].astype(str).str.strip() == ""))
        ].copy()
        
        if df_xq_day.empty:
            continue

        # 2. Get Liberate report for this date
        lib_path = get_liberate_path(pdate_dt)
        df_lib_unmatched = pd.DataFrame()
        df_bank_matched_this_day = pd.DataFrame(columns=df_bank_pool.columns)

        if not lib_path:
            print(f"Warning: Liberate report not found for {pdate_str}")
        else:
            print(f"  Reading Liberate: {lib_path}")
            try:
                df_lib = read_csv_path(lib_path)
            except Exception as e:
                print(f"  Error reading Liberate {lib_path}: {e}")
                df_lib = pd.DataFrame()

            if not df_lib.empty and 'Office No' in df_lib.columns:
                df_lib = df_lib[df_lib['Office No'].astype(str).str.strip() == str(OFFICE_NO)].copy()
                
                if not df_lib.empty:
                    df_lib['Acct Payment Amount'] = df_lib['Acct Payment Amount'].apply(clean_amount)
                    df_lib['matched'] = False
                    
                    levels = [
                        ['Payment Date', 'Batch No.'],
                        ['Payment Date', 'Batch No.', 'Acct No'],
                        ['Payment Date', 'Batch No.', 'Acct No', 'Payment No ']
                    ]
                    
                    for idx, bank_row in df_bank_pool[df_bank_pool['matched_date'].isna()].iterrows():
                        sap_amount = bank_row['Amount in Transaction Currency']
                        if round(sap_amount, 2) == 0: continue
                        
                        match_found = False
                        for level_idx, level in enumerate(levels, 1):
                            if match_found: break
                            if not all(col in df_lib.columns for col in level): continue

                            unmatched_lib = df_lib[~df_lib['matched']]
                            if unmatched_lib.empty: break
                            
                            grouped = unmatched_lib.groupby(level)['Acct Payment Amount'].sum().reset_index()
                            potential_matches = grouped[grouped['Acct Payment Amount'].round(2).abs() == abs(round(sap_amount, 2))]
                            
                            if not potential_matches.empty:
                                match_row = potential_matches.iloc[0]
                                mask = pd.Series(True, index=df_lib.index)
                                for col in level:
                                    mask &= (df_lib[col] == match_row[col])
                                
                                df_lib.loc[mask & ~df_lib['matched'], 'matched'] = True
                                match_found = True
                                df_bank_pool.at[idx, 'Match Info'] = f"Level {level_idx}"
                                df_bank_pool.at[idx, 'matched_date'] = pdate
                        
                        if not match_found:
                            unmatched_lib = df_lib[~df_lib['matched']]
                            potential_matches = unmatched_lib[unmatched_lib['Acct Payment Amount'].round(2).abs() == abs(round(sap_amount, 2))]
                            if not potential_matches.empty:
                                match_idx = potential_matches.index[0]
                                df_lib.at[match_idx, 'matched'] = True
                                match_found = True
                                df_bank_pool.at[idx, 'Match Info'] = "Level 4 (Individual)"
                                df_bank_pool.at[idx, 'matched_date'] = pdate
                    
                    df_lib_unmatched = df_lib[~df_lib['matched']].copy()
                    df_bank_matched_this_day = df_bank_pool[df_bank_pool['matched_date'] == pdate].copy()

        # 3. CONSTRUCT OUTPUT BLOCK
        total_xq = df_xq_day['Amount in Transaction Currency'].sum()
        total_bank = df_bank_matched_this_day['Amount in Transaction Currency'].sum() if not df_bank_matched_this_day.empty else 0
        diferencia = total_xq + total_bank
        
        sap_rows = pd.concat([df_xq_day, df_bank_matched_this_day], ignore_index=True)
        
        if not sap_rows.empty:
            for col in COLUMNAS_SALIDA:
                if col not in sap_rows.columns: sap_rows[col] = ""
            
            sap_rows['Amount in Company Code Currency'] = sap_rows.apply(
                lambda x: x['Amount in Transaction Currency'] if pd.notna(x.get('G/L Account')) and str(x.get('G/L Account')) != "" else "", 
                axis=1
            )
            sap_rows['Fiscal Period'] = sap_rows.apply(
                lambda x: pd.to_datetime(x['Posting Date']).strftime('%m') if pd.notna(x.get('Posting Date')) else "", 
                axis=1
            )
            sap_rows = sap_rows[COLUMNAS_SALIDA]

        all_output_blocks.append(('SAP_HEADER', pd.DataFrame([sap_rows.columns.tolist()], columns=sap_rows.columns)))
        all_output_blocks.append(('SAP', sap_rows))
        all_output_blocks.append(('SPACE', pd.DataFrame([['']] * 2)))
        
        diff_data = {col: "" for col in COLUMNAS_SALIDA}
        diff_data['Offsetting Account'] = f"NET DIFFERENCE - {pdate_str}"
        diff_data['Amount in Transaction Currency'] = diferencia
        diff_df = pd.DataFrame([diff_data], columns=COLUMNAS_SALIDA)
        all_output_blocks.append(('SAP', diff_df))
        all_output_blocks.append(('SPACE', pd.DataFrame([['']] * 2)))
        
        if not df_lib_unmatched.empty:
            lib_header = pd.DataFrame([['--- UNMATCHED LIBERATE ---'] + [''] * (len(df_lib_unmatched.columns) - 1)], columns=df_lib_unmatched.columns)
            df_lib_out = df_lib_unmatched.drop(columns=['matched'])
            lib_cols_header = pd.DataFrame([df_lib_out.columns.tolist()], columns=df_lib_out.columns)
            lib_combined = pd.concat([lib_header, lib_cols_header, df_lib_out], ignore_index=True)
            all_output_blocks.append(('LIBERATE', lib_combined))
        
        all_output_blocks.append(('SPACE', pd.DataFrame([['']] * 3)))

    # FINAL EXCEL GENERATION
    output_filename = f"Daily_Recon_1106_{datetime.now().strftime('%Y%m%d')}.xlsx"
    output_path = join_path(OUTPUT_DIR, output_filename)
    
    import io
    output_buffer = io.BytesIO()
    writer = pd.ExcelWriter(output_buffer, engine='xlsxwriter')
    
    workbook = writer.book
    worksheet = workbook.add_worksheet('Daily Recon')
    
    fmt_currency = workbook.add_format({'num_format': '#,##0.00'})
    fmt_id = workbook.add_format({'num_format': '0'})
    fmt_date = workbook.add_format({'num_format': 'dd.mm.yyyy'})
    fmt_header_sap = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
    fmt_header_lib = workbook.add_format({'bold': True, 'bg_color': '#FFC7CE', 'border': 1, 'font_color': '#9C0006'})
    fmt_bold = workbook.add_format({'bold': True})

    id_cols = ['Journal Entry', 'Clearing Journal Entry', 'Office No', 'Batch No.', 'Batch No', 'Acct No', 'Till Id', 'Cashier Id', 'Payment No', 'Payment No ', 'Journal Entry Item']
    date_cols = ['Posting Date', 'Value date', 'Clearing Date', 'Payment Date']

    current_row = 0
    for b_type, df in all_output_blocks:
        if len(df) == 0: continue
        
        for i, row in df.iterrows():
            for j, val in enumerate(row):
                if b_type in ['SAP', 'SAP_HEADER']:
                    col_name = df.columns[j]
                else: col_name = ""
                
                if pd.isna(val): val = ""
                cell_fmt = None
                
                if b_type == 'SAP_HEADER': cell_fmt = fmt_header_sap
                elif b_type == 'LIBERATE' and i <= 1: cell_fmt = fmt_header_lib
                
                if b_type == 'SAP' and isinstance(val, str) and "NET DIFFERENCE" in val:
                    cell_fmt = fmt_bold

                if isinstance(val, (int, float)):
                    if any(id_match in col_name for id_match in id_cols): cell_fmt = cell_fmt or fmt_id
                    elif 'Amount' in col_name: cell_fmt = cell_fmt or fmt_currency
                
                if col_name and any(date_match in col_name for date_match in date_cols):
                    if pd.notna(val) and val != "":
                        try:
                            val_dt = pd.to_datetime(val)
                            if pd.notna(val_dt):
                                val = val_dt
                                cell_fmt = cell_fmt or fmt_date
                        except: pass
                
                if cell_fmt:
                    if val == "": worksheet.write(current_row, j, "", cell_fmt)
                    else: worksheet.write(current_row, j, val, cell_fmt)
                else:
                    worksheet.write(current_row, j, val if val != "" else "")
            current_row += 1

    worksheet.set_column(0, 50, 18)
    writer.close()

    if IS_S3:
        with S3FS.open(output_path, "wb") as f:
            f.write(output_buffer.getvalue())
        print(f"Reconciliation completed. Output saved to S3: {output_path}")
    else:
        with open(output_path, "wb") as f:
            f.write(output_buffer.getvalue())
        print(f"Reconciliation completed. Output saved to: {output_path}")

if __name__ == "__main__":
    process_daily_reconciliation()
