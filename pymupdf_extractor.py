import os
import pymupdf
import pandas as pd
from typing import Literal

script_dir = os.path.dirname(os.path.abspath(__file__))

# Expected output columns — aligned with what the reconciliation process expects.
# Matches the reference code's columnas list (REF# is dropped before returning).
SCOTIA_COLUMNS = ["DATE", "TRANSACTION TYPE", "DESCRIPTION", "DEBITS", "CREDITS", "BALANCE"]
SAGICOR_COLUMNS = ["BOOKING DATE", "REFERENCE", "DESCRIPTION", "VALUE DATE", "DEBITS", "CREDITS", "CLOSING BALANCE"]


def get_doc_type(doc) -> Literal["scotia", "sagicor"]:
    first_page = doc[0]
    first_page_text = first_page.get_text()

    # TODO: maybe find better ways to detect this, for now it suffices
    if all([s in first_page_text for s in ["Account Number:", "Report Period:", "Account Currency:"]]):
        return "scotia"
    elif all([s in first_page_text for s in ["Account Statement", "Account :", "Customer :", "Currency :"]]):
        return "sagicor"
    else:
        raise ValueError("Invalid PDF or not accounted for")


def extract_for_scotia(doc) -> pd.DataFrame:
    """
    Extract transaction table from Scotia bank statements using PyMuPDF find_tables().

    Scotia PDFs have proper table borders so find_tables() works cleanly. Each page
    may repeat the header row — these are filtered out. The REF# column is dropped
    to match the expected output schema (aligns with the reference pdfplumber code).
    """
    all_table_dfs = []

    for page in doc:
        page_tables = page.find_tables()
        for page_table in page_tables:
            df = page_table.to_pandas()
            df_cols = list(df.columns)
            if len(df_cols) < 2:
                continue
            # TODO: validate all columns instead of just the first two
            if df_cols[0].strip() == "DATE" and df_cols[1] == "REF#":
                # Drop repeated header rows that appear at the top of each page
                df = df[df["DATE"].str.strip() != "DATE"].copy()
                df = df[df["DATE"].notna() & (df["DATE"].str.strip() != "")].copy()
                all_table_dfs.append(df)
            # other tables on the page (e.g. account info box) are skipped

    if not all_table_dfs:
        raise ValueError("No transaction table found in document — check that this is a Scotia statement")

    full_df = pd.concat(all_table_dfs, axis=0, ignore_index=True)

    # Drop REF# column (index 1) to match reference code output schema
    if full_df.shape[1] >= 2 and full_df.columns[1] == "REF#":
        full_df = full_df.drop(columns=["REF#"])

    # Rename to standard output column names
    if full_df.shape[1] == len(SCOTIA_COLUMNS):
        full_df.columns = SCOTIA_COLUMNS
    # else: column count mismatch — leave as-is and let caller handle it
    # TODO: see if column data requires normalization (dates → date type, amounts → float)
    #       or if the reconciliation process will handle that directly

    return full_df


def extract_for_sagicor(doc) -> pd.DataFrame:
    """
    Extract transaction table from Sagicor bank statements using PyMuPDF find_tables().

    Sagicor PDFs have no table borders, so find_tables() with lines strategy treats
    each text row as its own 1-row "table". Column names in these micro-tables encode
    the cell data as "{col_index}-{value}" for non-empty cells and "Col{index}" for
    empty cells. process_table_content() strips those prefixes to recover the values.

    Tested multiple combinations of vertical/horizontal strategies and min_words configs.
    lines+lines proved most reliable. Other strategies either collapsed all rows or
    missed cells entirely. TODO: see if any other config yields better results.

    Sample output (one "table" per transaction row):
      0-01 DEC 25  1-FT25335P6BCN\\SBJ  2-Bill Payment via e-ba\\nnk  3-01 DEC 25  Col4  5-9,000.00  6-17,698,294.24
      [None,        None,                 None,                          None,         None, None,        None          ]
    """

    def process_table_content(content):
        new_content = []
        for i, c in enumerate(content):
            if c.strip() == f"Col{i}":  # empty cells appear as "ColN" column names
                new_content.append(None)
                continue
            new_content.append(c.replace(f"{i}-", "").strip())  # strip index prefix
        return new_content

    table_cols = []
    table_content = []

    for page in doc:
        page_tables = page.find_tables(
            vertical_strategy="lines",
            horizontal_strategy="lines",
        )
        for page_table in page_tables:
            df = page_table.to_pandas()
            df_cols = list(df.columns)
            if df_cols[0] == "Booking Date" and df_cols[1] == "Reference":
                # This is the header row — capture column names
                table_cols = df_cols
            else:
                table_content.append(process_table_content(df_cols))

    if not table_content:
        raise ValueError("No transaction rows found — check that this is a Sagicor statement")

    table = pd.DataFrame(table_content, columns=table_cols if table_cols else None)

    # Normalize to the expected output schema when column count matches
    if table.shape[1] == len(SAGICOR_COLUMNS):
        table.columns = SAGICOR_COLUMNS

    return table

def run_extraction(filepath, out_path):
    print(f"\nReading: {filepath}")
    doc = pymupdf.open(filepath)

    doc_type = get_doc_type(doc)
    print(f"Detected type: {doc_type}")

    if doc_type == "scotia":
        doc_df = extract_for_scotia(doc)
    elif doc_type == "sagicor":
        doc_df = extract_for_sagicor(doc)

    doc.close()

    print(f"Shape: {doc_df.shape}")
    # print(doc_df.head(10).to_string())
    doc_df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    filenames = [
        # "7516 f.pdf",
        # "380911 SC.pdf",
        {"input": "FEB 2026/Sagicor Statement 5500120338.pdf",
         "output": "FEB 2026/EB_11136480.xlsx"
         },
         {"input": "JAN 2026/5500120338.pdf",
          "output": "JAN 2026/EB_11136480.xlsx"
         },
         {"input": "DEC 2025/5500120338.pdf",
          "output": "DEC 2025/EB_11136480.xlsx"
        },
        {"input": "NOV 2025/5500120338 NOV 1-15,2025.pdf",
         "output": "NOV 2025/EB_11136480-1.xlsx"
        },
        {"input": "NOV 2025/5500120338 NOV 15-30,2025.pdf",
         "output": "NOV 2025/EB_11136480-2.xlsx"
        }

    ]

    for filename in filenames:
        filepath = os.path.join(script_dir, "Bank Statements", filename["input"])
        out_path = os.path.join(script_dir, "Bank Statements", filename["output"])

        run_extraction(filepath, out_path)
