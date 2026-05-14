import os
import re
import pymupdf
import pandas as pd
from typing import Literal

script_dir = os.path.dirname(os.path.abspath(__file__))

# Expected output columns — aligned with what the reconciliation process expects.
# Matches the reference code's columnas list (REF# is dropped before returning).
SCOTIA_COLUMNS = ["DATE", "TRANSACTION TYPE", "DESCRIPTION", "DEBITS", "CREDITS", "BALANCE"]
SAGICOR_COLUMNS = ["BOOKING DATE", "REFERENCE", "DESCRIPTION", "VALUE DATE", "DEBITS", "CREDITS", "CLOSING BALANCE"]
CIBC_COLUMNS = ["DATE", "DESCRIPTION", "DEBITS", "CREDITS", "BALANCE"]
NCB_COLUMNS = ["DATE", "REF", "MARKER", "CREDITS", "DEBITS", "BALANCE", "DESCRIPTION"]

# Left-boundary x-coordinates for NCB columns (used as vertical_lines in find_tables).
NCB_X_COL_DATE        = 41
NCB_X_COL_REF         = 113
NCB_X_COL_MARKER      = 185
NCB_X_COL_CREDITS     = 244
NCB_X_COL_DEBITS      = 334
NCB_X_COL_BALANCE     = 424
NCB_X_COL_DESCRIPTION = 509

_NCB_VERTICAL_LINES = [
    NCB_X_COL_REF, NCB_X_COL_MARKER, NCB_X_COL_CREDITS,
    NCB_X_COL_DEBITS, NCB_X_COL_BALANCE, NCB_X_COL_DESCRIPTION,
]


def get_doc_type(doc) -> Literal["scotia", "sagicor", "cibc", "ncb", ""]:
    first_page = doc[0]
    first_page_text = first_page.get_text()

    # TODO: maybe find better ways to detect this, for now it suffices
    if all([s in first_page_text for s in ["Account Number:", "Report Period:", "Account Currency:"]]):
        return "scotia"
    elif all([s in first_page_text for s in ["Account Statement", "Account :", "Customer :", "Currency :"]]):
        return "sagicor"
    elif all([s in first_page_text for s in ["ACCOUNT NUMBER", "STATEMENT OF ACCOUNT", "BRANCH", "TAX ID"]]):
        return "cibc"
    elif "Transactions List -" in first_page_text:
        return "ncb"
    else:
        return ""


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
                # We cast to string to avoid "AttributeError: Can only use .str accessor with string values"
                # which happens if the column contains only NaNs or numbers.
                df["DATE"] = df["DATE"].astype(str)
                df = df[df["DATE"].str.strip() != "DATE"].copy()
                df = df[(df["DATE"].str.strip() != "") & (df["DATE"].str.strip().str.lower() != "nan")].copy()
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


def extract_for_cibc(doc) -> pd.DataFrame:
    """
    Pretty similar to scotia but has different column names
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
            if df_cols[0].strip() == CIBC_COLUMNS[0] and df_cols[1] == CIBC_COLUMNS[1]:
                # Drop repeated header rows that appear at the top of each page
                df["DATE"] = df["DATE"].astype(str)
                df = df[df["DATE"].str.strip() != "DATE"].copy()
                df = df[df["DATE"].notna() & (df["DATE"].str.strip() != "")].copy()
                all_table_dfs.append(df)
            # other tables on the page (e.g. account info box) are skipped

    if not all_table_dfs:
        raise ValueError("No transaction table found in document — check that this is a CIBC statement")

    full_df = pd.concat(all_table_dfs, axis=0, ignore_index=True)

    # Rename to standard output column names
    if full_df.shape[1] == len(CIBC_COLUMNS):
        full_df.columns = CIBC_COLUMNS
    # else: column count mismatch — leave as-is and let caller handle it
    # TODO: see if column data requires normalization (dates → date type, amounts → float)
    #       or if the reconciliation process will handle that directly

    return full_df


def extract_for_ncb(doc) -> pd.DataFrame:
    """
    NCB statements have no column header row. There is an account description section
    on the first page(s), then a "Transactions List - ..." title, after which the
    transaction table starts immediately with data rows (no column labels).
    """
    _is_placeholder = lambda s: bool(re.match(r"^Col\d+$", str(s).strip()))
    _vlines = _NCB_VERTICAL_LINES if all(x is not None for x in _NCB_VERTICAL_LINES) else None
    # When DATE x-coord is known, use it as the clip left edge to exclude the index column.
    # When unknown, fall back to x=0 and drop the intruding index column manually.
    _clip_x = NCB_X_COL_DATE if NCB_X_COL_DATE is not None else 0
    _drop_index_col = NCB_X_COL_DATE is None

    def treat_table(table) -> pd.DataFrame:
        df = table.to_pandas()
        if _drop_index_col:
            # Fallback: clip starts at x=0, so the leading index column is included
            df.drop(df.columns[0], axis=1, inplace=True)

        # Skip fragment tables (headers/footers) that don't have enough columns
        if df.shape[1] < 6:
            return pd.DataFrame(columns=NCB_COLUMNS)

        # Recover first-row values from column names ("ColN" = empty cell)
        first_row = [None if _is_placeholder(c) else str(c).strip() for c in df.columns[:6]]

        # Merge description fragments (cols 6+) — first row from col names, data rows from cells
        first_row_desc = " ".join(
            str(c).strip() for c in df.columns[6:] if not _is_placeholder(c)
        ) or None
        first_row.append(first_row_desc)

        data_desc = df.iloc[:, 6:].apply(
            lambda row: " ".join(
                str(v).strip() for v in row if v is not None and str(v).strip()
            ) or None,
            axis=1,
        )

        df = df.iloc[:, :6].copy()
        df.insert(6, "DESCRIPTION", data_desc)

        return pd.DataFrame([first_row] + df.values.tolist(), columns=NCB_COLUMNS)

    def treat_first_page(page) -> list:
        page_width = page.rect.width
        page_height = page.rect.height

        words = page.get_text("words")  # (x0, y0, x1, y1, text, block, line, word_idx)

        # Find the "Transactions List" title line and clip to everything below it
        trigger_y = None
        for i, w in enumerate(words):
            if w[4] == "Transactions":
                # Check if "List" appears close by (same line = similar y0)
                for w2 in words[i:i + 5]:
                    if w2[4] == "List" and abs(w2[1] - w[1]) < 5:
                        trigger_y = w[3]  # y1 (bottom) of "Transactions"
                        break
            if trigger_y is not None:
                break

        if trigger_y is None:
            print("Header line not found on first transactions page")
            return []

        clip = pymupdf.Rect(_clip_x, trigger_y, page_width, page_height)
        tabs = page.find_tables(
            vertical_strategy="text",
            horizontal_strategy="text",
            clip=clip,
            min_words_vertical=2,
            min_words_horizontal=1,
            vertical_lines=_vlines,
        )
        return [treat_table(tab) for tab in tabs]

    first_page_index = 0
    for i, p in enumerate(doc):
        if "Transactions List -" in p.get_text():
            first_page_index = i
            break

    all_table_dfs = []

    all_table_dfs.extend(treat_first_page(doc[first_page_index]))
    for page in doc[(first_page_index + 1):]:
        page_clip = pymupdf.Rect(_clip_x, 0, page.rect.width, page.rect.height) if _clip_x else None
        all_table_dfs.extend([
            treat_table(tab)
            for tab in page.find_tables(
                vertical_strategy="text",
                horizontal_strategy="text",
                clip=page_clip,
                vertical_lines=_vlines,
            )
        ])

    if not all_table_dfs:
        return pd.DataFrame(columns=NCB_COLUMNS)

    full_df = pd.concat(all_table_dfs, axis=0, ignore_index=True)

    # Drop rows where every cell is an empty string (not caught by dropna)
    full_df = full_df[~full_df.apply(lambda r: r.astype(str).str.strip().eq("").all(), axis=1)]

    # Drop page header/timestamp rows (e.g. "e and Time: ... :Page")
    full_df = full_df[~full_df.apply(lambda r: r.astype(str).str.contains(":Page").any(), axis=1)]

    full_df = full_df.reset_index(drop=True)
    return full_df



def run_extraction(filepath, out_path):
    print(f"\nReading: {filepath}")
    doc = pymupdf.open(filepath)

    doc_type = get_doc_type(doc)
    print(f"Detected type: {doc_type}")

    if doc_type == "scotia":
        doc_df = extract_for_scotia(doc)
    elif doc_type == "sagicor":
        doc_df = extract_for_sagicor(doc)
    elif doc_type == "cibc":
        doc_df = extract_for_cibc(doc)
    elif doc_type == "ncb":
        doc_df = extract_for_ncb(doc)
    else:
        print(f"Invalid PDF or not accounted for: {filepath}")
        return

    doc.close()

    print(f"Shape: {doc_df.shape}")
    # print(doc_df.head(10).to_string())
    doc_df.to_excel(out_path, index=False, engine="openpyxl")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    filenames = [
    
                # {"input": "FEB 2026/Sagicor Statement 5500120338.pdf",
                #  "output": "FEB 2026/EB_11136480.xlsx"
                #  },
                # {"input": "JAN 2026/5500120338.pdf",
                #  "output": "JAN 2026/EB_11136480.xlsx"
                #  },
                # {"input": "DEC 2025/5500120338.pdf",
                #  "output": "DEC 2025/EB_11136480.xlsx"
                # },
                # {"input": "NOV 2025/5500120338 NOV 1-15,2025.pdf",
                #  "output": "NOV 2025/EB_11136480-1.xlsx"
                # },
                # {"input": "NOV 2025/5500120338 NOV 15-30,2025.pdf",
                #  "output": "NOV 2025/EB_11136480-2.xlsx"
                # },

        
        {"input": "FEB 2026/1855 f.pdf",
         "output": "FEB 2026/EB_11137380.xlsx"
        },
        {"input": "JAN 2026/1855 9.pdf",
         "output": "JAN 2026/EB_11137380.xlsx"
        },
        {"input": "APR 2026/1855 abr.pdf",
         "output": "APR 2026/EB_11137380.xlsx"
        },
        {"input": "NOV 2025/1855.pdf",
         "output": "NOV 2025/EB_11137380.xlsx"
        },
        {"input": "DEC 2025/1855 SC.pdf",
         "output": "DEC 2025/EB_11137380.xlsx"
        },
    ]

    for filename in filenames:
        filepath = os.path.join(script_dir, "Bank Statements", filename["input"])
        out_path = os.path.join(script_dir, "Bank Statements", filename["output"])

        run_extraction(filepath, out_path)
