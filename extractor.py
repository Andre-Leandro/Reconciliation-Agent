import pdfplumber
import pandas as pd

pdf_path = "Bank Statements/NOV 2025/720911.pdf"
excel_output = "EB_11136180.xlsx"

columnas = ["DATE", "TRANSACTION TYPE", "DESCRIPTION", "DEBITS", "CREDITS", "BALANCE"]

datos_totales = []

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        # Extraer la tabla de la página
        table = page.extract_table()
        
        if table:
            # Filtramos para quitar las líneas que son encabezados o están vacías
            for row in table:
                # REGLA DE LIMPIEZA: 
                # Si la primera celda no tiene una fecha o es igual a 'Date', la saltamos
                if not row or not row[0]:
                    continue
                fila_texto = " ".join(str(c or "") for c in row)
                fila_lower = [str(c or "").strip().lower() for c in row]
                fila_nonempty = [c for c in fila_lower if c]
                encabezados = {"date", "transactiontype", "description", "debits", "credits", "balance"}
                # Saltar encabezados repetidos por pagina
                if str(row[0]).strip().lower() == "date":
                    continue
                if set(fila_nonempty).issuperset(encabezados):
                    continue
                if fila_nonempty[:6] == ["date", "transactiontype", "description", "debits", "credits", "balance"]:
                    continue
                if len(row) > 1 and "Account" in str(row[1]):
                    continue
                # Saltar filas numeradas 1,2,3... que vienen como falso encabezado
                if all(str(c).strip().isdigit() for c in row[:3] if c is not None):
                    continue
                
                # Agregamos solo las líneas que parecen datos reales
                datos_totales.append(row)

# 3. Crear DataFrame y guardar a Excel
# Asegúrate de que el número de columnas coincida con lo extraído
# Normalizar cantidad de columnas para evitar desajustes con el header
if datos_totales:
    max_cols = max(len(r) for r in datos_totales)
    normalizados = [r + [None] * (max_cols - len(r)) for r in datos_totales]
    df = pd.DataFrame(normalizados)
else:
    df = pd.DataFrame()

# Limpieza extra: Eliminar filas que sean puramente vacías (si las hay)
df.dropna(how='all', inplace=True)

# Exportar
if df.empty:
    print("No se encontraron filas de datos en el PDF.")
else:
    # Eliminar la segunda columna si no se necesita (indice 1)
    if df.shape[1] >= 2:
        df.drop(columns=[1], inplace=True)
    # Ajustar encabezados al numero real de columnas
    if df.shape[1] != len(columnas):
        columnas = [f"Col_{i+1}" for i in range(df.shape[1])]
    df.to_excel(excel_output, index=False, header=columnas)

print(f"Proceso terminado. Archivo guardado como: {excel_output}")