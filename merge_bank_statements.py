import os
import pandas as pd

def merge_bank_statements(directory, base_name, suffixes):
    inputs = []
    for suffix in suffixes:
        filename = f"{base_name}{suffix}.xlsx"
        path = os.path.join(directory, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file: {path}")
        inputs.append(path)

    frames = [pd.read_excel(path) for path in inputs]
    merged = pd.concat(frames, ignore_index=True)

    output_path = os.path.join(directory, f"{base_name}.xlsx")
    merged.to_excel(output_path, index=False)
    return output_path

if __name__ == "__main__":
    target_dir = os.path.join("Bank Statements", "NOV 2025")
    base = "EB_11136280"
    output = merge_bank_statements(target_dir, base, ["-1", "-2"])
    print(f"Saved: {output}")


