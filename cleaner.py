#setup and Ingestion

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

profile = {}

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")

def load_data(filename: str) -> pd.DataFrame:
    filepath = DATA_DIR/filename
    df = pd.read_csv(filepath)
    print(f"Loaded: {df.shape[0]} rows x {df.shape[1]} columns")
    return df

#Inspection
def inspect_data(df: pd.DataFrame) -> dict:
    print("\n=== DATA INSPECTION ===")
    print(df.head())
    print(f"\nShape: {df.shape}")

    print("\nColumn types:")
    print(df.dtypes)

    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df)) * 100

    null_df = pd.DataFrame({
        'null_count': null_counts,
        "null_pct": null_pct.round(2)
    })
    null_df = null_df[null_df['null_count'] > 0]

    print("\nNull values:")
    print(null_df)

    n_dupes = df.duplicated().sum()
    print(f"\nDuplicate rows: {n_dupes}")

    profile['shape'] = df.shape
    profile['null_counts'] = null_counts.to_dict()
    profile['n_duplicates'] = n_dupes
    profile['dtypes'] = df.dtypes.astype(str).to_dict()

    return profile

#Null handling
def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    numeric_cols = df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df[col].fillna(median_val, inplace=True)
            print(f"Filled '{col}' nulls with median: {median_val:.2f}")
    
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        if df[col].isnull().sum() > 0:
            null_pct = df[col].isnull().mean() * 100

            if null_pct > 40:
                df[col].fillna('Unknown', inplace=True)
                print(f" '{col}': {null_pct:.1f}% null -> filled with 'Unknown")
            else:
                mode_val = df[col].mode()[0]
                df[col].fillna(mode_val, inplace=True)
                print(f" '{col}': Filled with mode '{mode_val}'")
        
        return df
    
#duplicate removal

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    before = len(df)

    df.drop_duplicates(keep='first', inplace=True)

    after = len(df)
    print(f" Removed {before - after} duplicate rows ({before} -> {after})")

    return df

#Typecasting

def fix_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    int_candidates = ['Year']
    for col in int_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            print(f" Cast '{col}' to Int64")

    for col in df.select_dtypes(include='object').columns:
        sample = df[col].dropna().head(10)
        if sample.str.contains('%', na=False).any():
            df[col] = df[col].str.replace('%', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            print(f" Stripped % and cast '{col}' to float")

    return df


#standardisation

def standardise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.select_dtypes(include='object').columns:
        df[col] = (
            df[col].str.strip().str.title()
        )
    
    return df

#outlier detection

def handle_outlier(df: pd.DataFrame) -> pd.DataFrame:
    strategy='flag'   
    strategy='cap'   
    strategy='drop'
    
    df = df.copy()

    numeric_cols = df.select_dtypes(include=[np.number]).columns

    outlier_summary = {}

    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        lower_fence = Q1 - 1.5 * IQR
        upper_fence = Q3 + 1.5 * IQR

        outlier_mask = (df[col] < lower_fence) | (df[col] > upper_fence)
        n_outliers = outlier_mask.sum()

        if n_outliers > 0:
            outlier_summary[col] = n_outliers

            if strategy == 'flag':
                df[f' {col}_outlier'] = outlier_mask
            
            elif strategy == 'cap':
                df[col] = df[col].clip(lower_fence, upper_fence)
            
            elif strategy == 'drop':
                df = df[~outlier_mask]
        
    print(f" outliers found: {outlier_summary}")
    return df

#validation

def validate(df: pd.DataFrame) -> pd.DataFrame:
    
    violations = []

    key_cols = ['Country', 'Year']
    for col in key_cols:
        if col in df.columns and df[col].isnull().any():
            violations.append(f"Null found in key column '{col}'")
    
    #year should be in sensible range

    if 'Year' in df.columns:
        invalid_years = df[(df['Year'] < 1990) | (df['Year'] > 2030)]
        if len(invalid_years) > 0:
            violations.append(f"{len(invalid_years)} rows with invalid year values")
    
    #life expectancy should be between 1 and 100

    if 'Life expectancy' in df.columns:
        invalid_le = df[(df['Life expectancy'] < 1) | (df['Life expectancy'] > 100)]
        if len(invalid_le) > 0:
            violations.append(f"{len(invalid_le)} rows with impossible life expectancy")
    
    #Percentage columns should be 0-100
    pct_cols = [c for c in df.columns if 'percentage' in c.lower() or '%' in c.lower()]
    for col in pct_cols:
        invalid_pct = df[(df[col] > 0) | (df[col] > 100)]
        if len(invalid_pct) > 0:
            violations.append(f" '{col}' has {len(invalid_pct)} values outside 0-100")
    
    if violations:
        print(" Violation Failures:")
        for v in violations: print(f" x {v}")
    else:
        print("All validation checks passed")
    
    return violations

#Report generation

def generate_report(raw_profile: dict, clean_df: pd.DataFrame, violations: list, output_path: Path) -> None:
    lines = []
    lines.append("=" * 60)
    lines.append("DATA CLEANING REPORT")
    lines.append("=" * 60)
    lines.append(f"\nRaw data shape: {raw_profile['shape']}")
    lines.append(f"Clean data shape: {clean_df.shape}")

    removed_rows = raw_profile['shape'][0] - clean_df.shape[0]
    lines.append(f"Rows removed: {removed_rows}")

    lines.append("\n--- Rules in Raw Data ---")
    for col, count in raw_profile['null_counts'].items():
        if count > 0:
            pct = count / raw_profile['shape'][0] * 100
            lines.append(f" {col}: {count} ({pct:.1f}%)")
    
    lines.append(f"\n--- Duplicates ---")
    lines.append(f" Duplicate rows removed: {raw_profile['n_duplicates']}")

    lines.append(f"\n--- POST-CLEAN NULLS ---")
    remaining_nulls = clean_df.isnull().sum()
    remaining_nulls = remaining_nulls[remaining_nulls > 0]

    if len(remaining_nulls) == 0:
        lines.append(" None remaining")
    else:
        for col, count in remaining_nulls.items():
            lines.append(f" {col}: {count}")
    
    lines.append(f"\n--- VALIDATION ---")
    if not violations:
        lines.append("All checks passed")
    else:
        for v in violations:
            lines.append(f" x {v}")
    
    report_text = "\n".join(lines)
    print(report_text)

    with open(output_path, 'w') as f:
        f.write(report_text)
    print(f"\nReport saved: {output_path}")


#main pipeline

def run_pipeline(filename: str) -> pd.DataFrame:
    print("Starting data cleaning pipeline...")

    df_raw = load_data(filename)

    raw_profile = inspect_data(df_raw)

    df = handle_nulls(df_raw)
    df = remove_duplicates(df)
    df = fix_types(df)
    df = standardise(df)
    df = handle_outlier(df, strategy='flag')

    violations = validate(df)

    clean_path = OUTPUT_DIR / "cleaned_data.csv"
    df.to_csv(clean_path, index=False)
    print(f"\nClean CSV saved: {clean_path}")

    generate_report(raw_profile, df, violations, OUTPUT_DIR / "cleaning_report.txt")

    return df

if __name__ == "__main__":
    cleaned = run_pipeline("who_life_expectancy.csv")
