# Data Cleaning Pipeline

An end-to-end data cleaning pipeline built with Python and pandas, applied to the WHO Life Expectancy dataset (193 countries, 2938 rows, 22 columns).

Built to demonstrate real data engineering skills: not just writing code, but making deliberate decisions about *why* each cleaning step is done.

---

## What it does

| Stage | Description |
|-------|-------------|
| Inspect | Profiles raw data — nulls, dtypes, duplicates, shape |
| Handle nulls | Median for numerics, mode/Unknown for categoricals |
| Remove duplicates | Drops exact duplicate rows, logs count removed |
| Fix types | Corrects wrong dtypes (floats→int, %strings→float) |
| Standardise | Strips whitespace, applies consistent casing |
| Outlier detection | IQR method, flags outliers as boolean columns |
| Validate | Asserts business rules hold after cleaning |
| Report | Outputs before/after metrics to a text file |

---

## Project structure
```
data-cleaning-pipeline/
├── data/                        # raw data (not tracked in git)
├── output/
│   ├── cleaned_data.csv         # final clean dataset
│   └── cleaning_report.txt      # automated quality report
├── cleaner.py                   # full pipeline
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Setup
```bash
# Clone the repo
git clone https://github.com/SunnyKsah/data-cleaning-pipeline.git
cd data-cleaning-pipeline

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

---

## Dataset

Download the WHO Life Expectancy dataset from Kaggle:
https://www.kaggle.com/datasets/kumarajarshi/life-expectancy-who

Save it as `data/who_life_expectancy.csv` before running the pipeline.

The `data/` folder is gitignored intentionally — datasets should not live in version control.

---

## Run
```bash
python cleaner.py
```

Outputs are saved to the `output/` folder automatically.

---

## Key concepts demonstrated

- **Median vs mean imputation** — median is used for numeric columns because it is resistant to outliers; mean is skewed by extreme values
- **`df.copy()` discipline** — every function operates on a copy to avoid mutating the original DataFrame across pipeline stages
- **`errors='coerce'`** — unsafe type conversions produce NaN instead of crashing, which is then handled by the null stage
- **IQR outlier detection** — non-parametric method that works on any distribution, unlike Z-score which assumes normality
- **MCAR / MAR / MNAR** — null handling strategy depends on the type of missingness, not just the count
- **Modular pipeline design** — each stage is an independent function, making the pipeline testable and easy to extend

---

## Sample output
```
=== RAW DATA INSPECTION ===
Shape: (2938, 22)
Duplicate rows: 0

Columns with nulls:
                                null_count  null_pct
Life expectancy                         10      0.34
Adult Mortality                         10      0.34
Alcohol                                194      6.61
Hepatitis B                            553     18.82
BMI                                     34      1.16
...

=== HANDLING NULLS ===
  'Life expectancy ': filled 10 nulls with median 72.10
  'Alcohol': filled 194 nulls with median 3.75
  'Hepatitis B': filled 553 nulls with median 92.00

All validation checks passed ✓
Clean CSV saved to: output/cleaned_data.csv
Report saved to: output/cleaning_report.txt
```

---

## Tech stack

- Python 3.x
- pandas
- numpy

---

