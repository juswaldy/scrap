# Comprehensive Plan for a Data‑Anonymization Application

## 1. Introduction and Context

Modern organisations routinely collect personally identifiable information (PII) such as names, email addresses, phone numbers and government identifiers.  Unprotected PII increases the risk of privacy breaches, identity theft and non‑compliance with data‑protection legislation.  **Data anonymisation** is the process of removing or encrypting sensitive data so that records can be used for analytics or sharing without revealing individuals’ identities.  For example, Immuta notes that anonymisation involves removing or encrypting sensitive information (PII, PHI or other commercial secrets) to protect confidentiality while still retaining the data’s value for analysis【741706874534867†L183-L187】.  Common techniques include data masking, pseudonymisation, generalisation, perturbation and the creation of synthetic data【684068076377924†L470-L528】.

This plan outlines a Python application that can ingest CSV, JSON or XML data, identify PII columns, and **pseudonymise** those columns.  Pseudonymisation is a reversible form of anonymisation; the app will store a mapping between original values and pseudonyms so that authorised users can later reverse the process.  The design is intentionally modular so it can be integrated into existing data pipelines.

## 2. Regulatory background and pseudonymisation

The General Data Protection Regulation (GDPR) defines pseudonymisation as:

> The processing of personal data in such a manner that the data can no longer be attributed to a specific data subject without the use of additional information, provided that such additional information is kept separately and is subject to technical and organisational measures to ensure that the personal data are not attributed to an identified or identifiable natural person【321944407493628†L122-L131】.  

In practice, pseudonymisation replaces sensitive identifiers with pseudonyms or tokens while storing the original identifiers elsewhere.  Systems must maintain strict access controls to the mapping because re‑identification is possible【321944407493628†L146-L151】.  Although pseudonymised data cannot on its own identify individuals, it is still considered personal data under GDPR because it can be re‑linked.  Consequently, robust governance around the mapping (separation of duties, encryption, audit logging) is essential.

## 3. Overview of data‑anonymisation techniques

The Satori Cyber guide summarises several anonymisation techniques【684068076377924†L470-L528】:

| Technique | Description | When to use |
|---|---|---|
| **Data masking** | Creates a modified version of sensitive data so that original values cannot be reconstructed.  Masking can be static (in a copy of the data) or dynamic (on‑the‑fly during queries)【684068076377924†L470-L480】. | When irreversible anonymisation is acceptable and analytics only need obfuscated data. |
| **Pseudonymisation** | Replaces private identifiers with pseudonyms or tokens, e.g. swapping “David Bloomberg” with “John Smith”.  This preserves statistical accuracy and allows re‑identification using a separate mapping【684068076377924†L487-L490】. | When identifiers need to be hidden but still linked later.  Our app uses this technique. |
| **Generalisation** | Broadens data values (e.g. replacing exact ages with age ranges or omitting house numbers) to reduce identifiability while retaining useful patterns【684068076377924†L496-L500】. | When aggregated analysis suffices and detailed values aren’t needed. |
| **Data swapping** | Shuffles attribute values between records so that each row no longer corresponds to the original row【684068076377924†L504-L509】. | Useful for machine‑learning datasets where relative distributions matter more than individual records. |
| **Data perturbation** | Introduces random noise or rounding to numerical values; careful tuning balances privacy and utility【684068076377924†L511-L517】. | Appropriate for statistical aggregates (e.g. adding noise to salary figures). |
| **Synthetic data** | Generates artificial datasets with similar statistical properties to the original; no direct link to real individuals【684068076377924†L519-L523】. | Ideal for sharing data externally when real data cannot be exposed. |

This application focuses on **pseudonymisation** because it allows reversible anonymisation through a token–value mapping.  Other techniques can be added later.

## 4. High‑level architecture

### 4.1 Supported input formats

1. **CSV:** Tabular files with headers.  Pandas can parse these files directly.
2. **JSON:** Either a list of records or nested structures.  The app will normalise nested JSON into flat rows using pandas’ `json_normalize` or custom functions.
3. **XML:** Parsed with `xml.etree.ElementTree` into a list of dictionaries; elements will be flattened into key–value pairs.

### 4.2 PII detection and column selection

Users can explicitly specify which columns contain PII.  To aid usability, the app can also detect candidate PII columns using regular expressions (e.g. email addresses, phone numbers, credit‑card patterns).  Because automatic detection can produce false positives/negatives, it will operate in **preview** mode only: users confirm which columns to anonymise.

### 4.3 Pseudonymisation mechanism

The pseudonymisation strategy will use **tokenisation**, a form of pseudonymisation.  Each distinct sensitive value is replaced by a unique pseudonym.  Key design decisions include:

1. **Token generation:**  
   - *Random tokens:* Generate a UUID4 or random hexadecimal string per unique value.  This yields non‑guessable tokens.  The mapping must be stored to enable reversal.  
   - *Deterministic tokens:* Compute a hash (e.g. HMAC‑SHA256) of the value using a secret key.  This ensures the same input always produces the same token without storing each mapping.  However, given the key, one can still brute‑force values with small domains.  
  
   The app defaults to random tokens because the mapping is stored anyway; deterministic tokenisation can be activated via a flag.

2. **Mapping storage:**  The mapping between original values and tokens is critical.  The GDPR definition stresses that additional information enabling re‑identification must be kept separately under technical and organisational measures【321944407493628†L122-L131】.  Therefore, the mapping file will be saved separately from the anonymised data.  Options include:
   - JSON file: simple to use; appropriate for small‑to‑medium datasets.  
   - SQLite or another database: beneficial for larger datasets or multi‑user environments.  
   - Encrypted storage: for production, encrypt the mapping using a key management system (not implemented here, but recommended).  
  
   The app will write mapping files as JSON by default but exposes a pluggable interface to allow other back‑ends.

3. **Consistency:**  When anonymising multiple files or repeated runs, the same original value should map to the same token if desired.  The app will load existing mapping files and reuse tokens.  If the user opts for deterministic tokens, the secret key must remain constant.

4. **Referential integrity:**  When pseudonymising multiple columns or multiple files, preserving relationships is important.  The mapping is maintained per column; identical values in different columns will not necessarily map to the same token unless specifically configured.  For example, an email appearing in two tables will map to the same pseudonym if both tables share the same mapping file.

### 4.4 Workflow

1. **Read input file:**  Identify file type by extension and parse into a `pandas.DataFrame`.  Flatten nested structures.
2. **Identify PII columns:**  
   - If `pii_columns` parameter is supplied, use it directly.  
   - Otherwise, scan columns with regex patterns for emails, phone numbers, Social Insurance Numbers (SIN/SSN), credit cards, etc., and present candidates to the user for confirmation.
3. **Load existing mapping (optional):**  If a mapping file exists, load it to reuse existing pseudonyms.
4. **Generate tokens:**  For each PII column:
   - For each unique value not present in the mapping, generate a token (random or HMAC).  
   - Add mapping entries.  
   - Replace occurrences in the DataFrame with tokens.
5. **Write anonymised file:**  Output the anonymised DataFrame to the same file type (CSV, JSON or XML).  The structure and column order will be preserved.
6. **Write mapping file:**  Save the mapping to a separate JSON file (or another supported storage).  The mapping file will structure as `{column_name: {original_value: token}}`.
7. **Restore function:**  Provide a function to reverse the anonymisation.  This reads the mapping and replaces tokens with original values.  This process should only be used by authorised personnel.

### 4.5 Security and privacy considerations

1. **Access controls:**  Only authorised users should have access to mapping files.  Implement proper file permissions and, for production, consider storing mappings in encrypted databases with audit logging.
2. **Separation of duties:**  Developers using anonymised data should not have access to the mapping.  The GDPR definition of pseudonymisation emphasises storing the additional information separately【321944407493628†L122-L131】.
3. **Token collision:**  While UUIDs are extremely unlikely to collide, deterministic hashes might collide if truncated.  The app will use full 128‑bit UUIDs or full digest outputs to minimise risk.
4. **Metadata leakage:**  Even with pseudonyms, certain metadata (e.g. distributions, rare values) can enable re‑identification.  Consider combining pseudonymisation with generalisation or perturbation for high‑risk datasets.
5. **Log handling:**  Avoid logging original values or tokens in plain text.  Logging should be configurable and minimal.

## 5. Python implementation

The following section outlines the Python modules and classes that implement the anonymisation workflow.  The code is provided in a separate script file (`data_anonymizer.py`) and can be integrated into existing pipelines or called from the command line.

### 5.1 Dependencies

* `pandas` for data manipulation and CSV/JSON IO.  
* `xml.etree.ElementTree` for parsing XML.  
* `uuid` and `hashlib/hmac` for token generation.  
* `json` for storing mapping files.  
* (Optional) `argparse` for a command‑line interface.

### 5.2 Key classes and functions

```python
import json
import uuid
import hashlib
import hmac
import pandas as pd
from typing import Dict, List, Optional, Callable


class TokenGenerator:
    """
    Generates pseudonym tokens.  Two strategies are supported:
    1. random tokens (UUID4) – unpredictable and require mapping storage.
    2. deterministic tokens using HMAC-SHA256 – same input yields the same token.
    """

    def __init__(self, deterministic: bool = False, secret_key: Optional[bytes] = None):
        self.deterministic = deterministic
        if deterministic:
            if not secret_key:
                raise ValueError("secret_key is required for deterministic token generation")
            self.secret_key = secret_key

    def generate(self, value: str) -> str:
        if self.deterministic:
            # Use HMAC-SHA256 and return hexadecimal digest
            digest = hmac.new(self.secret_key, value.encode('utf-8'), hashlib.sha256).hexdigest()
            return digest
        else:
            # Generate a random UUID
            return uuid.uuid4().hex


class Pseudonymizer:
    """
    Handles pseudonymisation of PII columns within a DataFrame.
    Maintains a mapping and can reuse existing mappings.
    """

    def __init__(self, token_generator: TokenGenerator, mapping: Optional[Dict[str, Dict[str, str]]] = None):
        self.token_generator = token_generator
        self.mapping = mapping or {}

    def _ensure_column_mapping(self, column: str):
        if column not in self.mapping:
            self.mapping[column] = {}

    def pseudonymize_dataframe(self, df: pd.DataFrame, pii_columns: List[str]) -> pd.DataFrame:
        result = df.copy()
        for column in pii_columns:
            self._ensure_column_mapping(column)
            col_mapping = self.mapping[column]
            # Map each value to a token
            for original_value in df[column].dropna().unique():
                # Convert to string for consistent hashing
                original_str = str(original_value)
                if original_str not in col_mapping:
                    token = self.token_generator.generate(original_str)
                    col_mapping[original_str] = token
            # Apply mapping to the column
            result[column] = df[column].astype(str).map(col_mapping)
        return result

    def restore_dataframe(self, df: pd.DataFrame, pii_columns: List[str]) -> pd.DataFrame:
        result = df.copy()
        for column in pii_columns:
            if column not in self.mapping:
                raise ValueError(f"No mapping available for column '{column}'")
            col_mapping = self.mapping[column]
            reverse_mapping = {v: k for k, v in col_mapping.items()}
            result[column] = df[column].map(reverse_mapping)
        return result

    def save_mapping(self, path: str):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.mapping, f, ensure_ascii=False, indent=2)

    @classmethod
    def load_mapping(cls, path: str, token_generator: TokenGenerator):
        with open(path, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
        return cls(token_generator, mapping)


def detect_pii_columns(df: pd.DataFrame) -> List[str]:
    """
    Heuristic PII detection using regex patterns.  Returns a list of candidate PII columns.
    Users should verify these suggestions.
    """
    import re

    email_pattern = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    phone_pattern = re.compile(r"^(\+?\d{1,3}[\s\-]?)?(\(\d{1,4}\)[\s\-]?)?\d{3,4}[\s\-]?\d{3,4}$")
    ssn_pattern = re.compile(r"^\d{3}-\d{2}-\d{4}$")

    pii_candidates: List[str] = []
    for column in df.columns:
        sample = df[column].dropna().astype(str).head(20)
        # Check if most values match a PII pattern
        email_matches = sum(bool(email_pattern.match(x)) for x in sample)
        phone_matches = sum(bool(phone_pattern.match(x)) for x in sample)
        ssn_matches = sum(bool(ssn_pattern.match(x)) for x in sample)
        if email_matches > len(sample) * 0.5 or phone_matches > len(sample) * 0.5 or ssn_matches > len(sample) * 0.5:
            pii_candidates.append(column)
    return pii_candidates


def read_file(path: str) -> pd.DataFrame:
    """
    Reads CSV, JSON or XML into a DataFrame.  For JSON, supports list of records or nested objects.  For XML, flattens elements.
    """
    import os
    import xml.etree.ElementTree as ET
    import json as json_lib

    ext = os.path.splitext(path)[1].lower()
    if ext == '.csv':
        return pd.read_csv(path)
    elif ext == '.json':
        with open(path, 'r', encoding='utf-8') as f:
            data = json_lib.load(f)
        # If data is a list of dicts, directly convert
        if isinstance(data, list):
            return pd.json_normalize(data)
        elif isinstance(data, dict):
            # Flatten top-level keys into columns
            return pd.json_normalize(data)
        else:
            raise ValueError('Unsupported JSON structure')
    elif ext == '.xml':
        tree = ET.parse(path)
        root = tree.getroot()

        def parse_element(element) -> Dict[str, str]:
            record = {}
            for child in element:
                # If child has its own children, flatten them with prefix
                if list(child):
                    for sub in child:
                        record[f"{child.tag}_{sub.tag}"] = sub.text
                else:
                    record[child.tag] = child.text
            return record

        records = [parse_element(elem) for elem in root]
        return pd.DataFrame(records)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def write_file(df: pd.DataFrame, path: str):
    """
    Writes a DataFrame to CSV, JSON or XML based on file extension.
    """
    import os
    import xml.etree.ElementTree as ET

    ext = os.path.splitext(path)[1].lower()
    if ext == '.csv':
        df.to_csv(path, index=False)
    elif ext == '.json':
        # Convert DataFrame to a list of dicts for JSON
        data = df.to_dict(orient='records')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    elif ext == '.xml':
        root = ET.Element('root')
        for _, row in df.iterrows():
            elem = ET.SubElement(root, 'record')
            for col, value in row.items():
                child = ET.SubElement(elem, col)
                child.text = str(value)
        tree = ET.ElementTree(root)
        tree.write(path, encoding='utf-8', xml_declaration=True)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def anonymize_file(
    input_path: str,
    output_path: str,
    mapping_path: str,
    pii_columns: Optional[List[str]] = None,
    deterministic: bool = False,
    secret_key: Optional[bytes] = None,
    append_mapping: bool = True
) -> None:
    """
    High‑level function that reads a file, pseudonymises the selected PII columns and writes the anonymised file
    along with the mapping.  If append_mapping is True and a mapping file already exists, it is loaded and
    updated; otherwise a new mapping is created.
    """
    df = read_file(input_path)
    # Detect PII if not provided
    if pii_columns is None:
        pii_columns = detect_pii_columns(df)
        print(f"Detected PII columns: {pii_columns}")

    # Load or initialise mapping
    if append_mapping:
        try:
            pseudonymizer = Pseudonymizer.load_mapping(mapping_path, TokenGenerator(deterministic, secret_key))
        except FileNotFoundError:
            pseudonymizer = Pseudonymizer(TokenGenerator(deterministic, secret_key))
    else:
        pseudonymizer = Pseudonymizer(TokenGenerator(deterministic, secret_key))

    anonymised_df = pseudonymizer.pseudonymize_dataframe(df, pii_columns)
    write_file(anonymised_df, output_path)
    pseudonymizer.save_mapping(mapping_path)
    print(f"Anonymisation complete.  Output written to {output_path} and mapping saved to {mapping_path}")


def deanonymize_file(input_path: str, output_path: str, mapping_path: str, pii_columns: Optional[List[str]] = None, deterministic: bool = False, secret_key: Optional[bytes] = None) -> None:
    """
    Reads an anonymised file and restores the original PII values using a mapping file.  Only works if the mapping
    file contains corresponding token mappings.  The restored file is written to output_path.
    """
    df = read_file(input_path)
    pseudonymizer = Pseudonymizer.load_mapping(mapping_path, TokenGenerator(deterministic, secret_key))
    if pii_columns is None:
        # Use mapping keys as default PII columns
        pii_columns = list(pseudonymizer.mapping.keys())
    restored_df = pseudonymizer.restore_dataframe(df, pii_columns)
    write_file(restored_df, output_path)
    print(f"De‑anonymisation complete.  Restored file written to {output_path}")

```

### 5.3 Command‑line usage (optional)

The library can be wrapped with `argparse` to provide a simple CLI.  Users specify the input file, output file, mapping file and list of PII columns.  They can also choose deterministic tokens by providing a secret key.  Example:

```bash
python data_anonymizer.py --input users.csv --output users_anonymised.csv --mapping mapping.json \
    --pii-columns email phone --deterministic --secret-key "mysecretkey"

# To reverse:
python data_anonymizer.py --deanonymize --input users_anonymised.csv --output users_restored.csv --mapping mapping.json
```

The CLI implementation is omitted for brevity but can be built on top of the functions above.

## 6. Conclusion and recommendations

This plan details a Python application for pseudonymising PII in CSV, JSON and XML files.  It leverages tokenisation to replace sensitive values with tokens while storing a separate mapping for re‑identification.  The design follows best‑practice guidance that pseudonymised data should be reversible only with additional information stored separately and securely【321944407493628†L122-L131】.  The app can detect PII heuristically, supports deterministic or random tokens, and preserves referential integrity across runs.  For production deployments, the following enhancements are recommended:

1. **Robust PII detection:** integrate machine‑learning‑based classifiers for better accuracy and support user‑specified pattern definitions.
2. **Secure key and mapping storage:** use a hardware security module (HSM) or secrets manager for keys and store mapping tables in an encrypted database with access control and audit logging.
3. **Scalability:** adapt the mapping storage to handle millions of records (e.g. via SQLite or PostgreSQL) and stream large files to avoid memory issues.
4. **Additional anonymisation techniques:** add options for generalisation, noise addition or synthetic data generation when tokens alone do not provide sufficient protection.

By following these guidelines, organisations can responsibly share and analyse data while protecting individual privacy and complying with privacy regulations.
