"""
data_anonymizer.py
===================

This module implements a simple data anonymisation application that supports
CSV, JSON and XML files.  It pseudonymises user‑specified PII columns by
replacing original values with tokens and stores a mapping so that the process
can be reversed.  It can be used programmatically or via the command line.

Usage (command line):
    # Anonymise a CSV file, automatically detect PII columns, save mapping
    python data_anonymizer.py --input users.csv --output users_anonymised.csv --mapping mapping.json

    # Anonymise and specify which columns to pseudonymise
    python data_anonymizer.py --input users.csv --output users_anonymised.csv \
        --mapping mapping.json --pii-columns email,name,phone

    # Restore data from an anonymised file
    python data_anonymizer.py --deanonymize --input users_anonymised.csv \
        --output users_restored.csv --mapping mapping.json

The mapping file is a JSON document storing per‑column dictionaries mapping
original values to tokens.

See the accompanying documentation for design rationale and security
considerations.
"""

import argparse
import json
import uuid
import hashlib
import hmac
import os
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Callable

import pandas as pd


class TokenGenerator:
    """Generates pseudonym tokens for string values."""

    def __init__(self, deterministic: bool = False, secret_key: Optional[bytes] = None):
        self.deterministic = deterministic
        if deterministic:
            if not secret_key:
                raise ValueError("secret_key is required for deterministic token generation")
            self.secret_key = secret_key

    def generate(self, value: str) -> str:
        """Return a pseudonym token for the given input value."""
        if self.deterministic:
            digest = hmac.new(self.secret_key, value.encode('utf-8'), hashlib.sha256).hexdigest()
            return digest
        else:
            return uuid.uuid4().hex


class Pseudonymizer:
    """Handles pseudonymisation and restoration of DataFrame columns."""

    def __init__(self, token_generator: TokenGenerator, mapping: Optional[Dict[str, Dict[str, str]]] = None):
        self.token_generator = token_generator
        self.mapping: Dict[str, Dict[str, str]] = mapping or {}

    def _ensure_column_mapping(self, column: str) -> None:
        if column not in self.mapping:
            self.mapping[column] = {}

    def pseudonymize_dataframe(self, df: pd.DataFrame, pii_columns: List[str]) -> pd.DataFrame:
        result = df.copy()
        for column in pii_columns:
            self._ensure_column_mapping(column)
            col_mapping = self.mapping[column]
            # Generate tokens for new values
            for original_value in df[column].dropna().unique():
                original_str = str(original_value)
                if original_str not in col_mapping:
                    token = self.token_generator.generate(original_str)
                    col_mapping[original_str] = token
            # Replace column values with tokens
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

    def save_mapping(self, path: str) -> None:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.mapping, f, ensure_ascii=False, indent=2)

    @classmethod
    def load_mapping(cls, path: str, token_generator: TokenGenerator) -> 'Pseudonymizer':
        with open(path, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
        return cls(token_generator, mapping)


def detect_pii_columns(df: pd.DataFrame) -> List[str]:
    """
    Heuristically detect PII columns by matching patterns for emails, phone numbers and SSNs.
    Returns a list of candidate column names.  Users should verify these suggestions.
    """
    email_pattern = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    phone_pattern = re.compile(r"^(\+?\d{1,3}[\s\-]?)?(\(\d{1,4}\)[\s\-]?)?\d{3,4}[\s\-]?\d{3,4}$")
    ssn_pattern = re.compile(r"^\d{3}-\d{2}-\d{4}$")

    pii_candidates = []
    for column in df.columns:
        # Sample up to first 20 non-null values
        sample = df[column].dropna().astype(str).head(20)
        if sample.empty:
            continue
        email_matches = sum(bool(email_pattern.match(x)) for x in sample)
        phone_matches = sum(bool(phone_pattern.match(x)) for x in sample)
        ssn_matches = sum(bool(ssn_pattern.match(x)) for x in sample)
        if email_matches > len(sample) * 0.5 or phone_matches > len(sample) * 0.5 or ssn_matches > len(sample) * 0.5:
            pii_candidates.append(column)
    return pii_candidates


def read_file(path: str) -> pd.DataFrame:
    """
    Read a CSV, JSON or XML file into a pandas DataFrame.  For JSON, supports
    list/dict structures.  For XML, flattens nested elements.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == '.csv':
        return pd.read_csv(path)
    elif ext == '.json':
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.json_normalize(data)
        elif isinstance(data, dict):
            return pd.json_normalize(data)
        else:
            raise ValueError('Unsupported JSON structure')
    elif ext == '.xml':
        tree = ET.parse(path)
        root = tree.getroot()

        def parse_element(element) -> Dict[str, str]:
            record: Dict[str, str] = {}
            for child in element:
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


def write_file(df: pd.DataFrame, path: str) -> None:
    """Write a DataFrame back to CSV, JSON or XML based on the file extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext == '.csv':
        df.to_csv(path, index=False)
    elif ext == '.json':
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
    secret_key: Optional[str] = None,
    append_mapping: bool = True,
    verbose: bool = True,
) -> None:
    """
    Read a file, pseudonymise PII columns, write the anonymised file and save the mapping.
    If append_mapping is True and the mapping file exists, it loads and updates the mapping;
    otherwise it creates a new mapping.  secret_key is used only for deterministic token
    generation.
    """
    df = read_file(input_path)

    # Detect PII columns if none provided
    if pii_columns is None:
        pii_columns = detect_pii_columns(df)
        if verbose:
            print(f"Automatically detected PII columns: {pii_columns}")

    # Prepare token generator
    key_bytes: Optional[bytes] = None
    if deterministic:
        if not secret_key:
            raise ValueError("secret_key must be provided for deterministic token generation")
        key_bytes = secret_key.encode('utf-8')
    token_gen = TokenGenerator(deterministic=deterministic, secret_key=key_bytes)

    # Load existing mapping if append_mapping
    if append_mapping and os.path.exists(mapping_path):
        pseudonymizer = Pseudonymizer.load_mapping(mapping_path, token_gen)
    else:
        pseudonymizer = Pseudonymizer(token_gen)

    anonymised_df = pseudonymizer.pseudonymize_dataframe(df, pii_columns)
    write_file(anonymised_df, output_path)
    pseudonymizer.save_mapping(mapping_path)
    if verbose:
        print(f"Anonymisation complete.  Output written to {output_path}")
        print(f"Mapping saved to {mapping_path}")


def deanonymize_file(
    input_path: str,
    output_path: str,
    mapping_path: str,
    pii_columns: Optional[List[str]] = None,
    deterministic: bool = False,
    secret_key: Optional[str] = None,
    verbose: bool = True,
) -> None:
    """
    Restore an anonymised file using the provided mapping.  If pii_columns is None,
    all columns present in the mapping will be restored.  secret_key is needed
    only if deterministic token generation was used.
    """
    df = read_file(input_path)

    key_bytes: Optional[bytes] = None
    if deterministic:
        if not secret_key:
            raise ValueError("secret_key must be provided for deterministic token generation")
        key_bytes = secret_key.encode('utf-8')
    token_gen = TokenGenerator(deterministic=deterministic, secret_key=key_bytes)

    pseudonymizer = Pseudonymizer.load_mapping(mapping_path, token_gen)
    if pii_columns is None:
        pii_columns = list(pseudonymizer.mapping.keys())
    restored_df = pseudonymizer.restore_dataframe(df, pii_columns)
    write_file(restored_df, output_path)
    if verbose:
        print(f"De‑anonymisation complete.  Output written to {output_path}")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Anonymise or de‑anonymise CSV/JSON/XML files containing PII.")
    parser.add_argument('--input', '-i', required=True, help="Path to input file (CSV/JSON/XML)")
    parser.add_argument('--output', '-o', required=True, help="Path to output file")
    parser.add_argument('--mapping', '-m', required=True, help="Path to mapping JSON file")
    parser.add_argument('--pii-columns', help="Comma‑separated list of column names to pseudonymise")
    parser.add_argument('--deterministic', action='store_true', help="Use deterministic token generation (requires --secret-key)")
    parser.add_argument('--secret-key', help="Secret key for deterministic token generation")
    parser.add_argument('--append-mapping', action='store_true', help="Append to existing mapping if it exists")
    parser.add_argument('--deanonymize', action='store_true', help="Perform de‑anonymisation instead of anonymisation")
    parser.add_argument('--quiet', action='store_true', help="Suppress verbose output")
    args = parser.parse_args(argv)

    pii_columns = args.pii_columns.split(',') if args.pii_columns else None
    verbose = not args.quiet

    if args.deanonymize:
        deanonymize_file(
            input_path=args.input,
            output_path=args.output,
            mapping_path=args.mapping,
            pii_columns=pii_columns,
            deterministic=args.deterministic,
            secret_key=args.secret_key,
            verbose=verbose,
        )
    else:
        anonymize_file(
            input_path=args.input,
            output_path=args.output,
            mapping_path=args.mapping,
            pii_columns=pii_columns,
            deterministic=args.deterministic,
            secret_key=args.secret_key,
            append_mapping=args.append_mapping,
            verbose=verbose,
        )


if __name__ == '__main__':
    main()