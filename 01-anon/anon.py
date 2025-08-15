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
    """
    Generates pseudonym tokens for string values.

    This implementation attempts to produce type‑appropriate tokens.  For example,
    email addresses will be replaced by fake email addresses and phone numbers
    will be replaced by randomly generated phone numbers with the same pattern.
    If deterministic mode is enabled, tokens are derived from an HMAC of the
    original value so the same input always yields the same pseudonym.  The
    caller must still store a mapping to reverse tokens back to original values.
    """

    # Regular expressions for detecting data types
    _EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    _PHONE_PATTERN = re.compile(r"^(\+?\d{1,3}[\s\-]?)?(\(\d{1,4}\)[\s\-]?)?\d{3,4}[\s\-]?\d{3,4}$")
    _SSN_PATTERN = re.compile(r"^\d{3}-\d{2}-\d{4}$")
    # Name pattern: at least two words consisting of letters separated by whitespace
    _NAME_PATTERN = re.compile(r"^[A-Za-z]+(?:\s+[A-Za-z]+)+$")
    # Date patterns: match common date formats with digits and separators
    # Supported formats: YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, DD/MM/YYYY, MM/DD/YYYY
    _DATE_SEPARATOR_PATTERN = re.compile(r"[\-/.]")


    def __init__(self, deterministic: bool = False, secret_key: Optional[bytes] = None):
        self.deterministic = deterministic
        if deterministic:
            if not secret_key:
                raise ValueError("secret_key is required for deterministic token generation")
            self.secret_key = secret_key

    def _generate_random_digits(self, length: int) -> str:
        """Generate a string of random numeric digits of the given length."""
        # Use secrets if available for stronger randomness
        try:
            import secrets
            return ''.join(str(secrets.randbelow(10)) for _ in range(length))
        except ImportError:
            return ''.join(str(uuid.uuid4().int % 10) for _ in range(length))

    def _generate_deterministic_digits(self, value: str, length: int) -> str:
        """Generate a deterministic numeric string based on an HMAC digest of the value."""
        digest = hmac.new(self.secret_key, value.encode('utf-8'), hashlib.sha256).hexdigest()
        # Convert hex digest to a numeric string by mapping hex chars to digits
        digits = ''.join(str(int(ch, 16) % 10) for ch in digest)
        # Repeat the digits string if it's shorter than required
        while len(digits) < length:
            digest = hashlib.sha256(digest.encode('utf-8')).hexdigest()
            digits += ''.join(str(int(ch, 16) % 10) for ch in digest)
        return digits[:length]

    def _apply_numeric_pattern(self, pattern: str, digits: str) -> str:
        """Fill a numeric pattern (with non-digit separators) using the provided digits."""
        result_chars = []
        digit_index = 0
        for ch in pattern:
            if ch.isdigit():
                result_chars.append(digits[digit_index])
                digit_index += 1
            else:
                result_chars.append(ch)
        return ''.join(result_chars)

    def _generate_fake_email(self, value: str) -> str:
        """Generate a fake email address.  Keeps the domain generic to avoid revealing information."""
        # Determine local part length from original or default to 8
        local_length = 8
        if self.deterministic:
            digest = hmac.new(self.secret_key, value.encode('utf-8'), hashlib.sha256).hexdigest()
            local = digest[:local_length]
        else:
            local = uuid.uuid4().hex[:local_length]
        return f"{local}@anonymized.local"

    def _generate_fake_name(self, value: str) -> str:
        """
        Generate a fake full name.  The number of name parts (e.g. first and last
        names) will match the original.  Names are selected from fixed lists of
        common first and last names.  Deterministic mode uses the HMAC digest to
        choose names so the same original name yields the same pseudonym.
        """
        # Predefined lists of names.  In a real application these could be
        # extended or loaded from external resources.
        first_names = [
            'Alex', 'Jordan', 'Taylor', 'Casey', 'Morgan', 'Jamie', 'Cameron',
            'Riley', 'Sam', 'Charlie', 'Dakota', 'Reese', 'Robin', 'Avery',
            'Drew', 'Hayden'
        ]
        last_names = [
            'Smith', 'Johnson', 'Taylor', 'Brown', 'Anderson', 'Clark', 'Harris',
            'Lee', 'Wilson', 'Martin', 'Thompson', 'Lewis', 'Walker', 'Young',
            'Hall', 'Allen'
        ]
        parts = value.split()
        num_parts = len(parts)
        names: List[str] = []
        if self.deterministic:
            # Use HMAC digest to deterministically select names
            digest = hmac.new(self.secret_key, value.encode('utf-8'), hashlib.sha256).hexdigest()
            # Each part uses a 4‑hex‑digit chunk to compute an index
            for i in range(num_parts):
                chunk = digest[(i * 4):(i * 4) + 4]
                index = int(chunk, 16)
                if i == num_parts - 1:
                    # Last part uses last_names list
                    names.append(last_names[index % len(last_names)])
                else:
                    names.append(first_names[index % len(first_names)])
        else:
            try:
                import secrets
                rng_choice = secrets.choice
            except ImportError:
                import random
                rng_choice = random.choice
            for i in range(num_parts):
                if i == num_parts - 1:
                    names.append(rng_choice(last_names))
                else:
                    names.append(rng_choice(first_names))
        return ' '.join(names)

    def _is_date_pattern(self, val_str: str) -> bool:
        """
        Determine if the given string appears to be a date in a common numeric format.
        Returns True for patterns such as YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY, with
        consistent separators (-, / or .).
        """
        # Split on date separators
        sep_match = self._DATE_SEPARATOR_PATTERN.search(val_str)
        if not sep_match:
            return False
        sep = sep_match.group()
        parts = val_str.split(sep)
        if len(parts) != 3:
            return False
        # All parts must be digits
        if not all(part.isdigit() for part in parts):
            return False
        # Check for one part with four digits (year)
        length_counts = [len(part) for part in parts]
        if 4 not in length_counts:
            return False
        # At least one other part should be <= 2 digits
        # This simple check will treat many numeric codes as dates; for more
        # precise detection, additional context would be required.
        return True

    def _generate_fake_date(self, value: str) -> str:
        """
        Generate a fake date with the same separator and order as the original.
        Supported orders include YYYY-MM-DD, DD/MM/YYYY and MM/DD/YYYY.  Dates
        are generated within a 100‑year window starting from 1970.  Deterministic
        mode uses the HMAC digest to select a date.
        """
        import datetime
        sep_match = self._DATE_SEPARATOR_PATTERN.search(value)
        if not sep_match:
            # Fallback: return hashed value or random
            return self.generate(value)
        sep = sep_match.group()
        parts = value.split(sep)
        # Determine pattern: identify which part is year
        year_index = parts.index(next(p for p in parts if len(p) == 4))
        # Base date and range for generating random dates (100 years from 1970)
        base_date = datetime.date(1970, 1, 1)
        days_range = 365 * 100  # 100 years
        if self.deterministic:
            digest = hmac.new(self.secret_key, value.encode('utf-8'), hashlib.sha256).hexdigest()
            # Use first 12 hex digits to create an integer
            digest_int = int(digest[:12], 16)
            offset = digest_int % days_range
        else:
            try:
                import secrets
                offset = secrets.randbelow(days_range)
            except ImportError:
                import random
                offset = random.randrange(days_range)
        new_date = base_date + datetime.timedelta(days=offset)
        y = new_date.year
        m = new_date.month
        d = new_date.day
        # Reconstruct based on original order
        # If year is first (index 0)
        if year_index == 0:
            # Format year-month-day
            return f"{y:04d}{sep}{m:02d}{sep}{d:02d}"
        # If year is last (index 2)
        elif year_index == 2:
            # Determine if day or month comes first by checking original month/day values
            # If original first part > 12, treat as day-first; else month-first
            first_num = int(parts[0])
            if first_num > 12:
                # Day-month-year
                return f"{d:02d}{sep}{m:02d}{sep}{y:04d}"
            else:
                # Month-day-year
                return f"{m:02d}{sep}{d:02d}{sep}{y:04d}"
        else:
            # Unexpected pattern (year in middle); default to ISO
            return f"{y:04d}{sep}{m:02d}{sep}{d:02d}"

    def generate(self, value: str) -> str:
        """
        Return a pseudonym token for the given input value.  If the value matches
        known PII types (email, phone number, SSN), a type‑appropriate pseudonym
        will be generated.  Otherwise a hexadecimal token is returned.  In
        deterministic mode, tokens are reproducible for the same input.
        """
        if value is None:
            return value
        # Normalize to string for pattern matching
        val_str = str(value)
        # Email addresses
        if self._EMAIL_PATTERN.match(val_str):
            return self._generate_fake_email(val_str)
        # SSN pattern
        if self._SSN_PATTERN.match(val_str):
            num_digits = 9  # SSN has nine digits
            if self.deterministic:
                digits = self._generate_deterministic_digits(val_str, num_digits)
            else:
                digits = self._generate_random_digits(num_digits)
            # Format as ###-##-####
            return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"
        # Phone number pattern (may contain country code and separators)
        if self._PHONE_PATTERN.match(val_str):
            # Count number of digit positions in the phone number
            digit_count = sum(ch.isdigit() for ch in val_str)
            if self.deterministic:
                digits = self._generate_deterministic_digits(val_str, digit_count)
            else:
                digits = self._generate_random_digits(digit_count)
            return self._apply_numeric_pattern(val_str, digits)
        # Name pattern
        if self._NAME_PATTERN.match(val_str):
            return self._generate_fake_name(val_str)
        # Date pattern
        if self._is_date_pattern(val_str):
            return self._generate_fake_date(val_str)
        # Default: use deterministic digest or random UUID
        if self.deterministic:
            digest = hmac.new(self.secret_key, val_str.encode('utf-8'), hashlib.sha256).hexdigest()
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