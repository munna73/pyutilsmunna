import pandas as pd
import os

class FileReader:
    @staticmethod
    def read_text_file(filepath):
        """Read a plain text file line-by-line into a list of strings."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        with open(filepath, 'r') as file:
            return [line.strip() for line in file if line.strip()]

    @staticmethod
    def read_csv(filepath):
        """Read a CSV file into a DataFrame."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        return pd.read_csv(filepath)

    @staticmethod
    def read_excel(filepath, sheet_name=None):
        """
        Read an Excel file into a DataFrame or a dict of DataFrames.
        If sheet_name is None, all sheets are returned as a dictionary.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        return pd.read_excel(filepath, sheet_name=sheet_name)
