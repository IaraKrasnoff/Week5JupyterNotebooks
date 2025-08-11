#!/usr/bin/env python3
"""
json_helper.py - Exercise 3
A reusable module for working with JSON files and converting them to pandas DataFrames
"""

import pandas as pd
import json
import os
from glob import glob

def load_json_file(filepath):
    """
    Load a single JSON file and return the data
    
    Args:
        filepath (str): Path to the JSON file
    
    Returns:
        dict: JSON data or None if error
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def json_to_dataframe(json_data, results_key='results'):
    """
    Convert JSON data to a pandas DataFrame
    
    Args:
        json_data (dict): JSON data containing results
        results_key (str): Key in JSON data that contains the array of records
    
    Returns:
        pandas.DataFrame: DataFrame created from JSON data
    """
    if not json_data or results_key not in json_data:
        print(f"No '{results_key}' found in JSON data")
        return pd.DataFrame()
    
    try:
        df = pd.DataFrame(json_data[results_key])
        return df
    except Exception as e:
        print(f"Error converting JSON to DataFrame: {e}")
        return pd.DataFrame()

def load_multiple_json_files(file_pattern):
    """
    Load multiple JSON files matching a pattern and combine into one DataFrame
    
    Args:
        file_pattern (str): Glob pattern to match files (e.g., 'data/*.json')
    
    Returns:
        pandas.DataFrame: Combined DataFrame from all matching files
    """
    files = glob(file_pattern)
    if not files:
        print(f"No files found matching pattern: {file_pattern}")
        return pd.DataFrame()
    
    print(f"Found {len(files)} files matching pattern: {file_pattern}")
    
    all_dataframes = []
    
    for filepath in sorted(files):
        print(f"Loading: {filepath}")
        json_data = load_json_file(filepath)
        
        if json_data:
            df = json_to_dataframe(json_data)
            if not df.empty:
                all_dataframes.append(df)
                print(f"  Loaded {len(df)} records")
            else:
                print(f"  No data in {filepath}")
        else:
            print(f"  Failed to load {filepath}")
    
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"\nCombined {len(combined_df)} total records from {len(all_dataframes)} files")
        return combined_df
    else:
        print("No data loaded from any files")
        return pd.DataFrame()

def save_dataframe_to_pickle(dataframe, filepath):
    """
    Save DataFrame to a pickle file for fast loading later
    
    Args:
        dataframe (pandas.DataFrame): DataFrame to save
        filepath (str): Path where to save the pickle file
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        dataframe.to_pickle(filepath)
        print(f"Saved DataFrame to {filepath}")
        return True
    except Exception as e:
        print(f"Error saving to {filepath}: {e}")
        return False

def load_dataframe_from_pickle(filepath):
    """
    Load DataFrame from a pickle file
    
    Args:
        filepath (str): Path to the pickle file
    
    Returns:
        pandas.DataFrame: Loaded DataFrame or empty DataFrame if error
    """
    try:
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            return pd.DataFrame()
        
        df = pd.read_pickle(filepath)
        print(f"Loaded DataFrame from {filepath} ({len(df)} records)")
        return df
    except Exception as e:
        print(f"Error loading from {filepath}: {e}")
        return pd.DataFrame()

def summarize_dataframe(df, title="DataFrame Summary"):
    """
    Print a summary of the DataFrame
    
    Args:
        df (pandas.DataFrame): DataFrame to summarize
        title (str): Title for the summary
    """
    print(f"\n{title}")
    print("=" * 50)
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    if not df.empty:
        print(f"\nData types:")
        print(df.dtypes)
        
        print(f"\nFirst few rows:")
        print(df.head())
        
        print(f"\nMissing values:")
        print(df.isnull().sum())

# Example usage and testing
if __name__ == "__main__":
    print("Testing json_helper module...")
    
    # Test loading daily summaries
    pattern = "data/daily_summaries/daily_summaries_*.json"
    df = load_multiple_json_files(pattern)
    
    if not df.empty:
        summarize_dataframe(df, "Daily Summaries Data")
        
        # Save to pickle for fast loading later
        save_dataframe_to_pickle(df, "data/daily_summaries_combined.pkl")
        
        # Test loading from pickle
        df_loaded = load_dataframe_from_pickle("data/daily_summaries_combined.pkl")
        
        print(f"\njson_helper module is working correctly!")
    else:
        print("No data found to test with")
