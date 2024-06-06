# Methods to clean data from data sources

# Imported libaries
from data_extraction import DataExtractor
import numpy as np
import pandas as pd


class DataCleaning:

    # class constructor
    def __init__(self):
        """Inintailises the class"""
        return
    
    def clean_na_in_data(self):
        """
        Remove na data with numerical analysis

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        """
        # Remove na data with numerical analysis
        print("percentage of missing values in each column:")
        print(self.df_tran.isna().mean() * 100)
        self.df_clean = self.df_tran.dropna(how = 'any', axis = 0)
        print("percentage of missing values in each column:")
        print(self.df_clean.isna().mean() * 100)                
        return self.df_clean

    