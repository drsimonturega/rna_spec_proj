# This class will contain methods that extract data from a particular 
# data source, these sources will include CSV files, an API 
# and an S3 bucket.

# Imported libaries
#import boto3
import numpy as np
import pandas as pd
import requests
#import tabula

class DataExtractor:

    # class constructor
    def __init__(self):
        """Inintailises the class"""
        return
        
    def read_rds_table(self, tb_name):  # can add external arguments
        """
        Extracts tables from a data base

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        tb_name -- name of new data base table to be extracted
        """
        for tab in self.tab_lst:
            name = (f'df_{tb_name}')
            if tb_name in tab:
                self.df_tran = pd.read_sql_table(tab, self.engine)
                break       
        return  self.df_tran
    
    