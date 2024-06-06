# Imported libaries
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
import yaml

#import pandas as pd

# Methods to connect with and upload data to the database
class DatabaseConnector(DataExtractor, DataCleaning):

    # class constructor
    def __init__(self):
        """Inintailises the class"""
        return
        
    def read_db_creds(self, cred_file,):
        """
        load db credentials from a *.ymal file

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        cred_file -- *.ymal file with data base credentials 
        """
        with open(f'{cred_file}') as f:
            self.cred_dict = yaml.load(f, Loader=yaml.FullLoader)
        self.init_db_engine()
        return self.cred_dict 
    
    def init_db_engine(self):  # can add external arguments
        """
        read credentials, initialise and return an sqlalchemy db engine

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        """
        self.engine = create_engine(f"postgresql+psycopg2://{self.cred_dict['RDS_USER']}:{self.cred_dict['RDS_PASSWORD']}@{self.cred_dict['RDS_HOST']}:{self.cred_dict['RDS_PORT']}/{self.cred_dict['RDS_DATABASE']}")
        #self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        self.engine.connect()
        return self.engine 
    
    
    def list_db_tables(self):  # can add external arguments
        """
        Lists data base tables

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        """
        inspector = inspect(self.engine)
        self.tab_lst = inspector.get_table_names()
        print(f'here is {self.tab_lst}')
        return self.tab_lst
    
    def cast_col(self):  
        """
        col_cast

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        """
        work_tab = pd.read_sql_table('order_table', self.engine)
        print(work_tab.head())
        return work_tab

    def upload_to_db(self, tab_name):
        """
        Uploads tables to data base

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        tab_name -- name of new data base table created from self.df_clean
        """
        self.df_clean.to_sql(tab_name, self.engine, if_exists='replace')
        inspector = inspect(self.engine)
        print(inspector.get_table_names())
        return

def csv_to_db(self, csv_name, folder_name):
        """
        import tables in *.csv format to pandas

        Keyword arguments:
        self -- variables that store information unique to each 
        object created from the class
        tab_name -- name of new data base table created from self.df_clean
        """
        csv_name = pd.read_csv(f'{folder_name}/{csv_name}.csv')
        csv_name.head()
        return



if __name__ == "__main__":
    #data base operations    
    up_ld = DatabaseConnector()
    DatabaseConnector.read_db_creds(up_ld, 'db_creds.yaml')
    DatabaseConnector.list_db_tables(up_ld)
    print(f'End')
    
