import os, re
import pathlib
import folium
import geopandas as gpd
import pandas as pd

from collections import defaultdict

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

class Data_Processor():

    def __init__(self):
        self.load_data()
        self._load_data_variables()
        self._translate_data_variables_to_index_code()

    def load_data(self):
        """
        Load data variables.
        """
        self.puma_fin_17_df = {}
        self.puma_fin_17_meta_df = {}
        self.puma_fin_12_df = {}
        self.puma_fin_12_meta_df = {}

        data_table_keys = ['S2503', 'DP04', 'DP02', 'DP03']

        for key in data_table_keys:
            csv_string_17 = './ACS_local_data/ACS_17_5YR_' + key + '_with_ann.csv'
            meta_string_17 = './ACS_local_data/ACS_17_5YR_' + key + '_metadata.csv'
            csv_string_12 = './ACS_local_data/ACS_12_5YR_' + key + '_with_ann.csv'
            meta_string_12 = './ACS_local_data/ACS_12_5YR_' + key + '_metadata.csv'            

            self.puma_fin_17_df[key] = pd.read_csv(csv_string_17, skiprows=[1], encoding = "ISO-8859-1")
            self.puma_fin_17_meta_df[key] = pd.read_csv(meta_string_17)
            self.puma_fin_12_df[key] = pd.read_csv(csv_string_12, skiprows=[1], encoding = "ISO-8859-1")
            self.puma_fin_12_meta_df[key] = pd.read_csv(meta_string_12)

        self.target = ['GEO.id2']
        metro_data_xls = pd.ExcelFile('./metro_data.xls')
        self.metro_gdp_df = metro_data_xls.parse('metro_gdp')

        
    def _load_data_variables(self):
        """
        This function initializes a list of tuples the data strings used by the data processor.
        The first string in the tuple provides the full description string while the second
        string in the tuple provides an abbreviation of the full description.
        """
        variables_S2503 = [
                           (['Owner-occupied housing units; Estimate; Occupied housing units',
                             'Owner-occupied housing units; Estimate; Occupied housing units'],'OOHU'),
                           (['Renter-occupied housing units; Estimate; Occupied housing units',
                             'Renter-occupied housing units; Estimate; Occupied housing units'],'ROHU')
        ]

        variables_DP02 = [
                          (['Estimate; HOUSEHOLDS BY TYPE - Total households - Family households (families)',
                            'Estimate; HOUSEHOLDS BY TYPE - Family households (families)'], 'TFHH'),
                          (['Estimate; HOUSEHOLDS BY TYPE - Total households - Family households (families) - Married-couple family',
                            'Estimate; HOUSEHOLDS BY TYPE - Family households (families) - Married-couple family'], 'TMCHH'),
                          (['Estimate; HOUSEHOLDS BY TYPE - Total households - Nonfamily households',
                            'Estimate; HOUSEHOLDS BY TYPE - Nonfamily households'], 'TNFHH'),
                          (['Estimate; EDUCATIONAL ATTAINMENT - Population 25 years and over - Graduate or professional degree',
                            'Estimate; EDUCATIONAL ATTAINMENT - Graduate or professional degree'], 'EAGPD'),
                          (['Estimate; EDUCATIONAL ATTAINMENT - Population 25 years and over - Less than 9th grade',
                            'Estimate; EDUCATIONAL ATTAINMENT - Less than 9th grade'], 'EAL9G'),
                          (["Estimate; EDUCATIONAL ATTAINMENT - Population 25 years and over - Bachelor's degree",
                            "Estimate; EDUCATIONAL ATTAINMENT - Bachelor's degree"], 'EABD'),
                          (["Estimate; RESIDENCE 1 YEAR AGO - Population 1 year and over - Different house in the U.S. - Different county - Different state",
                            "Estimate; RESIDENCE 1 YEAR AGO - Different house in the U.S. - Different county - Different state"],'ROYADP02'),
                          (["Estimate; RESIDENCE 1 YEAR AGO - Population 1 year and over - Different house in the U.S.",
                            "Estimate; RESIDENCE 1 YEAR AGO - Different house in the U.S."], 'ROYADHIUSDP02'),
                          (["Estimate; PLACE OF BIRTH - Total population",
                            "Estimate; PLACE OF BIRTH - Total population"], 'TPOPDP02')
        ]
        
        variables_DP04 = [
                          (['Estimate; YEAR STRUCTURE BUILT - Total housing units',
                            'Estimate; YEAR STRUCTURE BUILT - Total housing units'], 'THU'),
                          (['Estimate; VEHICLES AVAILABLE - Occupied housing units',
                            'Estimate; VEHICLES AVAILABLE - Occupied housing units'], 'VA'),
                          (['Estimate; VEHICLES AVAILABLE - Occupied housing units - No vehicles available',
                            'Estimate; VEHICLES AVAILABLE - No vehicles available'], 'NVADP04'),
                          (['Estimate; GROSS RENT - Occupied units paying rent',
                            'Estimate; GROSS RENT - Occupied units paying rent'], 'OUPR'),
                          (['Estimate; VALUE - Owner-occupied units - Median (dollars)',
                            'Estimate; VALUE - Median (dollars)'], 'EMEDHPDP04'),
                          (['Estimate; SELECTED CHARACTERISTICS - Occupied housing units - Lacking complete plumbing facilities',
                            'Estimate; SELECTED CHARACTERISTICS - Lacking complete plumbing facilities'], 'HLCPFDP04')
            
        ]
        variables_DP03 = [
                          (['Estimate; EMPLOYMENT STATUS - Civilian labor force',
                            'Estimate; EMPLOYMENT STATUS - Civilian labor force'], 'ESCLF'),
                          (['Estimate; EMPLOYMENT STATUS - Population 16 years and over',
                            'Estimate; EMPLOYMENT STATUS - Population 16 years and over'], 'ESPOP'),
                          (['Percent; PERCENTAGE OF FAMILIES AND PEOPLE WHOSE INCOME IN THE PAST 12 MONTHS IS BELOW THE POVERTY LEVEL - All people',
                            'Percent; PERCENTAGE OF FAMILIES AND PEOPLE WHOSE INCOME IN THE PAST 12 MONTHS IS BELOW THE POVERTY LEVEL - All people'], 'PPIP'),
                          (['Estimate; INDUSTRY - Civilian employed population 16 years and over - Construction',
                            'Estimate; INDUSTRY - Construction'], 'EIC'),
                          (['Estimate; INDUSTRY - Civilian employed population 16 years and over - Manufacturing',
                            'Estimate; INDUSTRY - Manufacturing'], 'EIM'),
                          (['Estimate; INDUSTRY - Civilian employed population 16 years and over - Finance and insurance, and real estate and rental and leasing',
                            'Estimate; INDUSTRY - Finance and insurance, and real estate and rental and leasing'], 'EIFIRE'),
                          (['Estimate; INDUSTRY - Civilian employed population 16 years and over - Professional, scientific, and management, and administrative and waste management services',
                            'Estimate; INDUSTRY - Professional, scientific, and management, and administrative and waste management services'], 'EIPSM'),
                          (['Estimate; INCOME AND BENEFITS (IN 2017 INFLATION-ADJUSTED DOLLARS) - Families - Median family income (dollars)',
                            'Estimate; INCOME AND BENEFITS (IN 2012 INFLATION-ADJUSTED DOLLARS) - Median household income (dollars)'], 'IBMED'),
                          
        ]
        self.data_variables = {'S2503' : variables_S2503,
                               'DP02' : variables_DP02,
                               'DP04' : variables_DP04,
                               'DP03' : variables_DP03}

        self.feature_list = [ abbrev[1] + '_PC' for key, variable_list in self.data_variables.items() for abbrev in variable_list ]

    def _translate_data_variables_to_index_code(self):
        """
        This takes a list of variable string descriptions and translates them into
        the appropriate data frame index codes
        """
        self.header_vals_17 = defaultdict(lambda : ['GEO.id2'])
        self.header_vals_12 = defaultdict(lambda : ['GEO.id2'])

        for key, list_of_variables in self.data_variables.items():
            for variable_name in list_of_variables:
                index_code_17 = self.puma_fin_17_meta_df[key][self.puma_fin_17_meta_df[key]['Id'] == variable_name[0][0]]['GEO.id'].array[0]
                self.header_vals_17[key].append(index_code_17)
                index_code_12 = self.puma_fin_12_meta_df[key][self.puma_fin_12_meta_df[key]['Id'] == variable_name[0][1]]['GEO.id'].array[0]
                self.header_vals_12[key].append(index_code_12)
                
                                                                                                               
    def _create_tables(self):
        """
        Once a set of common columns have been identified between the two tables,
        next we want to merge into all the data into a single table
        """
        self.table_dictionary = defaultdict(lambda : pd.dataframe())

        # collect the data selected in self.data_variables

        for key in self.data_variables.keys():
            self.table_dictionary[key] =   pd.merge(self.puma_fin_17_df[key][self.header_vals_17[key]],
                                                    self.puma_fin_12_df[key][self.header_vals_12[key]],
                                                    on= 'GEO.id2',
                                                    how = 'inner',
                                                    suffixes = ('_17', '_12'))

        # relabel the header values
        

        for key in self.data_variables.keys():
            for idx, (val_17, val_12) in enumerate(zip(self.header_vals_17[key][1:], self.header_vals_12[key][1:])):
                if val_17 in self.table_dictionary[key].columns:
                    self.table_dictionary[key] = self.table_dictionary[key].rename({''.join([val_17]):''.join([self.data_variables[key][idx][1],'_17'])}, axis=1)
                elif ''.join([val_17, '_17']) in self.table_dictionary[key].columns:
                    self.table_dictionary[key] = self.table_dictionary[key].rename({''.join([val_17,'_17']):''.join([self.data_variables[key][idx][1],'_17'])}, axis=1)
                if val_12 in self.table_dictionary[key].columns:
                    self.table_dictionary[key] = self.table_dictionary[key].rename({''.join([val_12]):''.join([self.data_variables[key][idx][1],'_12'])}, axis=1)
                elif ''.join([val_12, '_12']) in self.table_dictionary[key].columns:
                    self.table_dictionary[key] = self.table_dictionary[key].rename({''.join([val_12,'_12']):''.join([self.data_variables[key][idx][1],'_12'])}, axis=1)


    def _populate_pc_data_table(self):
        """
        This function populates a new data frame with data that is to be
        used in the PCA.
        """

        column_name_dictionary = defaultdict(lambda : ['GEO.id2'])
        
        def select_columns(data_frame, column_names):
            new_frame = data_frame.loc[:, column_names]
            return new_frame

        for key, list_of_variables in self.data_variables.items():
            for variable in list_of_variables:
                key_name = variable[1] + '_PC'
                column_name_dictionary[key].append(key_name)
                self.table_dictionary[key][key_name] = (self.table_dictionary[key][variable[1]+'_17'] / self.table_dictionary[key][variable[1]+'_12'] - 1)*100

        self.pca_data_frame = self.table_dictionary['S2503'].loc[:, 'GEO.id2']

        for key, column_names in column_name_dictionary.items():
            self.pca_data_frame = pd.merge(self.pca_data_frame,
                                           select_columns(self.table_dictionary[key], column_names),
                                           on='GEO.id2',
                                           how='inner')


    def standardize_data(self):
        self.X = self.pca_data_frame.loc[:, self.feature_list].values
        self.y = self.pca_data_frame.loc[:, self.target].values
        self.X = StandardScaler().fit_transform(self.X)

    def run_PCA(self):
        self.pca = PCA(n_components=2)
        principal_components = self.pca.fit_transform(self.X)
        pc = self.pca.fit(self.X)

        principal_DF = pd.DataFrame(data = principal_components,
                                    columns = ['PC1','PC2'])
        self.finalDF = pd.concat([principal_DF,
                                  self.pca_data_frame.loc[:, self.target]],
                                 axis=1)


        self.finalDF['GEO.id2'] = self.finalDF['GEO.id2'].astype(str)
        mask = self.finalDF['GEO.id2'].str.len() == 6
        self.finalDF.loc[mask,'GEO.id2'] = '0' + self.finalDF[mask]['GEO.id2']
