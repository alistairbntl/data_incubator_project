import os, re
import pathlib
import folium
import geopandas as gpd
import pandas as pd

from collections import defaultdict

class GeoInformation():

    def __init__(self):
        """
        Loads the following geographic data
        """
        self.load_data()

    def load_data(self):

        puma_gdf_list = []
        for file_name in os.listdir('./'):
            if 'puma' in file_name:
                        puma_gdf_list.append(gpd.read_file(f"zip://{pathlib.Path(file_name)}"))
                        puma_gdf = pd.concat(puma_gdf_list, axis=0, ignore_index=True)
        self.puma_gdf = pd.concat(puma_gdf_list, axis=0, ignore_index=True)
       
        metro_data_xls = pd.ExcelFile('./metro_data.xls')
        metro_state_map_df = metro_data_xls.parse('metro_2_state')
        self.metro_state_map_df = self._relabel_metro_state_map_df(metro_state_map_df)

    def _relabel_metro_state_map_df(self, metro_df):
        """
        Helper function to rename the metro 
        """
        header = ['CBSA_Code',
                  'Metro_Div_Code',
                  'CSA_Code',
                  'CBSA_Title',
                  'MSA',
                  'Metr_Div_Title',
                  'CSA_Title',
                  'County',
                  'State',
                  'FIPS_State_Code',
                  'FIPS_County_Code',
                  'Central_Outlying_County']
        metro_df = metro_df[2:]
        metro_df.columns = header
        return metro_df

    def set_active_state(self, state):
        self.active_state = state

    def get_active_state(self):
        return self.active_state

    def set_active_metro(self, metro):
        self.active_metro = metro

    def get_active_metro(self):
        return self.active_metro

    def get_states(self):
        """ Return a list of U.S. states """
        return [state for state in self.metro_state_map_df['State'].dropna().unique() if state not in ['Puerto Rico']]
    
    def get_state_code(self, state):
        """ Returns the state code """
        return self.metro_state_map_df[self.metro_state_map_df['State']==state]['FIPS_State_Code'].iloc[0]

    def get_state_metros(self, state):
        """  Returns a list of metro areas for the selected state """
        state_mask = self.metro_state_map_df['State'] == state
        metro_ = self.metro_state_map_df[state_mask]['CBSA_Title'].dropna().unique()
        return metro_.tolist()

    def get_metro_counties(self, metro):
        metro_mask = self.metro_state_map_df['CBSA_Title'] == metro
        metro_code = self.metro_state_map_df[metro_mask]['CBSA_Code'].iloc[0]
        metro_code_mask = self.metro_state_map_df['CBSA_Code'] == metro_code
        return self.metro_state_map_df[metro_code_mask]['County'].unique()


    def _get_active_state_metro_masks(self):
        state = self.get_active_state()
        state_code = self.get_state_code(state)
        metro = self.get_active_metro()
        metro_names = metro[:-4].split('-')

        state_mask = self.puma_gdf['STATEFP10']==state_code
        
        conditional_str = ''
        for city in metro_names:
            conditional_str += city+'|'
        for county in self.get_metro_counties(metro):
            conditional_str += county[:-7]+'|'

        city_county_mask = self.puma_gdf['NAMELSAD10'].str.contains(conditional_str[:-1],
                                                                    regex=True)
        return state_mask, city_county_mask
    
    def set_plot_data(self):
        state_mask, city_mask = self._get_active_state_metro_masks()
#       return self.plotting_df = self.puma_gdf[state_mask]
        return self.puma_gdf[state_mask & city_mask]

    def get_active_city_location(self):
        state_mask, city_mask = self._get_active_state_metro_masks()
        
        return [self.puma_gdf[state_mask & city_mask]['INTPTLAT10'].iloc[0],
                self.puma_gdf[state_mask & city_mask]['INTPTLON10'].iloc[0]]
