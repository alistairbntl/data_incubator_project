import os, re
import pathlib
import folium
import geopandas as gpd
import pandas as pd

class StorageClass():

    def __init__(self):
        self.load_data()
        self.states = [x for x in self.metro_df['State'].unique() if str(x) != 'nan']

    def load_data(self):
        states_file_name= "tl_2017_us_state.zip"
        states_url = f"https://www2.census.gov/geo/tiger/TIGER2017/STATE/{states_file_name}"
        states_file = pathlib.Path(states_file_name)

        zipcode_file_name = "tl_2017_us_zcta510.zip"
        zipcode_url = f"https://www2.census.gov/geo/tiger/TIGER2017/ZCTA5/{zipcode_file_name}"
        zipcode_file = pathlib.Path(zipcode_file_name)

        county_file_name = 'tl_2017_us_county.zip'
        county_url = f"https://www2.census.gov/geo/tiger/TIGER2017/COUNTY/{county_file_name}"
        county_file = pathlib.Path(county_file_name)

        metro_file_name = 'tl_2017_us_cbsa.zip'
        metro_url = f"https://www2.census.gov/geo/tiger/TIGER2017/CBSA/{metro_file_name}"
        metro_file = pathlib.Path(metro_file_name)

        puma_gdf_list = []
        for file_name in os.listdir('./'):
            if 'puma' in file_name:
                        puma_gdf_list.append(gpd.read_file(f"zip://{pathlib.Path(file_name)}"))
                        puma_gdf = pd.concat(puma_gdf_list, axis=0, ignore_index=True)
        self.puma_gdf = pd.concat(puma_gdf_list, axis=0, ignore_index=True)
        self.states_gdf = gpd.read_file(f"zip://{states_file}")
        self.county_gdf = gpd.read_file(f"zip://{county_file}")
        self.metro_gdf = gpd.read_file(f"zip://{metro_file}")

        metro_xls_df = pd.ExcelFile('./metropolitan_data_Sep_2018.xls')
        metro_xls_df.sheet_names
        metro_df = metro_xls_df.parse('List 1')
        self.metro_df = self._relabel_metro_df(metro_df)


    def _relabel_metro_df(self, metro_df):
        header = ['CBSA_Code','Metro_Div_Code','CSA_Code','CBSA_Title','MSA','Metr_Div_Title','CSA_Title','County','State','FIPS_State_Code','FIPS_County_Code','Central_Outlying_County']
        metro_df = metro_df[2:]
        metro_df.columns = header
        return metro_df

    def get_selected_metros(self, selected_state):
#        metro_df[metro_df.State==selected_state.value].iloc[2]
        self.selected_state = selected_state
        self.state_code = self.metro_df[self.metro_df.State==selected_state]['FIPS_State_Code'].iloc[0]
        metro = self.metro_df[self.metro_df.State==selected_state]['CBSA_Title'].unique()
        self.metro = [x for x in metro if str(x) != 'nan']

    def get_select_counties(self, selected_metro):
        self.selected_metro = selected_metro
        self.metro_code = self.metro_df[self.metro_df.CBSA_Title == selected_metro]['CBSA_Code'].iloc[0]
        self.counties = self.metro_df[self.metro_df.CBSA_Code == self.metro_code]['County'].unique()
        
        
    def set_plot_data(self):
        self.county_lst = []
        for county in self.counties:
            self.county_lst.append(self.county_gdf[(self.county_gdf['NAMELSAD']==county) & (self.county_gdf['STATEFP']==self.state_code) ])
            self.state_map = self.states_gdf[self.states_gdf['NAME'] == self.selected_state]
            self.city_map = self.metro_gdf[self.metro_gdf['CBSAFP'] == self.metro_code]
            self.state_map.to_file("curr_state.geojson",driver='GeoJSON')
            self.city_map.to_file("curr_city.geojson", driver="GeoJSON")

        metro_names = self.selected_metro[:-4].split('-')
        self.conditional_str = ''
        for city in metro_names:
            self.conditional_str += city+'|'
        for county in self.counties:
                self.conditional_str += county[:-7]+'|'
        self.plotting_df = self.puma_gdf[(self.puma_gdf['STATEFP10']==self.state_code) & self.puma_gdf['NAMELSAD10'].str.contains(self.conditional_str[:-1], regex=True)]

            
    def jupyter_plot_state(self):
        base = self.state_map.plot()
        color_cycle = ['lightcoral','firebrick','red','greenyellow','palegreen','lawngreen']*100
        self.city_map.plot(ax=base,color=color_cycle[1])

    def jupyter_plot_county(self):
        base = self.state_map.plot()
        color_cycle = ['lightcoral','firebrick','red','greenyellow','palegreen','lawngreen']*100
        for i,county in enumerate(self.county_lst):
            county.plot(ax=base,color=color_cycle[i+1])

    def jupyter_plot_puma(self):
        df = self.puma_gdf[(self.puma_gdf['STATEFP10']==state_code) & self.puma_gdf['NAMELSAD10'].str.contains(self.conditional_str[:-1], regex=True)]
        base = city_map.plot()
        color_cycle = ['lightcoral','firebrick','red','greenyellow','palegreen','lawngreen']*100
        j=0
        for i,county in df.iterrows():
                county_df = df[j:j+1]
                j+=1
                county_df.plot(ax=base,color=color_cycle[i%(len(color_cycle))])   
