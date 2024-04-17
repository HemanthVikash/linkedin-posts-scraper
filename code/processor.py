import PySimpleGUI as sg
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
import os

import yaml

from tqdm import tqdm



with open("./settings.yml") as stream:
    try:
        SETTINGS = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


class PostsProcessor:
    def __init__(self):
        self.posts:pd.DataFrame = None
        self.filtered_posts:pd.DatacoFrame = None
        self.geolocator = Nominatim(user_agent="app.test", timeout=10)
        tqdm.pandas()
        

    def __get_country(self, location_string: str):
        if location_string != None:
            try:
                location = self.geolocator.geocode(location_string, addressdetails=True)
            except TimeoutError as err:
                print(f"TimeOut error for {location_string}")
                print(f"{err}")
                location = None
                
            if location is None:
                return np.nan, np.nan
            return location.raw['address']['country'], location.raw['display_name']
        return np.nan, np.nan

    '''
    Read from input_file and save in self.posts
    '''
    def read_input_posts(self):
        self.posts = pd.read_csv(SETTINGS['data_files']['input_file'])
        

    def process(self):
        self.posts['country'], self.posts['formatted_location'] = zip(*self.posts['location'].progress_map(self.__get_country))
        self.posts.drop(columns=['location'], inplace=True)
        self.posts.rename({
            'formatted_location': 'location'
        }, axis=1, inplace=True)
        self.posts.loc[self.posts.followers == '500+', 'followers'] = np.nan

    def filter_posts(self, options: dict) -> pd.DataFrame:
        filtered = self.posts.copy()

        for key, value in options.items():
            if value is not None:
                filtered = filtered[filtered[key] == value]
        
        return filtered
    

    def save(self, annotated: bool = False):
        # Save to daily posts
        # Write to daily_post_file which is unfiltered
        if not annotated:
            self.posts.to_csv(SETTINGS['data_files']['daily_posts'], index=False)

        else: 
            # Read annotated files for today
            annotated = pd.read_csv(SETTINGS['data_files']['annotated_daily'])
            ## Save to legacy file for a database
            annotated.to_csv(SETTINGS['data_files']['annotated'], mode='a', index=False, header=False)

        pass

    def preprocess_for_annotation(self):
        
        # Read input posts from scraper project
        self.read_input_posts()

        # Preprocess the posts
        self.process()


        # save preprocessed for annotation software
        self.save(annotated=False)
    

        


def main():
    
    pp = PostsProcessor()
    pp.preprocess_for_annotation()


    # Once annotated with software, download as name=SETTINGS['data_files']['annotated_daily']
    # Then call the save function to append the current annotations to a larger csv file safely
    # pp.save(annotated=True)
    


if __name__ == "__main__":
    main()