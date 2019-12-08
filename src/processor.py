import os
import re
import process_data
import numpy as np
import pandas as pd
from collections import defaultdict

class Processor(process_data.Processor):
    main_categories = set(['Top/News/World', 
                           'Top/News/Washington',
                           'Top/News/New York and Region',
                           'Top/News/Front Page',
                           'Top/News/Business',
                           'Top/News/U.S.',
                           'Top/News/Sports',
                           'Top/News/Obituaries',
                           'Top/News/Health',
                           'Top/News/Education',
                           'Top/News/Science',
                           'Top/News/Technology'])

    def __init__(self):
        super().__init__()
        self.ids = set()
        self.general_descriptors = defaultdict(int)
        self.news = dict(Id = [], Text = [], World = [], Washington = [], New_York_and_Region = [],
            Front_Page = [], Business = [], US = [], Sports = [], Obituaries = [], Health = [],
            Education = [], Science = [], Technology = [])   

        
    def set_document_id(self, id):
        assert id not in self.ids
        self.news['Id'].append(int(id))
        self.news['World'].append(0)
        self.news['Washington'].append(0)
        self.news['New_York_and_Region'].append(0)
        self.news['Front_Page'].append(0)
        self.news['Business'].append(0)
        self.news['US'].append(0)
        self.news['Sports'].append(0)
        self.news['Obituaries'].append(0)
        self.news['Health'].append(0)
        self.news['Education'].append(0)
        self.news['Science'].append(0)
        self.news['Technology'].append(0)
        
    def process_general_descriptors(self, descriptors):
        for text in descriptors:
            self.general_descriptors[text] += 1

    def get_column_name(self, column_name):
        return column_name.replace('.', '').replace(' ', '_')

    def process_taxonomic_news_classifiers(self, classifiers):
        assert all(['Top/News/' in text for text in classifiers])

        for classifier in classifiers:
            m = re.match(r'Top/News/((\w|\s|\.|\,|\(|\))+)', classifier)
            column_name = self.get_column_name(m.group(1))
            self.news[column_name][-1] = 1

    def process_body(self, text):
        # Need to convert to utf-8 becaues pytables cannot handle some types of unicode characters like émigré
        #text = text.encode("utf-8", "replace")
        #self.news['Text'].append(np.string_(text[0:1024])) # Truncate to fit the max column size
        text = str(text)
        self.news['Text'].append(text)

    def close(self):
        self.df = pd.DataFrame(self.news)
        self.news = None

if __name__ == "__main__":
    folder = '/Users/marceloblinder/w266/data'
    output_file = 'nyt_full.parquet.gz'

    if os.path.exists(output_file):
        os.remove(output_file)

    processor = Processor()

    for year in range(1987, 2008):
        print(year)
        process_data.process_all_files(folder, f'nyt_corpus_{year}.zip', processor)
        
    processor.close()
    processor.df.to_parquet(output_file, compression='gzip')
