import logging
logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format = '%(message)s')
logger.setLevel(logging.NOTSET)

import pandas as pd

def process_opinions(chunked_df : pd.DataFrame, author_df: pd.DataFrame):
    # Process a chunk and return a dataframe with only the processed chunk
    # We process the data by throwing out opinions with no "plain text" value and no author ID
    processed_df = chunked_df.loc[(~chunked_df['plain_text'].isnull() & ~chunked_df['author_id'].isnull())].join(
        author_df,
        left_on='author_id',
        right_on='id')
    logger.info('Acquired ' + str(len(processed_df)) + ' rows of good data')
    return processed_df

def process_dockets(chunked_df : pd.DataFrame, courts : list = ['scotus','ca1','ca2','ca3','ca4',
                                                                'ca5','ca6','ca7','ca8','ca9','ca10',
                                                                'ca11','cadc']):
    # Method to process the docket_ids for only federal courts
    # Filters the dockets for only federal courts
    return chunked_df.loc[chunked_df['court_id'].isin(courts)]


def process_opinion_clusters(chunked_df : pd.DataFrame, docket_df : pd.DataFrame):
    # Method to process the opinion clusters
    # Screens out null judges column and also joins onto federal courts
    processed_df = chunked_df.loc[~chunked_df['judges'].isnull()].merge(docket_df[['id','court_id']],
                                                                        left_on='docket_id',
                                                                        right_on='id'
                                                                        )
    logger.info('Acquired ' + str(len(processed_df)) + ' rows of good data')
    return processed_df

def join_onto_opinions(chunked_df : pd.DataFrame,
                       cluster_df : pd.DataFrame,
                       author_df : pd.DataFrame,
                       keep_cols : list = ['plain_text','date_filed','case_name','slug_x','court_id','slug_y','political_party']):
    # Method to join the docket IDs onto the opinion text
    processed_df = chunked_df.merge(cluster_df, left_on='cluster_id', right_on='id_x').merge(author_df
                                                                                             , left_on='author_id'
                                                                                             , right_on='id')
    logger.info('Acquired ' + str(len(processed_df)) + ' rows of good data')
    return processed_df[keep_cols]

class parseData:
    def __init__(self
                 , chunked_df : pd.DataFrame = None
                 , file_path : str = '~/Documents/judges-nlp/data/opinions-2022-11-30.csv.bz2'
                 , chunksize : int = 100000
                 , output_path : str = '~/Documents/judges-nlp/data/cleaned_data.csv.bz2'):
        # Initialize our processor
        self.chunked_df = chunked_df
        self.file_path = file_path
        self.chunksize = chunksize
        self.output_path = output_path

    def chunk_data(self, process, *args):
        # Split the data into chunks and process each chunk according to an arbitrary processing method
        new_df = pd.DataFrame()
        with pd.read_csv(self.file_path, chunksize = self.chunksize) as chunked_df:
            self.chunked_df = chunked_df
            rownum = 0
            for sub_df in self.chunked_df:
                logger.info('Processing row: '+str(rownum))
                processed_df = process(sub_df, *args)
                new_df = pd.concat([new_df, processed_df])
                rownum += self.chunksize
        return new_df

    def run_parse(self, process, *args):
        final_df = self.chunk_data(process, *args)
        final_df.to_csv(self.output_path, encoding='utf8', compression={'method':'bz2'})

if __name__ == '__main__':
    #processor = parseData(output_path='~/Documents/judges-nlp/data/cleaned_dockets.csv.bz2',
    #                      file_path='~/Documents/judges-nlp/data/dockets-2022-11-30.csv.bz2')
    author_df1 = pd.read_csv('~/Documents/judges-nlp/data/people-db-people-2022-11-30.csv.bz2', compression='bz2')
    author_df2 = pd.read_csv('~/Documents/judges-nlp/data/people-db-political-affiliations-2022-11-30.csv.bz2', compression='bz2')
    author_df = author_df1.merge(author_df2, on='id')
    author_df = author_df[['id', 'slug', 'political_party']]
    #processor.run_parse(process_opinions, author_df)
    cluster_df = pd.read_csv('~/Documents/judges-nlp/data/cleaned_clusters.csv.bz2', compression='bz2')
    processor = parseData(output_path='~/Documents/judges-nlp/data/joined_data.csv.bz2',
                          file_path='~/Documents/judges-nlp/data/cleaned_data.csv.bz2')
    processor.run_parse(join_onto_opinions, cluster_df, author_df)


