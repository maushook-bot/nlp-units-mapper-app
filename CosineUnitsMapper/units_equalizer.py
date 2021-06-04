#############################################################################################################
# units_equalizer.py : Automatically finds the Cosine Similarity score between source and track units #######
#############################################################################################################
'''
version: 2.0
'''

# Importing the Modules:-
import pandas as pd
from configobj import ConfigObj
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Config Object
conf = ConfigObj('config.ini')


class UnitsAutoMapper:
    def __init__(self, local, stage, prod):
        print("\n#####################################################################")
        print("~~~~~~~~~~~~~~~ Inside Units Mapper Class ~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("#####################################################################")

        # Input Config Parameters:-
        self.cust_info = conf['cust_info']
        self.migration_phase = conf['migration_phase']
        self.folio_select = conf['folio_select']
        self.source_sql = conf['source_sql']
        self.trk_sql = conf['trk_sql']
        self.left_select = conf['left_select']
        self.right_select = conf['right_select']

        # DB Engine Parameters
        self.local = local
        self.stage = stage
        self.prod = prod

        # Get Stage and Production Units Data:-
        print("Extracting the Source Dataset: Stage")
        self.df_src = pd.read_sql(self.source_sql, con=self.stage)
        print("Extracting the Units Dataset: Production")
        self.df_trk = pd.read_sql(self.trk_sql, con=self.prod)

    def dataset_filter(self):
        print("Filtering/Selecting the relevant units related columns in source and track")
        df_left = self.df_src[[self.left_select]].drop_duplicates().reset_index(drop=True)
        df_right = self.df_trk[[self.right_select, 'id']]
        return df_left, df_right

    def units_mapper(self, df_left, df_right):
        print("Start of Unit Mapping Module")

        df_master = pd.DataFrame({'source_units': [], 'track_units': [], 'cosine_score': []})
        for index, row in df_left.iterrows():
            # Left and Right Match Strings Selection:-
            a = df_left.iloc[:, 0].tolist()[index]
            lst_b = df_right.iloc[:, 0].tolist()

            # Vectorize all strings by creating a Bag of Words Matrix:
            vectorizer = CountVectorizer()
            X = vectorizer.fit_transform([a] + lst_b).toarray()
            df_parse = pd.DataFrame(X)  # sparse matrix of length (1+length of list)x(total no.of unique words)

            # Prepare list of vectors for each set of strings ( 1 + 46)
            lst_vectors = df_parse.values.tolist()

            # Calculate the cosine similarity between 1 & each of 46)
            cosine_sim = cosine_similarity(lst_vectors)

            # Convert the result of cos_sim to df
            df_cosine_sim = pd.DataFrame(cosine_sim)

            # Filtering the matches based on threshold value:-
            threshold = 0.0
            scores = df_cosine_sim.loc[0, 1:]
            scores = scores.reset_index(drop=True)
            match_scores = scores[scores > threshold]
            match_scores.sort_values(axis=0, ascending=False, inplace=True)

            match_indexes = list(match_scores.index)
            match_strings = [lst_b[i] for i in match_indexes]

            source_list = [a for i in range(0, len(match_indexes))]

            df_match = pd.DataFrame(
                {'source_units': source_list, 'track_units': match_strings, 'cosine_score': match_scores}).sort_values(
                by='cosine_score', ascending=False).head(1)
            df_master = pd.concat([df_master, df_match])

        return df_master

    def extract_cabin_id(self, df_mapper, df_right):
        print("Extracting the Cabin id : Master Mapped Data frame")

        # Merging the Master Mapper df with track units table for Cabin ID:-
        df_final = pd.merge(left=df_mapper, right=df_right, how='inner', left_on='track_units',
                            right_on=self.right_select)  # has only matched records

        # Merging the Master Mapper df with track units table for Cabin ID along with Folio:- (both matched & unmatched)
        df_source_folio = self.df_src[[self.folio_select, self.left_select]]
        df_final_master = pd.merge(left=df_source_folio, right=df_final, how='outer', left_on=self.left_select,
                                   right_on='source_units')
        df_final_master.sort_values(by='id', inplace=True)  # has folio and both matched anf unmatched records

        # Round Cosine scores to 2:-
        df_final_master = df_final_master.round(decimals=2)

        return df_final, df_final_master

    def load_stage(self, df_final_master):
        print("Loading Master Mapped Data : Stage")
        df_final_master.to_sql(f"nlp_um_{self.migration_phase}", con=self.stage, index=False, if_exists='replace')

    def load_local(self, df_final_master):
        print("Loading Master Mapped Data : Local")
        df_final_master.to_sql(f"nlp_um_{self.migration_phase}", con=self.local, index=False, if_exists='replace')
