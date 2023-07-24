import pandas as pd
import nltk
import time
from os import listdir, wait
from os.path import isfile, join
import re
import string
from googleapiclient import discovery
import json
import logging

class PerspectiveRequests():
    def __init__(self, dataframe_path, text_field):
        """
        dataframe_path: path to the dataframe used
        text_field: field within the dataframe to be analyzed by perspective

        """
        self._path = dataframe_path
        self._text_field = text_field
        self.df = pd.read_csv(self._path,  dtype = 'str')
        print('Number of rows:', len(self.df))


    # def _get_list_of_files(self):
    #     # adds path to file name
    #     self._files_list = [
    #         self._path + "/" + f for f in listdir(self._path) if isfile(join(self._path, f))]

    def create_client(self, api_key_path):
        self.api_key = open(api_key_path, "r").read()

        self.client = discovery.build(
            "commentanalyzer",
            "v1alpha1",
            developerKey=self.api_key,
            discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
            static_discovery=False,
        )

    def toxicity_request(self, text, sleep_time=0.1):
        time.sleep(sleep_time)
        attributes = {'TOXICITY': -1,
                      'SEVERE_TOXICITY': -1,
                      'IDENTITY_ATTACK': -1,
                      'INSULT': -1,
                      'PROFANITY': -1,
                      'THREAT': -1}

        analyze_request = {
            'comment': {'text': text},
            'requestedAttributes': {'TOXICITY': {},
                                    'SEVERE_TOXICITY': {},
                                    'IDENTITY_ATTACK': {},
                                    'INSULT': {},
                                    'PROFANITY': {},
                                    'THREAT': {}},
            'languages': ["pt"],
            'doNotStore': True
        }

        try:
            response = self.client.comments().analyze(body=analyze_request).execute()
            attributes["TOXICITY"] = response["attributeScores"]["TOXICITY"]["summaryScore"]["value"]
            attributes["SEVERE_TOXICITY"] = response["attributeScores"]["SEVERE_TOXICITY"]["summaryScore"]["value"]
            attributes["IDENTITY_ATTACK"] = response["attributeScores"]["IDENTITY_ATTACK"]["summaryScore"]["value"]
            attributes["INSULT"] = response["attributeScores"]["INSULT"]["summaryScore"]["value"]
            attributes["PROFANITY"] = response["attributeScores"]["PROFANITY"]["summaryScore"]["value"]
            attributes["THREAT"] = response["attributeScores"]["THREAT"]["summaryScore"]["value"]
        except Exception as e:
            print(e)

        print(attributes["TOXICITY"])
        return attributes

    # def _returns_only_text(self, char):
    #     return char in string.punctuation or char == " " or str.isalnum(char)

    # def _filters_string(self, string):
    #     if(string):
    #         return ''.join(list(filter(self._returns_only_text, string)))
    #     else:
    #         return " "

    # def preprocess_text(self, overwrite=False, save_log=True, clean_title=False, clean_description=False):
    #     if (save_log):
    #         logging.basicConfig(level=logging.INFO, filename="preprocess.log", filemode="w",
    #                             format='%(asctime)s,%(message)s', datefmt='%d-%b-%y %H:%M:%S')

    #     if not hasattr(self, '_files_list'):
    #         self._get_list_of_files()
    #     for i in range(len(self._files_list)):
    #         print("Processed "+"{:.4}".format(str(i /
    #                                               len(self._files_list)*100))+"% of files")
    #         df = pd.read_csv(self._files_list[i])
    #         df = df.fillna(' ')

    #         # Removes links from description
    #         df['description'] = df['description'].str.replace(
    #             'http\S+|www.\S+', '', case=False)

    #         df['description'] = df['description'].apply(
    #             lambda x: self._filters_string(x))

    #         # Optional preprocessing
    #         if(clean_title):
    #             df['title'] = df['title'].apply(lambda x: "".join(
    #                 [i for i in x if i not in string.punctuation]))
    #             df['title'] = df['title'].apply(lambda x: x.lower())
    #             df['title'] = df['title'].apply(lambda x: ' '.join(
    #                 [word for word in x.split() if word not in (stop)]))
    #         # Optional preprocessing
    #         if(clean_description):
    #             df['description'] = df['description'].apply(
    #                 lambda x: x.lower())
    #             df['description'] = df['description'].apply(lambda x: ' '.join(
    #                 [word for word in x.split() if word not in (stop)]))
    #             # punctuation needs to be last, or else breaks the urls
    #             df['description'] = df['description'].apply(
    #                 lambda x: "".join([i for i in x if i not in string.punctuation]))

    #         if(overwrite):
    #             df.to_csv(self._files_list[i], index=False)
    #         else:
    #             df.to_csv(self._files_list[i]+"_clean.csv", index=False)

    #         if(save_log):
    #             logging.info("Overwrite="+str(overwrite) +
    #                          ",file:"+str(self._files_list[i]))

    def retrieve_features(self, save_log=True):
        if(save_log):
            logging.basicConfig(level=logging.INFO, filename="features.log", filemode="w",
                                format='%(asctime)s,%(message)s', datefmt='%d-%b-%y %H:%M:%S')
        try:
            aux_column = self.df.apply(lambda row: pp.toxicity_request(
                str(row[self._text_field])), axis=1)

            self.df["toxicity"] = aux_column.apply(
                lambda x: x["TOXICITY"])
            self.df["severe_toxicity"] = aux_column.apply(
                lambda x: x["SEVERE_TOXICITY"])
            self.df["insult"] = aux_column.apply(
                lambda x: x["INSULT"])
            self.df["profanity"] = aux_column.apply(
                lambda x: x["PROFANITY"])
            self.df["threat"] = aux_column.apply(
                lambda x: x["THREAT"])
            self.df["identity_attack"] = aux_column.apply(
                lambda x: x["IDENTITY_ATTACK"])
    # ADICIONAR ATAQUE DE IDENTIDADE
        except Exception as e:
            print(e)
            self.df["toxicity"] = -1
            self.df["severe_toxicity"] = -1
            self.df["insult"] = -1
            self.df["profanity"] = -1
            self.df["threat"] = -1
            self.df["identity_attack"] = -1
            if(save_log):
                logging.error("ERROR,"+str(e))


pp = PerspectiveRequests("comments_sample.csv","comment_text")
# pp.preprocess_text(overwrite=True)
pp.create_client("api_key")
pp.retrieve_features()

# print(pp.toxicity_request("eu sou o breno"))
# time.sleep(1)
# pp.toxicity_request("what the fuck!!!")
# time.sleep(1)
# pp.toxicity_request("WHAT THE FUCK!!!")
# time.sleep(1)
# pp.toxicity_request("what the fuck...")
# time.sleep(1)
# print("Sem stop words\n")
# pp.toxicity_request("what fuck")
# time.sleep(1)
# pp.toxicity_request("what fuck!!!")
# time.sleep(1)
# pp.toxicity_request("WHAT FUCK!!!")
# time.sleep(1)
# pp.toxicity_request("what fuck...")

