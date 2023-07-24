import pandas as pd
import nltk
import time
from os import listdir, wait
from os.path import isfile, join
import re
import string
import threading
from googleapiclient import discovery
import json
from retrying import retry
import logging

class PerspectiveRequests():
    def __init__(self, dataframe_path, text_field, api_key_path ,n_threads=10):
        """
        dataframe_path: path to the dataframe used
        text_field: field within the dataframe to be analyzed by perspective
        n_threads: number of threads
        """
        self._path = dataframe_path
        self._text_field = text_field
        self.n_threads = n_threads
        self.api_key_path = api_key_path
        self.api_key = open(api_key_path, "r").read()

        self.df = pd.read_csv(self._path,lineterminator='\n')
        self.df = self.df.sample(100)
        print('Number of rows:', len(self.df))
        self.df.dropna(subset = [text_field], inplace=True)
        print("Number of rows after dropping NaN rows:", len(self.df))

    def threads_create_client(self):
        """
        Used to create multiple client objects for threaded use
        """
        client = discovery.build(
            "commentanalyzer",
            "v1alpha1",
            developerKey=self.api_key,
            discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
            static_discovery=False,
        )
        return client

    def global_create_client(self):
        """
        creates one client object for the entire class. Do not use this if you are using threads

        """
        self.client = discovery.build(
            "commentanalyzer",
            "v1alpha1",
            developerKey=self.api_key,
            discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
            static_discovery=False,
        )

    def _slice_list(self, list_to_slice, num_sub_lists):
        """
        list_to_slice: list to slice
        num_sub_lists: number of lists after the slice

        returns a list of lists
        """
        sub_lists = [
            list_to_slice[i * (len(list_to_slice) // num_sub_lists):(i + 1) * (len(list_to_slice) // num_sub_lists)]
            for i in range(num_sub_lists)
        ]
        return sub_lists
    
    def _loop_requests(self, text_list, thread_id, client,sleep_time=0.4):
        """
        helper to loop requests over each list of text
        """
        for text in text_list:
            self.toxicity_request(text, thread_id, client, sleep_time=sleep_time)


    @retry(wait_exponential_multiplier=1000, wait_exponential_max=20000, stop_max_attempt_number=10)
    def toxicity_request(self, text, thread_id, client,sleep_time=0.4):
        """
        text: text to be submitted to Perspective
        thread_id: thread id for debugging/logging purposes
        client: each thread must have a different client
        """
        text_id = text[1]
        text = text[0]
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
            response = client.comments().analyze(body=analyze_request).execute()
            attributes["TOXICITY"] = response["attributeScores"]["TOXICITY"]["summaryScore"]["value"]
            attributes["SEVERE_TOXICITY"] = response["attributeScores"]["SEVERE_TOXICITY"]["summaryScore"]["value"]
            attributes["IDENTITY_ATTACK"] = response["attributeScores"]["IDENTITY_ATTACK"]["summaryScore"]["value"]
            attributes["INSULT"] = response["attributeScores"]["INSULT"]["summaryScore"]["value"]
            attributes["PROFANITY"] = response["attributeScores"]["PROFANITY"]["summaryScore"]["value"]
            attributes["THREAT"] = response["attributeScores"]["THREAT"]["summaryScore"]["value"]
            print("Thread",thread_id,":",attributes["TOXICITY"], "text_id-correct:",text_id)
            return attributes
        except Exception as e:
            print("text_id:",text_id,e)
            raise IOError("text_id:",text_id,e)

    def threaded_requests(self):
        """
        performs threaded requests. Creates the specified amount of threads and speeds up the requests.

        """
        texts_list = []
        for index, row in self.df.iterrows():
            texts_list.append((row["comment_text"], row["comment_id"]))

        sliced_texts_list = self._slice_list(texts_list,self.n_threads)
        threads = []
        for thread_count in range(self.n_threads):
            # one client for each thread. Otherwise we'll get errors
            client = self.threads_create_client()
            thread = threading.Thread(target=self._loop_requests, args=(sliced_texts_list[thread_count],thread_count,client))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


pp = PerspectiveRequests("/data/Downloads/comments.csv","comment_text", "api_key", n_threads=10)
# pp.preprocess_text(overwrite=True)
# pp.retrieve_features()
pp.threaded_requests()