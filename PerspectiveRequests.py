import pandas as pd
import nltk
import time
import os
from os import listdir, wait
from os.path import isfile, join
import re
import string
import threading
from googleapiclient import discovery
import json
from retrying import retry
import logging
import logging

class PerspectiveRequests():
    def __init__(self, dataframe_path, text_field, text_id_field, api_key_path ,n_threads=10):
        """
        dataframe_path: path to the dataframe used
        text_field: field within the dataframe to be analyzed by perspective
        text_id_field: crucial to retrieve which comment is which after running requests
        n_threads: number of threads
        """
        self._path = dataframe_path
        self._text_field = text_field
        self._text_id_field = text_id_field
        self._formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

        self.n_threads = n_threads
        self.api_key_path = api_key_path
        self.api_key = open(api_key_path, "r").read()

        self.df = pd.read_csv(self._path,lineterminator='\n')
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
        if(num_sub_lists > len(list_to_slice)):
            # if we want more sublists than the entire lenght of the list, we default to returning a sliced list with the size of list_to_slice
            sub_lists = [[x] for x in list_to_slice]
        else:
            sub_lists = [
                list_to_slice[i * (len(list_to_slice) // num_sub_lists):(i + 1) * (len(list_to_slice) // num_sub_lists)]
                for i in range(num_sub_lists)
            ]

        return sub_lists
    
    def _loop_requests(self, text_list, thread_id, logger, client,sleep_time=0.4):
        """
        helper to loop requests over each list of text. To avoid dealing with race conditions, we'll save each threads'
        results in separate files. We can simply append them after we're done
        """
        for text in text_list:
            attributes = self.toxicity_request(text, thread_id, client=client, logger=logger,sleep_time=sleep_time)
            
            if not os.path.exists("results"):
                os.mkdir("results")
            
            with open("results/thread-"+str(thread_id)+".jsonl", "a+") as f:
                attributes["comment_id"] = text[1]
                f.write(json.dumps(attributes)+"\n")

    def setup_logger(self, name, log_file, level=logging.INFO):
        """To setup as many loggers as you want"""

        handler = logging.FileHandler(log_file)        
        handler.setFormatter(self._formatter)

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)

        return logger

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=20000, stop_max_attempt_number=10)
    def toxicity_request(self, text, thread_id, client,logger,sleep_time=0.4, print_result=False):
        """
        text: text to be submitted to Perspective
        thread_id: thread id for debugging/logging purposes
        client: each thread must have a different client

        We add the retry decorator to decrease error rate
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
            if(print_result):
                print("Thread",thread_id,":",attributes["TOXICITY"], "text_id-correct:",text_id)
            return attributes
        except Exception as e:
            logger.error("text_id-"+str(text_id)+"-"+str(e))
            print("text_id:",text_id,e)
            raise IOError("text_id:",text_id,e)

    def threaded_requests(self):
        """
        performs threaded requests. Creates the specified amount of threads and speeds up the requests.

        """
        texts_list = []
        for index, row in self.df.iterrows():
            texts_list.append((row[self._text_field], row[self._text_id_field]))

        sliced_texts_list = self._slice_list(texts_list,self.n_threads)
        threads = []

        loop_range = min(self.n_threads, len(sliced_texts_list)) # this avoids errors when we have less data to process than number of threads
        
        for thread_count in range(loop_range):
            # one client for each thread. Otherwise we'll get errors
            client = self.threads_create_client()

            if not os.path.exists("results"):
                os.mkdir("results")
            logger = self.setup_logger('logger-thread-'+str(thread_count), 'results/thread-'+str(thread_count)+'-logfile.log')

            thread = threading.Thread(target=self._loop_requests, args=(sliced_texts_list[thread_count],thread_count,logger, client))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()