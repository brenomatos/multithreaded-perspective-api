import pandas as pd
import os
import json


class ParseResults():
    def __init__(self, base_dataframe_path, text_field, text_id_field , results_path="results/"):
        """
        results_path: path to folder with the jsonl files
        base_dataframe: the ground truth dataframe, from which we generated the initial requests 
        """
        self.results_path = results_path
        self.base_dataframe_path = base_dataframe_path
        self.text_field = text_field
        self.text_id_field = text_id_field

        self.base_dataframe = pd.read_csv(self.base_dataframe_path,lineterminator="\n")
        print("Length of base dataframe:", len(self.base_dataframe))

    def concat_results(self):
        """
        Concatenates all results into a single dataframe. In this case, I opted
        to iterate with readlines because pd.read_json(,lines=True) returned errors
        """
        results = os.listdir(self.results_path)
        results = [x for x in results if x.endswith(".jsonl")]
        records = []

        for f in results:
            path = self.results_path+f
            with open(path, "r") as f:
                for line in f.readlines():
                    data = json.loads(line)
                    records.append(data)
        self.perspective_df = pd.DataFrame.from_records(records)
        print("Number of corretly processed entries:", len(self.perspective_df))

    def find_missing_ids(self):
        """
        Finds which ids need to be requested again
        returns the list of said ids
        """
        try:
            downloaded = list(self.perspective_df["comment_id"].values)
        except Exception as e:
            print(e," perspective_df has not been initialized yet! run concat_results beforehand")
        base_comparison = list(self.base_dataframe["comment_id"].values)

        missing_ids = set(base_comparison) - set(downloaded)
        return missing_ids

    def generate_retry_dataframe(self, missing_ids):
        """
        Given a list of ids that were not correctly computed, returns a new dataframe to be submitted
        to the API again
        """
        retry_df = self.base_dataframe[self.base_dataframe[self.text_id_field].isin(missing_ids)]
        return retry_df

pr = ParseResults("comments.csv","comment_text", "comment_id")
pr.concat_results()
missing_ids = pr.find_missing_ids()
print(len(missing_ids))
generate_retry_dataframe = pr.generate_retry_dataframe(missing_ids)
print(generate_retry_dataframe.head())