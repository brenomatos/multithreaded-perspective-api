from PerspectiveRequests import PerspectiveRequests
from ParseResults import ParseResults

N_THREADS = 10
# run this for the initial requests


def inital_requests(dataframe_path, text_field, text_id_field, api_key_path, n_threads=10):
    """
    Helper to run requests.
    """
    pp = PerspectiveRequests(dataframe_path, text_field, text_id_field, api_key_path, n_threads=N_THREADS)
    pp.threaded_requests()


def retry_missing_cases(base_dataframe_path, text_field, text_id_field, results_path="results/"):
    """
    Helper to re-run code for missing cases. The number of missing cases will depend on the wait parameters
    used during the initial requests
    """
    # now, we check if we have any missing ids:
    pr = ParseResults(base_dataframe_path, text_field, text_id_field, results_path="results/")
    pr.concat_results()
    missing_ids = pr.find_missing_ids()
    print("Number of missing Ids:", len(missing_ids))
    if(len(missing_ids)):
        generate_retry_dataframe = pr.generate_retry_dataframe(missing_ids)
        generate_retry_dataframe.to_csv("retry_dataframe.csv",index=False)
        missing_pp = PerspectiveRequests("retry_dataframe.csv","comment_text","comment_id","api_key", n_threads=N_THREADS)
        # note that we don't need to specifiy a new file to save. It will just append to the .jsonl files inside the results folder
        missing_pp.threaded_requests()
    else:
        print("No more instances to run!")


def main(dataframe_path, text_field, text_id_field, api_key_path, inital_requests_bool=True):
    """
    Enable the parameter to make initial requests and disable it if you just want to correct missing instances
    """
    if(inital_requests_bool):
        inital_requests(dataframe_path, text_field, text_id_field, api_key_path, n_threads=N_THREADS)
    retry_missing_cases(dataframe_path, text_field, text_id_field)

    #concatenating results and making a final dataframe
    print("Making results dataframe")
    pr = ParseResults("comments.csv","comment_text", "comment_id", results_path="results/")
    pr.concat_results()


if(__name__ == "__main__"):
    main("/data/Downloads/comments.csv","comment_text", "comment_id" , "api_key", inital_requests_bool=True)
