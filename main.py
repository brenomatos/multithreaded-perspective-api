from PerspectiveRequests import PerspectiveRequests

pp = PerspectiveRequests("/data/Downloads/comments.csv","comment_text", "comment_id" , "api_key", n_threads=10)
pp.threaded_requests()