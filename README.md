# Multithreaded requests to Perspective API


## Introduction
It is common to ask the Perspective team for higher quotas when developing large projects. You can do so following this link: [Request Quota Increase](https://developers.perspectiveapi.com/s/request-quota-increase?language=en_US)

The idea is to leverage higher quotas to speed up processing by using threads: We divide the entire workload (i.e., the strings that must be submitted to Perspective) into multiple threads. Then, to avoid dealing with race conditions, we save each thread's results in separate files. We also add methods in our modules to concatenate results from the multiple files generated.

Additionally, we also add retrying decorators for the requesting method. This helps to retry requests that failed for any reason (usually, Error 429: Too Many Requests)

## Modules

This module works on generic text dataframes; all you need is a dataframe with two fields:
- text_field: the text to be sent to Perspective
- text_field_id: unique id that identifies each text entry

These fields must be specified as parameters when using one of our two modules:
- PerspectiveRequests: standard code to run multithreaded requests. Main class has the following parameters: 
    - dataframe_path: path to dataframe containing text_field and text_field_id
    - text_field: column name with text to be sumitted to the API
    - text_id_field: column name with unique Id for each dataframe instance
    - api_key_path: a file containing you API key (and nothing else)
    - n_threads: number of threads to be used
- ParseResults: parsing options for results, including generating a dataframe of cases that returned erros, which can then be used to perform additional rounds of requests. Main class has similar parameters, with the addition of:
    - results_path: path to a folder containing the results. Defaults to 'results/'

Both modules have documented methods and should be easy to use. We also added a ```main.py``` script with an example on how to run the pipeline

## Installation

We also added a requirements.txt file for easier installation:

```
pip install -r requirements.txt

```

## Results

By default, our modules save results and logs in a dinamically created folder '/results', but you can specify the path to another folder. Just be mindful that, in case you need to re-run missing instances (e.g., cases that returned errors), you must specifiy the same results folder path. If you run modules in the default setting, you should not have any problems with this.

Note:
The user should be aware of the ```@retry``` decorator. We've left the parameters as values that worked best for us, but you should adjust them if needed.