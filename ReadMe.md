
# Code Submission: *Semantic Hypothesis Re-ranking for Natural Language-to-Query Improvement*

This is the code to produce the results of ICDE 2023 submission 31: *"Semantic Hypothesis Re-ranking for Natural Language-to-Query Improvement"*

This code was tested on *Ubuntu 20.04 Focal* and with *Python 3.8*

## Setup

* (Optional) Setup and activate virtual environment:
  ```shell
  python -m venv venv
  source venv/bin/activate
  ```

* Install requirements:
  ```shell
   pip install -r requirements.txt
  ```

* Download nltk tokenizers: 
  ```shell
  ./download_tokenizers.sh
  ```

* Download and setup Nubia (Semantic Similarity). This will install more dependencies via *pip*.
  ```shell
  ./setup_nubia.sh
  ```

## Run Experiments

Each step below will read an input files and write output files to the *./outs/* folder.
The files named *./outs/{dataset}/{system}/raw_output.txt* contain the raw hypotheses as provided
by the different NLIDB systems.


* Run back-translation. This generates back-translation for each SQL/OT query hypothesis using OT3 (called NL_out in the paper):
  ```shell
  python full_backtranslation.py
  ```
  
* Run Semantic Similarity. Computes semantic similarity scores between NL_in and NL_out using Nubia.
  This will take around 24h. It will start out by downloading the nubia models:
  ```shell
  python full_semantic_similarity.py
  ```
  
* Evaluate different re-ranking strategies. Produces main results:
  ```shell
  python full_compute_scores.py
  ```
