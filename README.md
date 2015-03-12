# Info-retrieval
Implemented and compared retrieval systems using the following vector space models and language models:
* OkapiTF
* TF-IDF
* Okapi BM25
* Unigram LM with Laplace Smoothing
* Unigram LM with Jelinek-Mercer smoothing

This project uses elasticsearch, a commercial-grade indexer.  

Models class does the following:
* Parse the corpus and index it with elasticsearch
* Create a query processor, which runs queries from an input file against all retrieval models

Index class does the following:
* Created an index out of 85000 documents without any external tools
* Provided an interface to access the indexed documents to get values like term frequency, document frequency etc. This interface will act as replacement for the previously used ElasticSearch calls.

Models are evaluated in Python. 
Index is created using Java as file operations will be more faster than Python. This class will be able to handle large number of documents and terms without using excessive memory or disk I/O.
