# Info-retrieval
Implemented and compared retrieval systems using the following vector space models and language models:
* OkapiTF
* TF-IDF
* Okapi BM25
* Unigram LM with Laplace Smoothing
* Unigram LM with Jelinek-Mercer smoothing
* Proximity Search
About the proximity search:
The "Finding Blurbs" algorithm is used to find the proximity of the query terms in the given documents and the resulting score is used as a criteria in finding and ranking the relavancy of documents in the corpus. 

This project uses elasticsearch, a commercial-grade indexer for indexing the courpus. Later the elasticsearch calls are replaced by my own interface that does the job of elasticsearch ie. indexing, calculting TF, DF, TTF, Vocab_Size etc...  

Models class does the following:
* Parses the entire corpus and index it with elasticsearch
* Creates a query processor, which runs queries from an input file against all retrieval models

Index class does the following:
* Creates an index out of 85000 documents without any external tools (like ElasticSearch)

SidSearch class does the following:
* Provides an interface to access the indexed documents to get values like term frequency, document frequency etc. This interface will act as replacement for the previously used ElasticSearch calls in the Models class thereby evaluating the retrieval models using my own index.

Models class is written in Python. 
Index class is written in Java as file operations will be more faster than Python. This class will be able to handle large number of documents and terms without using excessive memory or disk I/O operations thereby making it system memory independent.
