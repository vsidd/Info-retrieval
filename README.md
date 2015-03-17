# Info-retrieval
Implemented and compared retrieval systems using the following vector space models and language models:
* OkapiTF
* TF-IDF
* Okapi BM25
* Unigram LM with Laplace Smoothing
* Unigram LM with Jelinek-Mercer smoothing

This project uses elasticsearch, a commercial-grade indexer.  

Models class does the following:
* Parses the entire corpus and index it with elasticsearch
* Creates a query processor, which runs queries from an input file against all retrieval models

Index class does the following:
* Creates an index out of 85000 documents without any external tools (like ElasticSearch)

SidSearch class does the following:
* Provides an interface to access the indexed documents to get values like term frequency, document frequency etc. This interface will act as replacement for the previously used ElasticSearch calls in the Models class thereby evaluating the retrieval models using my own index.

Models class is written in Python. 
Index is created in Java as file operations will be more faster than Python. This class will be able to handle large number of documents and terms without using excessive memory or disk I/O operations thereby making it system memory independent.
