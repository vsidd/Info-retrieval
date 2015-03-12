'''
Created on Feb 23, 2015

@author: Siddarthan
'''
import os

"""
This class acts as an interface to access the indexed documents. Users can use this interface to 
get details about the documents that is indexed. This was created to replace the functionalities
done by the ElasticSearch.
Features provided in this class are
1. matchAll() - To get all the documents
2. getTF(term) - To get the list of term frequency of the given term 
3. getDocID(id) - To return the original document ID for the given internal ID.
"""


class SidSearch:
    termIdMap = {}
    docIdMap = {}
    """
    This method initializes two maps. 
    1. termIdMap - This contains a string and a corresponding ID for that string 
                   which was created at the time of indexing
    2. docIdMap - This contains the original document ID and the corresponding ID 
                  for that document which was created at the time of indexing
    """
    def __init__(self):
        termIdMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/termIdMap.txt')
        with open(termIdMapFileLocation) as termIdMapFile:
            for line in termIdMapFile:
                l = line.split(" ")
                if(len(l) > 1):
                    self.termIdMap[l[0]] = l[1]
        
        docIdMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/docIdMap.txt')
        with open(docIdMapFileLocation) as docIdMapFile:
            for line in docIdMapFile:
                l = line.split(" ")
                if(len(l)>1):
                    self.docIdMap[l[0]] = l[2]

        
        
        
    """
    matchAll(): This method returns all the documents in the index
    """    
    def matchAll(self):
        docIdMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/docIdMap.txt')
        docIdMapAll = []
        
        with open(docIdMapFileLocation) as docIdMapFile:
            for line in docIdMapFile:
                l = line.split(" ")
                if(len(l)>1):
                    docIdMapAll.append({"_id" : l[0]})
        outputFormat = {"hits" : {"hits" : docIdMapAll}}
        return outputFormat
    
    """
    getTF(term): This method receives a term and returns a map of documents containing the term and the term frequency in that document
    """ 
    def getTF(self, term):
        if(self.termIdMap.has_key(term)):
            termId = self.termIdMap[term]
        else:
            return {"hits" : { "total" : 0, "hits" : []}}
        docFreq = 0
        catalogFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/InvertedIndex/FinalCatalog.txt')
        offset = -1
        with open(catalogFileLocation) as  catalogFile:
            for line in catalogFile:
                l = line.split(" : ")
                if(l[0].strip() == termId.strip()):
                    offset = l[1]
        if offset == -1:
            return {"hits" : { "total" : 0, "hits" : []}}
         
        invertedIndexFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/InvertedIndex/FinalInvertedIndex.txt')
        invertedIndexFile = open(invertedIndexFileLocation, 'r');
        invertedIndexFile.seek(int(offset))
        record = invertedIndexFile.readline()
        docs = record
        docsList = docs.split("#")
        hitsResult = []
        for dblock in docsList:
            dblockValues = dblock.split(" ")      #docid, tf, ttf, positions
            if(len(dblockValues) > 1):
                docFreq += 1
                docNo = self.docIdMap[dblockValues[0]]
                hitsResult.append({"_id" : dblockValues[0], "_score" : int(dblockValues[1]), "fields" : {"docno" : [docNo]}, "positions" : dblockValues[2:]})
        outputFormat = {"hits" : { "total" : docFreq, "hits" : hitsResult}}
        return outputFormat
    
    """
    getDocID(id): This method receives internal ID format and returns the original document ID.
    """    
    def getDocID(self,id):
        outputFormat = {"_source" : {"docno" : self.docIdMap[id]}}
        return outputFormat

