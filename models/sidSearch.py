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

"""
Contents in terIdMap.txt file will be like

celluloid 1
torch 2
pass 3
new 4
gener 5
filmmak 6

"""

"""
Contents in docIdMap.txt file will be like

1  AP890101-0001 
2  AP890101-0002 
3  AP890101-0003 
4  AP890101-0004 
5  AP890101-0005 
6  AP890101-0006 

"""

"""
Contents in FinalCatalog.txt will be like
1 : 0
2 : 136
3 : 2885
4 : 84526
5 : 837938
6 : 1083645
"""

"""
Contents in FinalInvertedIndex.txt will be like
1 1 1#5976 1 90#8058 1 60#18030 1 12#28169 1 18#54971 1 19#54973 1 148#55528 1 148#57454 1 19#67641 1 78#72561 1 12#81786 4 1 18 29 90#
1 1 2#416 1 224#697 1 234#1322 1 150#1626 1 268#1650 1 139#1676 1 31#1761 1 367#1795 1 86#2157 
1 1 3#8 1 211#11 1 195#17 1 132#21 1 45#56 1 139#78 1 390#82 1 552#103 1 69#137 1 48#157 1 174#
1 4 5 63 480 519#14 1 294#18 1 108#27 1 336#32 1 14#40 1 138#62 2 32 192#78 1 419#
1 2 6 29#24 1 207#1027 1 104#1461 2 79 138#1617 1 51#1743 1 1#2130 1 116#3057 1 107# 
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
                l = line.split(" ")                     #will be in the format "term termId"
                if(len(l) > 1):
                    self.termIdMap[l[0]] = l[1]
        
        docIdMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/docIdMap.txt')
        with open(docIdMapFileLocation) as docIdMapFile:
            for line in docIdMapFile:
                l = line.split(" ")                     #will be in the format "internalDocID originalDocID"
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
                l = line.split(" : ")                               #will be in the format "termID offsetPositionAtIndex"
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
            dblockValues = dblock.split(" ")      #will be in the format "docID tf positions"
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

