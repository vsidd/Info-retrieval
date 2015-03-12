'''
Created on Feb 23, 2015

@author: Siddarthan
'''
import os

class SidSearch:
    termIdMap = {}
    docIdMap = {}
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
                #print "l val : ",l
                if(len(l)>1):
                    self.docIdMap[l[0]] = l[2]

        
        
        
        
    def matchAll(self):
        docIdMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/docIdMap.txt')
        #outputFormat = {"hits" : {"hits" : [{"_id" : ""}]}}
        docIdMapAll = []
        
        with open(docIdMapFileLocation) as docIdMapFile:
            for line in docIdMapFile:
                l = line.split(" ")
                if(len(l)>1):
                    docIdMapAll.append({"_id" : l[0]})
        outputFormat = {"hits" : {"hits" : docIdMapAll}}
        return outputFormat
    
    def getTF(self, term):
        if(self.termIdMap.has_key(term)):
            termId = self.termIdMap[term]
        else:
            return {"hits" : { "total" : 0, "hits" : []}}
        #print "### term ",term
        #print "### ",termId
        docFreq = 0
        catalogFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/InvertedIndex/FinalCatalog.txt')
        offset = -1
        #print "docIDMAP : ",docIdMap
        with open(catalogFileLocation) as  catalogFile:
            for line in catalogFile:
                l = line.split(" : ")
                if(l[0].strip() == termId.strip()):
#                     print "?????"
                    offset = l[1]
#        print "offset : ",offset
        if offset == -1:
            return {"hits" : { "total" : 0, "hits" : []}}
         
        invertedIndexFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/InvertedIndex/FinalInvertedIndex.txt')
        invertedIndexFile = open(invertedIndexFileLocation, 'r');
        invertedIndexFile.seek(int(offset))
        record = invertedIndexFile.readline()
#        print record
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
#        print outputFormat
        return outputFormat
    
        
    def getDocID(self,id):
#         docIdMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/docIdMap.txt')
#         with open(docIdMapFileLocation) as docIdMapFile:
#             for line in docIdMapFile:
#                 l = line.split(" ")
#                 #print "l val : ",l
#                 if(len(l)>1):
#                     docIdMap[l[0]] = l[2]
        outputFormat = {"_source" : {"docno" : self.docIdMap[id]}}
        return outputFormat

#search = SidSearch()
#search.getTF("500")