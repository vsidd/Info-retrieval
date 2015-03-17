'''
Created on Jan 28, 2015

@author: Siddarthan

This class evaluates the various retrieval models like
Okapi-TF, Okapi BM25, TF-IDF, Unigram LM with Laplace Smoothing
and Unigram LM with Jelinek-Mercer smoothing. 
Initially this class worked with ElasticSearch tool to index documents 
which are then commented out and uses my own interface to index and 
read the required measurements
'''
import glob
import os
from os import walk
import math
from genericpath import isfile
from collections import Counter
from elasticsearch import Elasticsearch
import re
from PorterStemmer import *
import operator
import ast
from HW1.sidSearch import SidSearch


#counter = 0
def main():
    es = Elasticsearch()
#    createIndex(es)
    search = SidSearch()
    #print search.matchAll()
    #indexedDocuments = es.search(index='ap_dataset',body={"query": {"match_all": {}}}, size=84679)
    indexedDocuments = search.matchAll()
    docIDs = [doc['_id'] for doc in indexedDocuments['hits']['hits']]
    print "here"
    #print "docIDS : ",docIDs
    #docFreqAndAvg = avgDocLen(es , docIDs)
    avg = 247.806315615
    totalDocuments = 84678.0
    #avg = docFreqAndAvg[0]
    #docsLen = docFreqAndAvg[1]
    docLenFile = os.path.join(os.path.dirname(__file__), '/College/temp/HW1-IR/HW1/docLength.txt')
    q = open(docLenFile, "r")
    docsLen = ast.literal_eval( q.readline())
    q.close()
    #sumVal = sum(docsLen.values())
    #avg = sumVal/84679.0
    #print "docsLen : ",docsLen
    #print "avg : ",avg
    
    filename = os.path.join(os.path.dirname(__file__), '/College/temp/HW1-IR/resources/AP_DATA/query_desc.51-100.short.txt')
    f = open(filename, "r")
    q = open(filename, "r")
    queryNo = []
    for line in iter(q):
        if(len(line)>2):
            if(line[2] == "."):
                queryNo.append(line[:2])
            else:
                queryNo.append(line[:3])
    q.close()
    queryCounter = -1
          
          
    #es.put_script(lang='groovy', id='getTF', body={"script": "_index[field][term].tf()"})
            
    for currentLine in iter(f):
        queryCounter = queryCounter+1
        okapi = {}
        tfIDF = {}
        okapiBM25 = {}
        lm_laplace = {}
        jm_smoothing = {}
        
        if(len(currentLine)>2):
            print currentLine
            lm_laplace_tracker = {}
            jelinek_mercer_tracker = {}
            jm_tracker = 1
            laplace_tracker = 1
            processedText = processText(currentLine)
            print processedText
            proximitySearch(search, processedText, queryCounter, queryNo)
            for term in processedText.split():
                #scriptResult = es.search(index='ap_dataset',body={"query": {"function_score": {"query": {"match": {"text": term}},"functions": [{"script_score": {"script_id": "getTF","lang" : "groovy","params": {"term": term,"field": "text"}}}],"boost_mode": "replace"}},"size": totalDocuments,"fields": ["docno"]})
                scriptResult = search.getTF(term)
                df = scriptResult['hits']['total']
                processedScriptResult = processScriptResult(scriptResult)
                #print "script result : ",processedScriptResult
                for x in range(len(processedScriptResult)):
                    if(lm_laplace_tracker.has_key(processedScriptResult[x][0]) == False):
                        lm_laplace_tracker[processedScriptResult[x][0]] = 1
                        jelinek_mercer_tracker[processedScriptResult[x][0]] = 1
                resultTDF = 0
                resultTTF = 0
                for x in range(len(processedScriptResult)):
                    resultTDF = resultTDF + docsLen[processedScriptResult[x][0]]
                    resultTTF = resultTTF + processedScriptResult[x][1]
                       
                laplace_tracker = laplace_tracker + 1
                jm_tracker = jm_tracker + 1
                
                for i in range(len(processedScriptResult)):    
                    docID = processedScriptResult[i][0]
                    tf = processedScriptResult[i][1]
                    tfwq = processedText.count(term)
                    okapiResult =okapiTF(tf, docsLen[docID], avg) 
                    okapiBM25Result = okapiBM25Cal(totalDocuments, df, tf, docsLen[docID], avg, tfwq)
                    p_LaplaceResult = p_LaplaceCalc(tf, docsLen[docID])
                    jelinek_mercer = jelinek_mercerCalc(tf, docsLen[docID], resultTTF, resultTDF)
                    
                    jelinek_mercer_tracker[docID] = jelinek_mercer_tracker[docID] + 1
                    lm_laplace_tracker[docID] = lm_laplace_tracker[docID] + 1
                    
                    if(jm_smoothing.has_key(docID)):
                        jm_smoothing[docID] = jm_smoothing[docID] * jelinek_mercer
                    else:
                        jm_smoothing[docID] = jelinek_mercer
                    
                    if(lm_laplace.has_key(docID)):
                        lm_laplace[docID] = lm_laplace[docID] * p_LaplaceResult
                    else:
                        lm_laplace[docID] = p_LaplaceResult
                    
                    if(okapiBM25.has_key(docID)):
                        okapiBM25[docID] = okapiBM25[docID] + okapiBM25Result
                    else:
                        okapiBM25[docID] = okapiBM25Result
                        
                    if(okapi.has_key(docID)):
                        okapi[docID] = okapi[docID] + okapiResult
                        tfIDF[docID] = tfIDF[docID] + (okapiResult * math.log(totalDocuments/(df*1.0)))
                    else:
                        okapi[docID] = okapiResult
                        tfIDF[docID] = (okapiResult * math.log(totalDocuments/(df*1.0)))
                        
                for y in lm_laplace_tracker:
                    if(lm_laplace_tracker[y] != laplace_tracker):
                        while(lm_laplace_tracker[y] != laplace_tracker):
                            lm_laplace[y] = lm_laplace[y] * p_LaplaceCalc(0, docsLen[y])
                            lm_laplace_tracker[y] = lm_laplace_tracker[y] + 1
                            if(processedScriptResult != []):
                                jm_smoothing[y] = jm_smoothing[y] * jelinek_mercerCalc(0, docsLen[y],resultTTF,resultTDF)
                                jelinek_mercer_tracker[y] = jelinek_mercer_tracker[y] + 1
                
        sorted_Okapi = sorted(okapi.items(), key=operator.itemgetter(1), reverse = True)[:1000]
        sorted_tfIDF = sorted(tfIDF.items(), key=operator.itemgetter(1), reverse = True)[:1000]
        sorted_OkapiBM25 = sorted(okapiBM25.items(), key=operator.itemgetter(1), reverse = True)[:1000]
        sorted_Lm_Laplace = sorted(lm_laplace.items(), key=operator.itemgetter(1), reverse = True)[:1000]
        sorted_Jm_Smoothing = sorted(jm_smoothing.items(), key=operator.itemgetter(1), reverse = True)[:1000]
        
                     
#         writeOutputToFile(es, '1_Okapi_Result.txt', sorted_Okapi, queryCounter, queryNo)
#         writeOutputToFile(es, '2_TFIDF_Result.txt', sorted_tfIDF, queryCounter, queryNo)
#         writeOutputToFile(es, '3_OkapiBM25_Result.txt', sorted_OkapiBM25, queryCounter, queryNo)
#         writeOutputToFile(es, '4_LM_Laplace_Result.txt', sorted_Lm_Laplace, queryCounter, queryNo)
#         writeOutputToFile(es, '5_JM_Smoothing_Result.txt', sorted_Jm_Smoothing, queryCounter, queryNo)
#         
        writeOutputToFile(search, '1_Okapi_Result.txt', sorted_Okapi, queryCounter, queryNo)
        writeOutputToFile(search, '2_TFIDF_Result.txt', sorted_tfIDF, queryCounter, queryNo)
        writeOutputToFile(search, '3_OkapiBM25_Result.txt', sorted_OkapiBM25, queryCounter, queryNo)
        writeOutputToFile(search, '4_LM_Laplace_Result.txt', sorted_Lm_Laplace, queryCounter, queryNo)
        writeOutputToFile(search, '5_JM_Smoothing_Result.txt', sorted_Jm_Smoothing, queryCounter, queryNo)
                 

"""
Returns a list of tuples that contains (document_ID, term_frequrency, document_number)
"""
def processScriptResult(searchResult):
    docIDs = [(doc['_id'], doc['_score'], doc['fields']['docno'])for doc in searchResult['hits']['hits']]
    return docIDs

"""
Returns a list of tuples that contain (document_id, term, term_positions)
"""
def getPositions(term, searchResult):
    docIDs = [(doc['_id'],term, doc['positions'])for doc in searchResult['hits']['hits']]
    return docIDs

"""
Returns a list of tuples that contains (document_id, document_frequency, document_number)
"""
def processDFScriptResult(searchResult):
    docIDs = [(doc['_id'], doc['_score'], doc['fields']['docno'])for doc in searchResult['hits']['hits']]
    return docIDs

"""
Returns a calculated value of TF-IDF from the given paramenters
"""
def tfidfCalculate(okapiResult, totalDocs, docFreq, term, id):
    if(docFreq != []):
        tfidf = okapiResult[2] * math.log(totalDocs/docFreq)
        return (id, term, tfidf)
    else:
        return (-1, term, 0)

"""
Returns a calculated value of Okapi-BM25 from the given paramenters
"""
def okapiBM25Cal(D, df, tf, docLen, avg, tfwq):
    k1 = 1.2
    k2 = 100.0
    b = 0.75
    t1 = math.log((D+0.5)/(df+0.5))
    t2 = (tf+ (k1*tf*1.0))/(tf+ (k1*((1-b) + (b*(docLen/(avg * 1.0))))*1.0))
    t3 = (tfwq + (k2 * tfwq*1.0))/((tfwq+k2)*1.0)
    bm25 = t1 * t2 * t3
    return bm25

"""
Returns a calculated value of Unigram LM with Laplace smoothing from the given paramenters
"""
def p_LaplaceCalc(tf, docLen):
    vocabSize = 178050
    p_Laplace = (tf+1.0)/((docLen+vocabSize)*1.0)
    return p_Laplace

"""
Returns a calculated value of Unigram LM with Jelinek_Mercer smoothing from the given paramenters
"""
def jelinek_mercerCalc(tf, docLen, resultTTF, resultTDF):
    smoother = 0.7
    t1 = smoother * (tf/docLen*1.0)
    t2 = (1-smoother) * (resultTTF/resultTDF*1.0)
    jelinek_mercer = t1 + t2
    return jelinek_mercer

"""
Returns a calculated value of Proximity search from the given paramenters
"""
def proximitySearch(search, processedText, queryCounter, queryNo):
    docLenMap = {}
    docLenMapFileLocation = os.path.join(os.path.dirname(__file__), '/College/temp/HW2-IR/src/OtherFiles/docLenMap.txt')
    with open(docLenMapFileLocation) as docLenMapFile:
        for line in docLenMapFile:
            l = line.split(" ")
            if(len(l) > 1):
                docLenMap[l[0]] = l[1]
                    
    listOfPositions = []
    for term in processedText.split():
        positions = search.getTF(term)
        #print"positions : ",getPositions(positions)
        tdp = getPositions(term,positions)
        if (len(tdp) == 0):
            return 
        listOfPositions += tdp
    listOfPositions.sort()
    docTermMap = {}
    docIds = []
    for block in listOfPositions:
        if not block[0] in docTermMap:
            docIds.append(block[0])
            docTermMap[block[0]] = [(block[1],block[2])]
        else:
            docTermMap[block[0]].append((block[1],block[2]))
    #print len(docTermMap['1'])
    proximityScore={}
    for id in docIds:
        numberOfTerms = len(docTermMap[id])
#         if(numberOfTerms == len(processedText.split())):
        #print "ID : ",docTermMap[id]
        termPointers = [0] * numberOfTerms
        range = calculateRange(docTermMap[id], termPointers)
        score = ((3000 - range) * numberOfTerms *1.0)/(1.0* int(docLenMap[id]) + 160470)
        #score = ((range - numberOfTerms)*1.0)/(numberOfTerms * 1.0)
        proximityScore[id] = score
    sorted_Proximity = sorted(proximityScore.items(), key=operator.itemgetter(1), reverse = True)[:1000]
    writeOutputToFile(search, '6_ProximitySearch_Result.txt', sorted_Proximity, queryCounter, queryNo)
    #print processedText, "DDDDDDDDOOOOOOOOOOOONNNNNNNNNNNEEEEEEEEEEE"   

def calculateRange(termPositions, termPointers):
    positions =[]
    for i in range(len(termPointers)):
        val = termPositions[i][1]
        intPositions = map(int, val)
        positions.append(intPositions)
        termPointers[i] = -len(val)
    flag = True    
    prev = 100000
    #print "termPointers ",termPointers
    while(flag):
        flag = False
        numbers = []
        for i in range(len(termPointers)):
            numbers.append(positions[i][termPointers[i]])
        #print numbers
        rang = max(numbers) - min(numbers)
        if rang < prev:
            prev = rang
        
        for i in range(len(termPointers)):
            index = numbers.index(min(numbers))
            if termPointers[index] != -1:
                termPointers[index] += 1
                flag = True
                break
    return prev

"""
Writes the result to a corresponding output file
"""
def writeOutputToFile(es, fileName, results, queryIndex, queryNumber):
    f = open(fileName, 'a')
    rank = 1
    for i in range(len(results)):
        #documentNumbers = es.get(index='ap_dataset', id=str(results[i][0]), doc_type='document')
        #print "@@@@@@@@@@",documentNumbers
        documentNumbers = es.getDocID(results[i][0]) 
        outputToWrite = str(queryNumber[queryIndex])+" "+"Q0 "+str(documentNumbers['_source']['docno'])+" "+str(rank)+" "+str(results[i][1])+" Exp\n"
        f.write(outputToWrite)
        rank += 1
    f.close()
    
"""
The query is processed to remove stop words and punctuations and also stem the query
"""
def processText(currentLine):
    currentLine = re.sub('[,"\']','',currentLine)
    currentLine = currentLine.replace("-"," ")
    currentLine = currentLine[currentLine.index("."):]
    currentLine = currentLine[4:]
    currentLine = removeWords(3, currentLine)
    currentLine = removeStopWords(currentLine)
    if(currentLine.rfind('.') != -1):
        currentLine = currentLine[:currentLine.rfind('.')]
    currentLine = stemQuery(currentLine)
    currentLine = currentLine.strip()
    currentLine = currentLine.lower()
    return currentLine

"""
Stem the input query from given text
"""
def stemQuery(currentLine):
    temp = ""
    res = PorterStemmer()
    for word in currentLine.split():
        temp = temp +" "+ res.stem(word, 0 , (len(word)-1))
    return temp

"""
Remove the stop words from given text
"""
def removeStopWords(currentLine):
    filename = os.path.join(os.path.dirname(__file__), '/College/IR/elasticsearch-1.4.2/config/stoplist.txt')
    f = open(filename, "r")
    for i in iter(f):
        i = i.strip()
        i = " "+i+" "
        currentLine = re.sub(i,' ',currentLine)
    return currentLine

def removeWords(num, currentLine):
    for i in range(num):
        index = currentLine.index(" ")
        currentLine = currentLine[index+1:]
    if(currentLine[:2]=="a "):
        currentLine = currentLine[2:]
    elif(currentLine[:3]=="an "):
        currentLine = currentLine[3:]
    elif(currentLine[:3]=="or "):
        currentLine = currentLine[3:]
    elif(currentLine[:3]=="on "):
        currentLine = currentLine[3:]
    elif(currentLine[:4]=="the "):
        currentLine = currentLine[4:]
    elif(currentLine[:4]=="how "):
        currentLine = currentLine[4:]
    return currentLine


def okapiTF(tf, docLen,avg):
        temp = 0.0
        tf = tf * 1.0
        docLen = docLen * 1.0
        avg = avg * 1.0
        temp = tf/(tf + 0.5 + (1.5 * (docLen/avg)))
        return temp
    
def findDocID(tfwd, word):
    tf_DocID = []
    for i in range(len(tfwd)):
        if(word.lower() == tfwd[i][1].lower()):
            tf = tfwd[i][2]
            docID = tfwd[i][0]
            tf_DocID.append((tf, docID))
            return tf_DocID
           
def findDocLen(docLength, docID):
    docLen = 0
    for i in range(len(docLength)):
        if(docID == docLength[i][0]):
            docLen = docLength[i][1]
            return docLen
"""
getBuilders : returns the DOCNO and TEXT in json format from the given file
"""
def getBuilders(file):
    docID = ""
    text = ""
    contentBuilder= [];
    textFlag = False;
    f = open(file, "r")
    for currentLine in iter(f):
        currentLine = currentLine.strip()
        if("<DOCNO>" in currentLine):
            docID = currentLine[(currentLine.find(">")+1) : currentLine.find("</DOCNO>")]
        if("</TEXT>" in currentLine):
            textFlag = False;
        if(textFlag):
            text = text+" "+ currentLine
        if("<TEXT>" in currentLine):
            textFlag = True;
        if("</DOC>" in currentLine):
            docID = docID.strip()
            contentBuilder.append((docID,text));
            text = ""
            docID = ""
    return contentBuilder


"""
createIndex : creates and fills the index with the data from directory
"""
def createIndex(es):
    filename = os.path.join(os.path.dirname(__file__), '/College/temp/HW1-IR/resources/AP_DATA/ap89_collection/')
    builders = []
    for (dirpath, dirnames, filenames) in walk(filename): 
        for file in filenames:
            builders.append(getBuilders('C:/College/temp/HW1-IR/resources/AP_DATA/ap89_collection/'+file))
    counter = 1
    for builder in builders:
        for doc in builder:
            print counter
            es.index(index = 'ap_dataset', doc_type='document', body={'docno':doc[0],'text':doc[1]}, id=counter)
            counter = counter+1
    
    
"""
avgDocLen : Average length of all documents in index
"""
def avgDocLen(es , docs):
    i = 0
    j = 0
    count = 0
    docFreqs = {}
    dFs = open('Document_Lengths.txt','w+')
    while(j<84679):
        i = j
        if(j+500<84679):
            j = j + 500
        else:
            j = 84679+1
        mtv = es.mtermvectors(index='ap_dataset', doc_type='document',ids=docs[i:j])
        print "i,j",i,"...",j
        for doc in mtv['docs']:
            docFreq = 0
            if(doc['term_vectors']!= {}):
                for term in doc['term_vectors']['text']['terms']:
                    docFreq += doc['term_vectors']['text']['terms'][term]['term_freq']
                docId = doc['_id']
                docFreqs[docId] =docFreq
                dFs.write(str(docId)+" "+str(docFreq)+"\n")
            else:
                docFreqs[doc['_id']]=0
    temp = 0.0
    temp = sum(docFreqs.values())
    avg = temp/(len(docFreqs) * 1.0)
    dFs.close()
    print "avg : ",avg
    print "total terms : ",temp
    print "docFreqs : ",docFreqs
    print "len(docFreqs) : ",len(docFreqs)
    return (avg, docFreqs)
        
"""
getStatistics: Returns search result statistics
"""
def getStatistics(es , ID):
    stats = es.mtermvectors(index='ap_dataset', doc_type='document',body={"ids":[""+ID], "parameters":{"fields":["text"],"term_statistics": True}})
    return stats


"""
docsLength : Returns size of each document in the search result
"""
def docsLength(es , docs, id):
    docFreqs = []
    docFreq = 0
    if(docs['docs'][0]['term_vectors']!= {}):
        for term in docs['docs'][0]['term_vectors']['text']['terms']:
            docFreq += docs['docs'][0]['term_vectors']['text']['terms'][term]['term_freq']
        docFreqs.append((id, docFreq))
    else:
        docFreqs.append((id,0))
    return docFreqs
    

"""
termFrequency: Returns the frequency of the given word in each document(docs)
"""
def termFrequency(es, docs, tfwd, word, id):
    if(docs['docs'][0]['term_vectors']!= {}):
        if word in docs['docs'][0]['term_vectors']['text']['terms']:
            tfwd.append((id,word, docs['docs'][0]['term_vectors']['text']['terms'][word]['term_freq']))
    return tfwd

def docFrequency(es, docs, df, word, id):
    if(docs['docs'][0]['term_vectors']!= {}):
        if word in docs['docs'][0]['term_vectors']['text']['terms']:
            df.append((id,word, docs['docs'][0]['term_vectors']['text']['terms'][word]['doc_freq']))
    return df


if __name__ == "__main__":
    main()
                
                