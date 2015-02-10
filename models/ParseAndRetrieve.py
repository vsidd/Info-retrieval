'''
Created on Jan 28, 2015

@author: Siddarthan
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


#counter = 0
def main():
    es = Elasticsearch()
#    createIndex(es)
    #avg = 238.354857757
    indexedDocuments = es.search(index='ap_dataset',body={"query": {"match_all": {}}}, size=84679)
    #print "indexedDoc ",indexedDocuments['hits']['hits'][0]
    docIDs = [doc['_id'] for doc in indexedDocuments['hits']['hits']]
    # (avg,dictOf{id : docLen})
    #docFreqAndAvg = avgDocLen(es , docIDs)
    avg = 247.806315615
    #avg = 247.811
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
    #print "query NO ",queryNo
    queryCounter = -1
          
          
    es.put_script(lang='groovy', id='getTF', body={"script": "_index[field][term].tf()"})
#     es.put_script(lang='groovy', id='getDF', body={"script": "_index[field][term].df()"})
            
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
            #print "1"
            print processedText
            for term in processedText.split():
                #term = res.stem(term, 0 , (len(term)-1))
                #term = res.stem(term, 0 , (len(term)-1))
                scriptResult = es.search(index='ap_dataset',body={"query": {"function_score": {"query": {"match": {"text": term}},"functions": [{"script_score": {"script_id": "getTF","lang" : "groovy","params": {"term": term,"field": "text"}}}],"boost_mode": "replace"}},"size": 84769,"fields": ["docno"]})
                df = scriptResult['hits']['total']
                #print "scriptResult: ",scriptResult
                processedScriptResult = processScriptResult(scriptResult)
                #print "processedScriptResult: ",processedScriptResult
                for x in range(len(processedScriptResult)):
                    if(lm_laplace_tracker.has_key(processedScriptResult[x][0]) == False):
                        lm_laplace_tracker[processedScriptResult[x][0]] = 1
                        jelinek_mercer_tracker[processedScriptResult[x][0]] = 1
                resultTDF = 0
                resultTTF = 0
                for x in range(len(processedScriptResult)):
                    resultTDF = resultTDF + docsLen[processedScriptResult[x][0]]
                    resultTTF = resultTTF + processedScriptResult[x][1]
                       
                #processedDFScriptResult = processDFScriptResult(scriptResultDF)
                #in the form (id, tf, docno)
                laplace_tracker = laplace_tracker + 1
                jm_tracker = jm_tracker + 1
                
                for i in range(len(processedScriptResult)):    
                    #print "reached here : "
                    docID = processedScriptResult[i][0]
                    tf = processedScriptResult[i][1]
                    tfwq = processedText.count(term)
                    #return (id, word, okapiVal)
                    okapiResult =okapiTF(tf, docsLen[docID], avg) 
                    okapiBM25Result = okapiBM25Cal(84679.0, df, tf, docsLen[docID], avg, tfwq)
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
                        tfIDF[docID] = tfIDF[docID] + (okapiResult * math.log(84679.0/(df*1.0)))
                    else:
                        okapi[docID] = okapiResult
                        tfIDF[docID] = (okapiResult * math.log(84679.0/(df*1.0)))
                        
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
        
                     
#        for id in range(len(sorted_tfIDF)):
#            if(sorted_tfIDF[id][1] > 0):
#                    tfIDFResultSet.append(sorted_tfIDF[id])
                
#        for id in range(len(sorted_Okapi)):
#            if(sorted_Okapi[id][1] > 0):
#                    okapiResultSet.append(sorted_Okapi[id])

        #print "Okapi Result set: ",okapiResultSet
        #print "TFIDF Result set: ",tfIDFResultSet
#        createOutput(es, okapiResultSet,tfIDFResultSet, queryCounter, queryNo)
        createOutput(es, sorted_Okapi,sorted_tfIDF,sorted_OkapiBM25,sorted_Lm_Laplace,sorted_Jm_Smoothing, queryCounter, queryNo)
#         createTfIDFOutput(es, tfIDFResultSet, queryCounter, queryNo)
                 

def processScriptResult(searchResult):
    docIDs = [(doc['_id'], doc['_score'], doc['fields']['docno'])for doc in searchResult['hits']['hits']]
    return docIDs

def processDFScriptResult(searchResult):
    docIDs = [(doc['_id'], doc['_score'], doc['fields']['docno'])for doc in searchResult['hits']['hits']]
    return docIDs
         
def tfidfCalculate(okapiResult, totalDocs, docFreq, term, id):
    if(docFreq != []):
        tfidf = okapiResult[2] * math.log(totalDocs/docFreq)
        return (id, term, tfidf)
    else:
        return (-1, term, 0)

def okapiBM25Cal(D, df, tf, docLen, avg, tfwq):
    bm25 = 0.0
    k1 = 1.2
    k2 = 100.0
    b = 0.75
    D = D * 1.0
    df = df * 1.0
    tf = tf * 1.0
    docLen = docLen * 1.0
    avg = avg * 1.0
    tfwq = tfwq * 1.0
    t1 = math.log((D+0.5)/(df+0.5))
    t2 = (tf+ (k1*tf))/(tf+ (k1*((1-b) + (b*(docLen/(avg * 1.0))))))
    t3 = (tfwq + (k2 * tfwq))/(tfwq+k2)
    bm25 = t1 * t2 * t3 * 1.0
    return bm25

def p_LaplaceCalc(tf, docLen):
    vocabSize = 178050
    tf = tf * 1.0
    docLen = docLen * 1.0
    p_Laplace = (tf+1.0)/(docLen+vocabSize)
    return p_Laplace

def jelinek_mercerCalc(tf, docLen, resultTTF, resultTDF):
    tf = tf * 1.0
    docLen = docLen * 1.0
    resultTDF = resultTDF * 1.0
    resultTTF = resultTTF * 1.0
    smoother = 0.7
    t1 = smoother * (tf/docLen)
    t2 = (1-smoother) * (resultTTF/resultTDF)
    jelinek_mercer = t1 + t2
    return jelinek_mercer

def createOutput(es,okapiResultSet,tfIDFResultSet,sorted_OkapiBM25,sorted_Lm_Laplace, sorted_Jm_Smoothing, queryCounter, queryNo):
    f = open('1_Okapi_Result.txt','a')
    f2 = open('2_TFIDF_Result.txt','a')
    f_BM25 = open('3_OkapiBM25_Result.txt','a')
    f_lmLaplace = open('4_LM_Laplace_Result.txt','a')
    f_jmSmoothing = open('5_JM_Smoothing_Result.txt','a')
    count = 1
    
    for i in range(len(okapiResultSet)):
        results = es.get(index='ap_dataset', id=str(okapiResultSet[i][0]), doc_type='document')
        tfidf_results = es.get(index='ap_dataset', id=str(tfIDFResultSet[i][0]), doc_type='document')
        okapiBM25_Results = es.get(index='ap_dataset', id=str(sorted_OkapiBM25[i][0]), doc_type='document')
        LmLaplace_Results = es.get(index='ap_dataset', id=str(sorted_Lm_Laplace[i][0]), doc_type='document')
        JmSmoothing_Results = es.get(index='ap_dataset', id=str(sorted_Jm_Smoothing[i][0]), doc_type='document')
        
        stri = str(queryNo[queryCounter])+" "+"Q0 "+str(results['_source']['docno'])+" "+str(count)+" "+str(okapiResultSet[i][1])+" Exp\n"
        tfIDF_Result = str(queryNo[queryCounter])+" "+"Q0 "+str(tfidf_results['_source']['docno'])+" "+str(count)+" "+str(tfIDFResultSet[i][1])+" Exp\n"
        BM25_Result = str(queryNo[queryCounter])+" "+"Q0 "+str(okapiBM25_Results['_source']['docno'])+" "+str(count)+" "+str(sorted_OkapiBM25[i][1])+" Exp\n"
        LmLaplace_Result = str(queryNo[queryCounter])+" "+"Q0 "+str(LmLaplace_Results['_source']['docno'])+" "+str(count)+" "+str(sorted_Lm_Laplace[i][1])+" Exp\n"
        JmSmoothing_Result = str(queryNo[queryCounter])+" "+"Q0 "+str(JmSmoothing_Results['_source']['docno'])+" "+str(count)+" "+str(sorted_Jm_Smoothing[i][1])+" Exp\n"
        
        f.write(stri)
        f2.write(tfIDF_Result)
        f_BM25.write(BM25_Result)
        f_lmLaplace.write(LmLaplace_Result)
        f_jmSmoothing.write(JmSmoothing_Result)
        
        count = count + 1
        
    f.close()
    f2.close()
    f_BM25.close()
    f_lmLaplace.close()
    f_jmSmoothing.close()
    
# def createTfIDFOutput(es, tfIDFResultSet,queryCounter, queryNo):
#     count2 = 1
#     for i in range(len(tfIDFResultSet)):
#         results = es.get(index='ap_dataset', id=str(tfIDFResultSet[i][0]), doc_type='document')
#         stri = str(queryNo[queryCounter])+" "+"Q0 "+str(results['_source']['docno'])+" "+str(count)+" "+str(tfIDFResultSet[i][1])+" Exp\n"
#         f2.write(stri)
#         count = count + 1
#     f2.close()
    
def processText(currentLine):
    currentLine = re.sub('[,"]','',currentLine)
    currentLine = currentLine.replace("'","")
    currentLine = currentLine.replace("-"," ")
    currentLine = currentLine[currentLine.index("."):]
    currentLine = currentLine[4:]
    currentLine = removeWords(3, currentLine)
    currentLine = removeStopWords(currentLine)
    currentLine = currentLine[:currentLine.rfind('.')]
#     currentLine = re.sub('[,"-()]',' ',currentLine)
#     currentLine = re.sub('[.]','',currentLine)
    #currentLine = re.sub('[0123456789,"]','',currentLine)
    currentLine = stemQuery(currentLine)
    currentLine = stemQuery(currentLine)
    currentLine = currentLine.strip()
    return currentLine

def stemQuery(currentLine):
    temp = ""
    res = PorterStemmer()
    for word in currentLine.split():
        temp = temp +" "+ res.stem(word, 0 , (len(word)-1))
    return temp
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
    #if(tfwd != []):
#         okapi_TF = []
        temp = 0.0
        #docLen = findDocLen(docLength, tfwd[0][0])
        #docLen = docLength[0][1]
        tf = tf * 1.0
        docLen = docLen * 1.0
        avg = avg * 1.0
        temp = tf/(tf + 0.5 + (1.5 * (docLen/avg)))
#         okapi_TF = (docID, word, temp)
#         if(okapi_TF == []):
#             okapi_TF = (-1,word,0)
        return temp
    #else:
    #    return (-1,word,0)
    
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
        #count = count + 1
        #print "@@@@@@@",count,"..."
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
    #for i in range(len(docFreqs)):
    #    temp += docFreqs[i][1]
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
            #print "DF :",df
    return df


if __name__ == "__main__":
    main()
                
                