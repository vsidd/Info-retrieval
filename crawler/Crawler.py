'''
Created on Mar 11, 2015

@author: Siddarthan
'''

# from __future__ import print_function
import urlparse
import robotparser
from bs4 import BeautifulSoup
import urllib2
import heapq
import time
from elasticsearch import Elasticsearch
import pickle
# import cProfile
# import re
# from line_profiler import LineProfiler


extensionsToIgnore = ['.jpg','.png','.svg','.pdf']

class HW3:
#     @profile  
    def __init__(self):
        self.currFrontier = []
        self.nextFrontier = []
        self.crawled = []
        self.frontierContains = {}
        self.frontierMap = {}
        self.changeToMap = {}
        self.inlinkMap = {}
        self.outlinkMap = {}
        self.robotCache = {}
        self.counter = 1
        self.currOutLinks = ""
        
        robotParser = robotparser.RobotFileParser()
        robotParser.set_url(urlparse.urljoin("http://www.google.com", "/robots.txt"))
        robotParser.read()
        self.robotCache["http://www.google.com"] = robotParser
        self.robotTxt = robotParser
        
        self.start_time = time.time()
        self.outLinkFile = open('OutLink.txt', 'ab+')
        self.inLinkFile = open('InLink.txt', 'ab+')
        self.crawledPages = open('myfile','w')
        
#     @profile  
    def loadFrontier(self, SeedUrl):
        heapq.heappush(self.currFrontier, (float("-inf"), self.counter, SeedUrl))
        self.frontierContains[SeedUrl] = True
        self.counter = self.counter + 1
        self.inlinkMap[SeedUrl] = set()
        

#     @profile        
    def crawler(self):
        i = 0
        while self.currFrontier != [] or self.nextFrontier != []:
                if(self.currFrontier == []) and (self.nextFrontier != []):
                    self.loadCurrentFrontier()
                    
                record = heapq.heappop(self.currFrontier)
                page = record[2]
                self.outlinkMap[page] = set()
                parsedURL = urlparse.urlparse(page)
                domain = parsedURL.scheme + "://" + parsedURL.netloc
                
                try:
                    self.getRobotParser(domain)
                    print "Peeked item : ",page
                    self.crawledPages.write(page+"\n")
                    self.crawled.append(page)
                    httpHeader = self.head(page)
                    pagesource=urllib2.urlopen(page)      # receive an object
                    s=pagesource.read()                   # the html page is got here
                    soup=BeautifulSoup(s)
                    title = soup.title.string
                except:
                    continue

                self.currOutLinks = ""
                self.loadNextFrontier(soup, page)

                content = self.extractContent(soup) 
                self.indexWebPage(content, page, unicode(soup), httpHeader, unicode(title), self.currOutLinks)
                
                i = i + 1
                print "I : ",i
                print "frontier length : ",len(self.nextFrontier)
                if(i>=3):
                    self.finalOperations()
                    break

    def finalOperations(self):
        print "crawled length : ",len(self.crawled)
        print time.time() - self.start_time
        pickle.dump(self.outlinkMap, self.outLinkFile)
        pickle.dump(self.inlinkMap, self.inLinkFile)
        self.outLinkFile.close()
        self.inLinkFile.close()
        self.crawledPages.close()
    
    def loadCurrentFrontier(self):
        self.findAndReplace()
        self.currFrontier = self.nextFrontier[:]
        heapq.heapify(self.currFrontier)
        self.nextFrontier = []
                    
    def loadNextFrontier(self, soup, page):
        canFetch = True
        for link in soup.find_all('a',href=True):
            try:
                cleanURL = self.urlCanonicalization(page,link['href'])
                if any(ext in cleanURL for ext in extensionsToIgnore):
                    raise Exception("Image URL")
                canFetch = self.robotTxt.can_fetch("*",cleanURL)
                if canFetch:
                    if cleanURL not in self.crawled:
                        if self.frontierMap.has_key(cleanURL):
                            nodeVal = self.frontierMap[cleanURL]
                            toChange = self.changeToMap[nodeVal] 
                            self.changeToMap[nodeVal] = (toChange[0]-1, toChange[1],toChange[2])
                        else:
                            self.inlinkMap[cleanURL] = set()
                            self.outlinkMap[page].add(cleanURL)
                            self.currOutLinks = self.currOutLinks + ", "+cleanURL
                            heapq.heappush(self.nextFrontier, (-1, self.counter, cleanURL))
                            self.frontierMap[cleanURL] = (-1, self.counter, cleanURL)
                            self.changeToMap[(-1, self.counter, cleanURL)] = (-1, self.counter, cleanURL)
                            self.counter = self.counter + 1
                        self.inlinkMap[cleanURL].add(page)
            except:
                continue

    def getRobotParser(self, domain):
        if self.robotCache.has_key(domain):
            self.robotTxt = self.robotCache[domain]
        else:
            robotParser = robotparser.RobotFileParser()
            robotParser.set_url(urlparse.urljoin(domain, "/robots.txt"))
            robotParser.read()
            self.robotCache[domain] = robotParser
            self.robotTxt = self.robotCache[domain]

    def extractContent(self, soup):
        contentsToRemove = ['script','style','title','a','link']
        [s.extract() for s in soup(contentsToRemove)]
        content = soup.get_text().encode('utf-8')
        content = content.replace('\n', ' ').replace('\r', ' ').replace('\t',' ')
        content = content.strip()
        return content
    
#     @profile          
    def findAndReplace(self):
            for n,i in enumerate(self.nextFrontier):
                    self.nextFrontier[n]=self.changeToMap[i]

    def indexWebPage(self, content, url, rawHTML, httpHeader, title, outLinks):
        es = Elasticsearch(['localhost:9201'])
        es.index(index="1504_team_apple", doc_type="document", id=url,
                 body={
                       "HTTPheader": httpHeader,
                       "title": title,
                       "text": content,
                       "html_Source": rawHTML,
                       "out_links": outLinks,
                       "in_links": ' 0'
        })
    
            
#     @profile  
    def urlCanonicalization(self, page,link):
        absURL =  urlparse.urljoin(page,link)
        parsedURL = urlparse.urlparse(absURL)
        domainName = parsedURL.netloc.lower()
        scheme = parsedURL.scheme.lower()
        if scheme is 'http':
            domainName = domainName.replace(':80','')
        if scheme is 'https':
            domainName = domainName.replace(':443','')
        cleanURL = scheme+"://"+ domainName+ parsedURL.path.replace('//','/')
        
        return cleanURL

#     @profile  
    def head(self, url):
        try:
            request = urllib2.Request(url)
            request.get_method = lambda: 'HEAD'
            head = urllib2.urlopen(request).info()
            head = unicode(head)
        except:
            head=""
        return head
        
crawl = HW3()
crawl.loadFrontier('http://en.wikipedia.org/wiki/History_of_Apple_Inc.')
crawl.loadFrontier('http://en.wikipedia.org/wiki/Retina_Display')
crawl.loadFrontier('http://www.webopedia.com/TERM/R/retina_display.html')
# crawl.loadFrontier('http://computer.howstuffworks.com/')
crawl.crawler()
# profile = LineProfiler(crawl.crawler())
# profile.print_stats()

    
