#!/usr/bin/python

from libreddit.libreddit import pullSubmissions
import redis
import time
import sys

KEYSPACE = "reddit:wordcount:"

''' 
Title -> [Word]
'''
def getWordListFromTitle(title):
    isAlnumOrSpace = lambda x: x.isalnum() or x.isspace()
    return filter(isAlnumOrSpace, title.lower()).split()

'''
[Word] -> {Word, Count}
'''
def getWordCountFromWordList(words):
    wordSet = set(words)
    return dict([(w, words.count(w)) for w in wordSet])


def getSubreddits(r):
    return r.smembers(KEYSPACE + 'subreddits')

def getDates(subreddit, r):
    return r.smembers(KEYSPACE + 'subreddits:' + subreddit)

def getWordCount(subreddit, date, r):
    d = r.hgetall(KEYSPACE + 'subreddits:' + subreddit + ':' + date)
    return dict(map(lambda (w,c): (w, int(c)), d.items()))

def getTotalWordCount(subreddit, r):
    wordCounts = [getWordCount(subreddit, date, r) for date in getDates(subreddit, r)]
    
    return apply(merge, [lambda x,y:x+y] + wordCounts)
    
def saveWordCount(wordCountDict, subreddit, date, r):
    r.sadd(KEYSPACE + 'subreddits', subreddit)
    r.sadd(KEYSPACE + 'subreddits:' + subreddit, date)
    
    for (word, count) in wordCountDict.iteritems():
        r.hincrby(KEYSPACE + 'subreddits:' + subreddit + ':' + date, word, count)
        

def getWordSources(word, subreddit, date, r):
    return r.smembers(KEYSPACE + 'subreddits:' + subreddit + ':' + date + ':' + word)
    
def getAllWordSources(word, subreddit, r):
    wordSources = [getWordSources(word, subreddit, date, r) for date in getDates(subreddit, r)]
    return reduce(list.__add__, wordSources)
        
def saveWordSources(submissions, subreddit, date, r):
    submissions = [(getWordListFromTitle(title), redditURL) for (title, _, redditURL) in submissions]
    
    for (word, _) in getWordCount(subreddit, date, r).iteritems():
        relevantSubmissions = filter(lambda x: word in x[0], submissions)
        for (_, redditURL) in relevantSubmissions:
            r.sadd(KEYSPACE + 'subreddits:' + subreddit + ':' + date + ':' + word, redditURL)


def getSavedSubmissions(r):
    return r.smembers(KEYSPACE + 'submissions')

def alreadySaved(redditURL, r):
    return r.sismember(KEYSPACE + 'submissions', redditURL)

def saveSubmission(redditURL, r):
    r.sadd(KEYSPACE + 'submissions', redditURL)

        
              

def main():
    subreddit = sys.argv[1]
    r = redis.Redis()
    
    while(True):
        newSubmissions = filter(lambda x: not alreadySaved(x[2], r), pullSubmissions(subreddit))
        print "Got", len(newSubmissions), "new stories for /r/",subreddit, "on", time.strftime("%Y-%m-%d %H:%M:%S")
        for (title, _, _) in newSubmissions:
            print "\t", title

        if newSubmissions:
            titles = [title for (title, _, _) in newSubmissions]
            wordCounts = map(compose(getWordCountFromWordList, getWordListFromTitle), titles)
            wordFrequencyDict = apply(merge, [lambda x,y:x+y] + wordCounts)
            
            dateKey = time.strftime("%Y%m%d")
            
            saveWordCount(wordFrequencyDict, subreddit, dateKey, r)
            for (_, _, redditURL) in newSubmissions:
                saveSubmission(redditURL, r)
            
            saveWordSources(newSubmissions, subreddit, dateKey, r)
        
        print "\t", "Sleeping for 5 minutes."
        time.sleep(5 * 60) # sleep for 5 minutes
    
    
###### Utilities #######

def compose(*fs):
    def _compose(f, g):
        return lambda x: f(g(x))
    return reduce(_compose, fs)


''' Taken (and slightly modified) from: http://goo.gl/o5c4I '''
def merge(mergef, *d):
    def _merge(d1, d2):
        result = dict(d1)
        for k,v in d2.iteritems():
            if k in result:
                result[k] = mergef(result[k], v)
            else:
                result[k] = v
        return result
    
    return reduce(_merge, d)

if __name__ == "__main__":
    main()
