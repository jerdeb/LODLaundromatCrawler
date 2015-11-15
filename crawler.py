import SPARQLWrapper
import rdflib
import urllib2
import logging
import rdfextras
import requests
import os
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.serializer import Serializer
from rdflib import plugin


# Logging configuration
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(relativeCreated)d - %(name)s - %(levelname)s - %(message)s")
logger_crawl = logging.getLogger("crawler")
logger_crawl.setLevel(logging.DEBUG)



QUALITY_SERVER = 'http://localhost:8080/Luzzu/compute_quality'
LOD_LAUNDROMAT_SPARQL = 'http://lodlaundromat.org/sparql/'
LOD_LAUNDROMAT_DOWNLOAD = 'http://download.lodlaundromat.org/'
CRAWLER_DIR = '/tmp/crawled/'

# FUNCTIONS
def download(uri):
    response = urllib2.urlopen(uri)
    localName = response.info()['Content-Disposition'].split('filename=')[1]
    fh = open(CRAWLER_DIR+localName, "w")
    fh.write(response.read())
    fh.close()
    return CRAWLER_DIR+localName

def loadMetricConfiguration():
    g = rdflib.Graph();
    config = g.parse("config.ttl", format="turtle")
    return g.serialize(format="json-ld", indent=0)

def formatMetricConfiguration(configStr):
    formattedStr = configStr.replace('\n', ' ').replace('\r', '').replace('"','\"')
    return formattedStr

# MAIN
sparql = SPARQLWrapper(LOD_LAUNDROMAT_SPARQL)
sparql.setQuery('PREFIX llo: <http://lodlaundromat.org/ontology/> SELECT ?md5 WHERE { ?d llo:triples ?n . ?d llo:md5 ?md5 . FILTER (?n > 0) }')
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

if not os.path.exists(CRAWLER_DIR):
    os.makedirs(CRAWLER_DIR)

metricsConf = formatMetricConfiguration(loadMetricConfiguration())

for result in results["results"]["bindings"]:
    document = LOD_LAUNDROMAT_DOWNLOAD + result['md5']['value']
    print 'Downloading : '+document
    filename = download(document)

    logger_crawl.info("Metrics config: {0}".format(metricsConf))
    payload = {'Dataset' : filename, 'QualityReportRequired' : 'false', 'MetricsConfiguration' : metricsConf, 'BaseUri' : document, 'IsSparql': 'false' }
    logger_crawl.debug("Sending POST. URL: {0}. Dataset: {1}. Base URI: {2}".format(QUALITY_SERVER, filename, document))
    try:
        r = requests.post(QUALITY_SERVER, data=payload)
        logger_crawl.info("Quality assessment completed for: {0}. Result: {1}".format(filename, r.text))
    except Exception as ex:
        logger_crawl.exception("Error processing request. Crawling aborted.")
    os.remove(filename)