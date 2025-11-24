import json
from rdfparser import RDFTransformer

# First run the spider and save JSON
# scrapy runspider updated_spider.py -o documents.json

# Then convert to RDF
with open('bolivar.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

transformer = RDFTransformer()
transformer.json_to_rdf(data, 'documents.rdf')

print("RDF file created: documents.rdf")