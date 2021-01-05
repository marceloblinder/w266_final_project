import os
import zipfile
import html
from bs4 import BeautifulSoup
from collections import defaultdict

def get_attribute(element, attribute_name):
    attributes = element.get(attribute_name)

    return attributes if attributes != None else None

def has_full_text(doc):
    body = doc.nitf.body
    content = body.find('body.content')
    full_text = [i for i in content.findAll('block') if get_attribute(i, 'class') == 'full_text']

    return len(full_text) == 1

def is_news(classifiers):
    taxonomic_classifiers = [i for i in classifiers if get_attribute(i, 'type') == 'taxonomic_classifier']
    news_classifiers = [i.text for i in taxonomic_classifiers if 'Top/News' in i.text]
    has_news = len(news_classifiers) != 0
    has_valid_news = has_news and max([len(i.split('/')) for i in news_classifiers]) > 2

    if not has_valid_news:
        return False

    is_corrections = any(i.text == 'Top/News/Corrections' for i in taxonomic_classifiers)
    is_editor_notes = any(i.text == "" for i in taxonomic_classifiers)
    is_classifieds = any('Top/Classifieds' in i.text for i in taxonomic_classifiers)

    # <classifier class="indexing_service" type="descriptor">Reviews</classifier>
    is_review = any([i.text == 'Reviews' for i in classifiers if get_attribute(i, 'class') == 'indexing_service' and 
                                                                get_attribute(i, 'type') == 'descriptor'])

    return not (is_corrections or is_editor_notes or is_review or is_classifieds)

def is_correction(doc):
    blocks = doc.nitf.body.findAll('block')

    # <block class="correction_text">
    return any([i for i in blocks if get_attribute(i, 'class') == 'correction_text'])

class Processor:
    def __init__(self):
        pass

    def set_document_id(self, document_id):
        pass

    def process_general_descriptors(self, descriptors):
        pass

    def process_taxonomic_news_classifiers(self, classifiers):
        pass

    def process_body(self, text):
        pass

general_descriptors = defaultdict(int)  
taxonomic_classifiers = defaultdict(int)

def get_document_id(doc):
    doc_id = doc.nitf.head.docdata.find('doc-id')

    return get_attribute(doc_id, 'id-string')

def get_classifiers_by_type(classifiers, classifier_type):
    return (i for i in classifiers if get_attribute(i, 'type') == classifier_type)

def get_general_descriptors(classifiers):
    return [i.text for i in get_classifiers_by_type(classifiers, 'general_descriptor')]

def get_top_news(classifiers):
    return [i.text for i in get_classifiers_by_type(classifiers, 'taxonomic_classifier') if 'Top/News/' in i.text]

def process_body(body):
    content = body.find('body.content')
    full_text = [i for i in content.findAll('block') if get_attribute(i, 'class') == 'full_text'][0]
    paragraphs = [html.unescape(i.text).replace("''", '"') for i in full_text.findAll('p')]

    return "\n".join(paragraphs)

def process_xml(filename, raw_xml, processor):
    doc = BeautifulSoup(raw_xml, 'xml')
    classifiers = doc.nitf.head.docdata.findAll('classifier')

    if is_news(classifiers) and has_full_text(doc) and not is_correction(doc):
        processor.set_document_id(get_document_id(doc))
        processor.process_general_descriptors(get_general_descriptors(classifiers))
        processor.process_taxonomic_news_classifiers(get_top_news(classifiers))
        processor.process_body(process_body(doc.nitf.body))

def process_all_files(folder, filename, processor):
    with zipfile.ZipFile(os.path.join(folder, filename)) as zip_file:  
        for xml_filename in (i for i in zip_file.namelist() if i[-4:] == '.xml' and '__' not in i):
            with zip_file.open(xml_filename) as f:
                raw_xml = f.read()
                process_xml(xml_filename, raw_xml, processor)

if __name__ == '__main__':
    folder = '/Users/marceloblinder/w266/data'
    filename = 'nyt_corpus_2004.zip'
    process_all_files(folder, filename, Processor())
    print(len(general_descriptors))
    print(len(taxonomic_classifiers))
