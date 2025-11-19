import json
import xml.etree.ElementTree as ET
from xml.dom import minidom

class RDFTransformer:
    def __init__(self):
        self.namespaces = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'z': 'http://www.zotero.org/namespaces/export#',
            'dcterms': 'http://purl.org/dc/terms/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'bib': 'http://purl.org/net/biblio#'
        }
    
    def json_to_rdf(self, json_data, output_file):
        """Convert JSON data to Zotero-compatible RDF"""
        # Create root element with namespaces
        root = ET.Element('rdf:RDF')
        for prefix, uri in self.namespaces.items():
            root.set(f'xmlns:{prefix}', uri)
        
        # Process each document
        for i, doc in enumerate(json_data, 1):
            item_id = f"item_{i}"
            
            # Create main description
            desc = self._create_main_description(doc, item_id)
            root.append(desc)
            
            # Create memo for content if field_6_content exists
            if doc.get('field_6_content'):
                memo = self._create_content_memo(doc, item_id)
                root.append(memo)
        
        # Write to file
        tree = ET.ElementTree(root)
        self._pretty_write(tree, output_file)
    
    def _create_main_description(self, doc, item_id):
        """Create the main rdf:Description for a document"""
        desc = ET.Element('rdf:Description')
        desc.set('rdf:about', doc['url'])
        
        # Required Zotero fields
        self._add_element(desc, 'z:itemType', 'document')
        ref_elem = ET.SubElement(desc, 'dcterms:isReferencedBy')
        ref_elem.set('rdf:resource', f"#{item_id}")
        # Map fields
        self._add_element(desc, 'dc:title', doc.get('field_1_title'))
        
        # Split keywords into multiple subject elements
        keywords = doc.get('field_5_palabras_clave')
        if keywords:
            for keyword in keywords.split(','):
                self._add_element(desc, 'dc:subject', keyword.strip())
        
        self._add_element(desc, 'dcterms:abstract', doc.get('field_7_notas'))
        self._add_element(desc, 'z:language', 'es')  # Hardcoded Spanish
        self._add_element(desc, 'z:archive', 'Archivo del Libertador')  # Hardcoded archive
        
        self._add_element(desc, 'dc:coverage', doc.get('field_2_seccion'))
        
        # URL identifier
        if doc.get('url'):
            ident = ET.SubElement(desc, 'dc:identifier')
            uri = ET.SubElement(ident, 'dcterms:URI')
            self._add_element(uri, 'rdf:value', doc['url'])
        
        self._add_element(desc, 'dcterms:dateSubmitted', doc.get('scraped_at'))
        
        # Combined description for lugares and personas
        lugares = doc.get('field_4_lugares')
        personas = doc.get('field_3_personas')
        if lugares or personas:
            desc_text = ""
            if lugares:
                desc_text += f"Lugar:\"{lugares}\"\n"
            if personas:
                desc_text += f"Gente:{personas}"
            self._add_element(desc, 'dc:description', desc_text.strip())
        
        return desc
    
    def _create_content_memo(self, doc, item_id):
        """Create bib:Memo for document content"""
        memo = ET.Element('bib:Memo')
        memo.set('rdf:about', f"#{item_id}")
        
        # Convert newlines to HTML breaks
        content = doc['field_6_content']
        html_content = content.replace('\n', '<br>')
        
        # Wrap in Zotero-style HTML
        wrapped_content = f'<div data-schema-version="9"><p>{html_content}</p></div>'
        self._add_element(memo, 'rdf:value', wrapped_content)
        
        return memo
    
    def _add_element(self, parent, tag, text):
        """Add element only if text is not None or empty"""
        if text:
            elem = ET.SubElement(parent, tag)
            elem.text = text
            return elem
        return None
    
    def _pretty_write(self, tree, filename):
        """Write XML with proper formatting"""
        rough_string = ET.tostring(tree.getroot(), encoding='utf-8')
        reparsed = minidom.parseString(rough_string)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(reparsed.toprettyxml(indent="  "))