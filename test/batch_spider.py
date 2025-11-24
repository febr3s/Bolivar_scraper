import scrapy
import json
import time
from datetime import datetime
import os

class BatchDocumentSpider(scrapy.Spider):
    name = 'batch_document_spider'
    
    # Configuration - easily adjustable
    BATCH_SIZE = 2
    TOTAL_ITEMS = 10
    START_ID = 1
    DELAY_BETWEEN_REQUESTS = 3  # seconds
    DELAY_BETWEEN_BATCHES = 10  # seconds
    
    custom_settings = {
        'USER_AGENT': 'AcademicResearchBot/1.0 (contact: eduardofebresm@gmail.com)',
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 1,
        'AUTOTHROTTLE_ENABLED': True,
        'DOWNLOAD_DELAY': DELAY_BETWEEN_REQUESTS,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_file = 'state.json'
        self.output_file = 'documents.json'
        self.load_state()
        
    def load_state(self):
        """Load scraping state from file"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                self.state = json.load(f)
        else:
            self.state = {
                "last_id": self.START_ID - 1,
                "total_scraped": 0,
                "current_batch": 0
            }
            self.save_state()
    
    def save_state(self):
        """Save current scraping state"""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def load_existing_data(self):
        """Load existing documents from JSON file"""
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    
    def save_batch(self, batch_data):
        """Append batch to JSON file"""
        existing_data = self.load_existing_data()
        existing_data.extend(batch_data)
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)
    
    def start_requests(self):
        """Generate requests starting from last scraped ID"""
        current_id = self.state["last_id"] + 1
        end_id = min(current_id + self.BATCH_SIZE - 1, self.START_ID + self.TOTAL_ITEMS - 1)
        
        self.logger.info(f"Starting batch: IDs {current_id} to {end_id}")
        
        for doc_id in range(current_id, end_id + 1):
            url = f'https://www.archivodellibertador.gob.ve/archlib/web/index.php/site/documento?id={doc_id}'
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'doc_id': doc_id},
                errback=self.handle_error
            )
    
    def parse(self, response):
        """Parse individual document page"""
        doc_id = response.meta['doc_id']
        
        # Field 1: Title
        title = response.css('div.float-left h1::text').get()
        
        # Fields 2-5
        fields = {
            'field_2_seccion': 'Sección',
            'field_3_personas': 'Personas', 
            'field_4_lugares': 'Lugares',
            'field_5_palabras_clave': 'Palabras Clave',
        }
        
        result = {
            'id': doc_id,
            'url': response.url,
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'field_1_title': title.strip() if title else None,
        }
        
        # Extract fields 2-5
        for field_key, field_label in fields.items():
            field_data = response.css(f'p:contains("{field_label}") ::text').getall()
            result[field_key] = self.clean_field_content(field_data, field_label)
        
        # Special handling for Fields 6 and 7
        descripcion_paragraph = response.css('p:contains("Descripción:")')
        if descripcion_paragraph:
            result['field_6_content'] = self.extract_document_content(descripcion_paragraph)
            result['field_7_notas'] = self.extract_notes_section(descripcion_paragraph)
        else:
            result['field_6_content'] = None
            result['field_7_notas'] = None
        
        self.logger.info(f"Successfully scraped document ID: {doc_id}")
        return result
    
    def handle_error(self, failure):
        """Handle request failures"""
        doc_id = failure.request.meta['doc_id']
        self.logger.error(f"Failed to scrape document ID: {doc_id} - {failure.value}")
        
        # Return a minimal result to keep the pipeline going
        return {
            'id': doc_id,
            'url': f'https://www.archivodellibertador.gob.ve/archlib/web/index.php/site/documento?id={doc_id}',
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'field_1_title': None,
            'field_2_seccion': None,
            'field_3_personas': None,
            'field_4_lugares': None,
            'field_5_palabras_clave': None,
            'field_6_content': None,
            'field_7_notas': None,
            'error': str(failure.value)
        }
    
    def clean_field_content(self, field_data, field_name):
        """Helper method to extract content after the field label"""
        if not field_data:
            return None
            
        full_text = ' '.join([text.strip() for text in field_data if text.strip()])
        
        if field_name in full_text:
            content = full_text.split(field_name)[-1].strip()
            content = content.lstrip(':').strip()
            return content if content else None
        
        return full_text.strip() if full_text else None
    
    def extract_document_content(self, descripcion_paragraph):
        """Extract Field 6: Main document content"""
        all_text_nodes = descripcion_paragraph.css('::text').getall()
        
        content_parts = []
        collecting_content = False
        notes_started = False
        
        for text in all_text_nodes:
            clean_text = text.strip()
            
            if not clean_text:
                continue
                
            if 'Descripción:' in clean_text:
                collecting_content = True
                clean_text = clean_text.split('Descripción:')[-1].strip()
                if clean_text:
                    content_parts.append(clean_text)
                continue
                    
            if 'NOTAS' in clean_text:
                notes_started = True
                break
                
            if collecting_content and not notes_started:
                content_parts.append(clean_text)
        
        full_content = ' '.join(content_parts).strip()
        return full_content if full_content else None
    
    def extract_notes_section(self, descripcion_paragraph):
        """Extract Field 7: NOTAS section content"""
        all_text_nodes = descripcion_paragraph.css('::text').getall()
        
        notes_parts = []
        collecting_notes = False
        
        for text in all_text_nodes:
            clean_text = text.strip()
            
            if not clean_text:
                continue
                
            if 'NOTAS' in clean_text:
                collecting_notes = True
                clean_text = clean_text.split('NOTAS')[-1].strip()
                if clean_text:
                    notes_parts.append(clean_text)
                continue
                    
            if collecting_notes:
                notes_parts.append(clean_text)
        
        full_notes = ' '.join(notes_parts).strip()
        return full_notes if full_notes else None

    def closed(self, reason):
        """Called when spider closes - update state"""
        if hasattr(self, 'state'):
            # Update state based on what was actually scraped
            existing_data = self.load_existing_data()
            if existing_data:
                last_id = max(doc['id'] for doc in existing_data)
                self.state['last_id'] = last_id
                self.state['total_scraped'] = len(existing_data)
                self.save_state()
            
            self.logger.info(f"Spider closed. Reason: {reason}")
            self.logger.info(f"Current state: {self.state}")