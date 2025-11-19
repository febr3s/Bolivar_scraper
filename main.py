import scrapy

class DocumentSpider8Fields(scrapy.Spider):
    name = 'document_spider_8fields'
    
    custom_settings = {
        'USER_AGENT': 'AcademicResearchBot/1.0 (contact: eduardofebresm@gmail.com)',
        'DOWNLOAD_DELAY': 3,
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 1,
        'AUTOTHROTTLE_ENABLED': True,
    }
    
    start_urls = ['https://www.archivodellibertador.gob.ve/archlib/web/index.php/site/documento?id=1']
    
    def parse(self, response):
        # Field 1: Title
        title = response.css('div.float-left h1::text').get()
        
        # Fields 2-5, 8: Using the same pattern
        fields = {
            'field_2_seccion': 'Sección',
            'field_3_personas': 'Personas', 
            'field_4_lugares': 'Lugares',
            'field_5_palabras_clave': 'Palabras Clave',
            'field_8_traduccion': 'Traducción'
        }
        
        result = {
            'url': response.url,
            'scraped_at': '2024-01-01',
            'field_1_title': title.strip() if title else None,
        }
        
        # Extract fields 2-5 and 8 using the same method
        for field_key, field_label in fields.items():
            field_data = response.css(f'p:contains("{field_label}") ::text').getall()
            result[field_key] = self.clean_field_content(field_data, field_label)
        
        # Special handling for Fields 6 and 7 which are in the same paragraph
        descripcion_paragraph = response.css('p:contains("Descripción:")')
        if descripcion_paragraph:
            # Field 6: Main document content (everything after "Descripción:")
            result['field_6_content'] = self.extract_document_content(descripcion_paragraph)
            
            # Field 7: NOTAS section
            result['field_7_notas'] = self.extract_notes_section(descripcion_paragraph)
        else:
            result['field_6_content'] = None
            result['field_7_notas'] = None
        
        yield result
    
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
        """Extract Field 6: Main document content (everything after Descripción:)"""
        # Get all text nodes from the description paragraph
        all_text_nodes = descripcion_paragraph.css('::text').getall()
        
        content_parts = []
        collecting_content = False
        notes_started = False
        
        for text in all_text_nodes:
            clean_text = text.strip()
            
            if not clean_text:
                continue
                
            # Start collecting when we see "Descripción:"
            if 'Descripción:' in clean_text:
                collecting_content = True
                # Remove "Descripción:" from the content
                clean_text = clean_text.split('Descripción:')[-1].strip()
                if clean_text:  # If there's content immediately after
                    content_parts.append(clean_text)
                continue
                    
            # Stop collecting when we hit "NOTAS"
            if 'NOTAS' in clean_text:
                notes_started = True
                break
                
            # Collect content if we're in the content section
            if collecting_content and not notes_started:
                content_parts.append(clean_text)
        
        # Join all content parts
        full_content = ' '.join(content_parts).strip()
        return full_content if full_content else None
    
    def extract_notes_section(self, descripcion_paragraph):
        """Extract Field 7: NOTAS section content"""
        # Get all text nodes from the description paragraph
        all_text_nodes = descripcion_paragraph.css('::text').getall()
        
        notes_parts = []
        collecting_notes = False
        
        for text in all_text_nodes:
            clean_text = text.strip()
            
            if not clean_text:
                continue
                
            # Start collecting when we see "NOTAS"
            if 'NOTAS' in clean_text:
                collecting_notes = True
                # Remove "NOTAS" from the content
                clean_text = clean_text.split('NOTAS')[-1].strip()
                if clean_text:
                    notes_parts.append(clean_text)
                continue
                    
            # Collect notes content
            if collecting_notes:
                notes_parts.append(clean_text)
        
        # Join all notes parts
        full_notes = ' '.join(notes_parts).strip()
        return full_notes if full_notes else None