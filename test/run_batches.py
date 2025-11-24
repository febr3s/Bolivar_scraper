import time
import subprocess
import json
import os

def run_scraping_batches():
    state_file = 'state.json'
    output_file = 'documents.json'
    total_items_needed = 10
    batch_size = 2
    
    # Initialize state if doesn't exist
    if not os.path.exists(state_file):
        with open(state_file, 'w') as f:
            json.dump({"last_id": 0, "total_scraped": 0, "current_batch": 0}, f)
    
    while True:
        # Check current progress
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        print(f"Progress: {state['total_scraped']}/{total_items_needed} items")
        
        # Check if we're done
        if state['total_scraped'] >= total_items_needed:
            print("Scraping completed!")
            break
        
        # Run one batch
        print("Starting new batch...")
        result = subprocess.run([
            'scrapy', 'runspider', 'batch_spider.py', 
            '-o', 'temp_batch.json',  # Temporary output
            '--nolog'  # Cleaner output
        ])
        
        if result.returncode == 0:
            print("Batch completed successfully")
        else:
            print("Batch had errors, but continuing...")
        
        # Wait before next batch
        print("Waiting 10 seconds before next batch...")
        time.sleep(10)

if __name__ == "__main__":
    run_scraping_batches()