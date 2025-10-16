import os
import sys
import json
import re
import time
import requests 
from dotenv import load_dotenv # You may need to run: pip install python-dotenv
from PyPDF2 import PdfReader
from ebooklib import epub
import ebooklib
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
load_dotenv() # Load variables from the .env file into the environment
API_KEY = os.getenv("GEMINI_API_KEY") 
CACHE_FILE = 'publisher_cache.json'
DEFAULT_PROMPT_FILE = 'prompt.txt'

def load_cache():
    """Loads the publisher normalization cache from a JSON file."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache_data):
    """Saves the publisher normalization cache to a JSON file."""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_data, f, indent=2)

def normalize_publishers_batch_ai(publisher_list, prompt_template, verbose=False):
    """
    Normalizes a list of publisher names in a single batch API call.
    Returns a dictionary mapping original names to normalized names.
    """
    normalized_map = {}
    if not publisher_list:
        return normalized_map

    if verbose:
        print(f"\n[i] Normalizing {len(publisher_list)} unique publisher(s) with AI...")

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={API_KEY}"
    
    publisher_json_string = json.dumps(publisher_list)
    
    # Use the prompt template read from the file
    prompt = prompt_template.format(publisher_json_string=publisher_json_string)

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        ai_response_text = result['candidates'][0]['content']['parts'][0]['text'].strip()
        
        json_str = ai_response_text.strip('` \n').removeprefix('json').strip()

        normalized_map = json.loads(json_str)
        if verbose:
            print("[i] AI normalization successful.")

    except requests.exceptions.RequestException as e:
        if verbose:
            print(f"\n[!] API Error during batch normalization: {e}")
    except (KeyError, IndexError, json.JSONDecodeError) as e:
        if verbose:
            print(f"\n[!] Could not parse AI batch response: {e}")
            print(f"    Raw response was: {ai_response_text}")

    return normalized_map

def extract_epub_metadata(file_path, verbose=False):
    """Extracts raw metadata from an EPUB file without normalization."""
    try:
        book = epub.read_epub(file_path)
        metadata = {
            'title': book.get_metadata('DC', 'title')[0][0] if book.get_metadata('DC', 'title') else 'N/A',
            'authors': [author[0] for author in book.get_metadata('DC', 'creator')] if book.get_metadata('DC', 'creator') else [],
            'publisher': book.get_metadata('DC', 'publisher')[0][0] if book.get_metadata('DC', 'publisher') else 'N/A',
        }
        return metadata
    except Exception as e:
        if verbose:
            print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def extract_pdf_metadata(file_path, verbose=False):
    """Extracts raw metadata from a PDF file without normalization."""
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            meta = reader.metadata
            if not meta:
                return {'title': 'N/A', 'authors': [], 'publisher': 'N/A'}
            
            authors = [meta.author] if meta.author else []
            publisher = meta.producer or 'N/A'

            metadata = {
                'title': meta.title or 'N/A',
                'authors': authors,
                'publisher': publisher,
            }
            return metadata
    except Exception as e:
        if verbose:
            print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def process_file(file_path, verbose=False):
    """
    Processes a single file to extract its metadata.
    Designed to be run in a separate thread.
    """
    filename = os.path.basename(file_path)
    book_meta = None
    if filename.lower().endswith('.epub'):
        book_meta = extract_epub_metadata(file_path, verbose)
    elif filename.lower().endswith('.pdf'):
        book_meta = extract_pdf_metadata(file_path, verbose)
    
    if book_meta:
        book_meta['filename'] = filename
    return book_meta

def main():
    """Main function to find and process files, collecting their metadata."""
    args = sys.argv[1:]
    verbose = '-v' in args or '--verbose' in args
    args = [arg for arg in args if arg not in ('-v', '--verbose')]

    num_threads = os.cpu_count() or 4
    prompt_filepath = DEFAULT_PROMPT_FILE
    
    # --- Argument Parsing ---
    # Use a while loop to safely remove items as we iterate
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ('-t', '--threads'):
            try:
                num_threads = int(args[i + 1])
                if num_threads <= 0: raise ValueError
                args.pop(i) # Remove flag
                args.pop(i) # Remove value
                continue
            except (ValueError, IndexError):
                print(f"Error: {arg} requires a positive integer. e.g., {arg} 8")
                return
        elif arg in ('-p', '--prompt'):
            try:
                prompt_filepath = args[i + 1]
                args.pop(i) # Remove flag
                args.pop(i) # Remove value
                continue
            except IndexError:
                print(f"Error: {arg} requires a file path argument.")
                return
        i += 1

    target_directory = args[0] if args else os.getcwd()

    if not os.path.isdir(target_directory):
        print(f"Error: The specified path '{target_directory}' is not a valid directory.")
        return

    publisher_cache = load_cache()
    
    try:
        files_to_process = [f for f in os.listdir(target_directory) if f.lower().endswith(('.epub', '.pdf'))]
        total_files = len(files_to_process)
        if total_files == 0:
            print(f"No .epub or .pdf files found in '{target_directory}'.")
            return

        # --- Step 1: Extract all raw metadata using a thread pool ---
        print(f"Step 1: Extracting raw metadata from {total_files} files in '{target_directory}' using {num_threads} threads...")
        raw_metadata_list = []
        all_publishers = set()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {executor.submit(process_file, os.path.join(target_directory, f), verbose): f for f in files_to_process}
            
            processed_count = 0
            for future in as_completed(futures):
                book_meta = future.result()
                
                if book_meta:
                    raw_metadata_list.append(book_meta)
                    if book_meta['publisher'] and book_meta['publisher'] != 'N/A':
                        all_publishers.add(book_meta['publisher'])

                processed_count += 1
                percent = (processed_count / total_files) * 100
                bar = '█' * int(percent / 2) + '-' * (50 - int(percent / 2))
                sys.stdout.write(f'\rProgress: |{bar}| {processed_count}/{total_files} ({percent:.1f}%)')
                sys.stdout.flush()
        print("\nExtraction complete.")

        # --- Step 2: Normalize all unique publisher names ---
        print("\nStep 2: Normalizing publisher names...")
        publisher_map = {}
        publishers_for_ai = []

        rules = [
            (['packt', 'paclt'], 'Packt Publishing'),
            (["o'reilly", "o’reilly"], "O'Reilly Media"),
            (['mercury learning'], 'Mercury Learning and Information'),
            (['leaping hare'], 'Leaping Hare Press'),
            (['berrett-koehler'], 'Berrett-Koehler Publishers')
        ]

        for name in all_publishers:
            if name in publisher_cache:
                publisher_map[name] = publisher_cache[name]
                continue
            
            lower_name = name.lower()
            found_rule = False
            for keywords, canonical in rules:
                if any(kw in lower_name for kw in keywords):
                    publisher_map[name] = canonical
                    publisher_cache[name] = canonical
                    found_rule = True
                    break
            if not found_rule:
                publishers_for_ai.append(name)
        
        if publishers_for_ai:
            if not API_KEY:
                print("[!] AI normalization skipped: GEMINI_API_KEY not set in .env file.")
            else:
                try:
                    with open(prompt_filepath, 'r') as f:
                        prompt_template = f.read()
                    
                    ai_results = normalize_publishers_batch_ai(publishers_for_ai, prompt_template, verbose)
                    publisher_map.update(ai_results)
                    publisher_cache.update(ai_results)
                except FileNotFoundError:
                    print(f"\n[!] Prompt file not found at '{prompt_filepath}'. Skipping AI normalization.")
        
        print("Normalization complete.")

        # --- Step 3: Assemble final metadata ---
        print("\nStep 3: Assembling final results...")
        final_metadata = {}
        for raw_meta in raw_metadata_list:
            publisher = raw_meta['publisher']
            normalized = publisher_map.get(publisher, publisher)
            
            final_metadata[raw_meta['filename']] = {
                'title': raw_meta['title'],
                'authors': raw_meta['authors'],
                'publisher': publisher,
                'publisher_normalized': normalized
            }
        print("Assembly complete.")

        print("\n--- All Collected Metadata ---")
        print(json.dumps(final_metadata, indent=2))

    finally:
        print("\nSaving publisher cache...")
        save_cache(publisher_cache)
        print("Done.")

if __name__ == "__main__":
    main()


