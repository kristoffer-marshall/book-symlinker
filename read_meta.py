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

# --- CONFIGURATION ---
load_dotenv() # Load variables from the .env file into the environment
API_KEY = os.getenv("GEMINI_API_KEY") 
CACHE_FILE = 'publisher_cache.json'

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

def normalize_publisher_with_ai(publisher_name, cache, verbose=False):
    """
    Normalizes publisher names using a hybrid approach:
    1. Checks a local cache.
    2. Applies specific hard-coded rules.
    3. Falls back to an AI model for unknown names.
    """
    if not publisher_name or publisher_name == 'N/A':
        return 'N/A'

    # 1. Check the cache first for a quick result
    if publisher_name in cache:
        return cache[publisher_name]

    lower_pub = publisher_name.lower()

    # 2. Apply specific rules for common publishers
    rules = [
        (['packt', 'paclt'], 'Packt Publishing'),
        (["o'reilly", "o’reilly"], "O'Reilly Media"),
        (['mercury learning'], 'Mercury Learning and Information'),
        (['leaping hare'], 'Leaping Hare Press'),
        (['berrett-koehler'], 'Berrett-Koehler Publishers')
    ]
    for keywords, canonical_name in rules:
        if any(keyword in lower_pub for keyword in keywords):
            cache[publisher_name] = canonical_name # Save to cache
            return canonical_name

    # 3. If no rule matches, fall back to the Gemini AI model
    if not API_KEY:
        if verbose:
            print(f"\n[!] AI normalization skipped for '{publisher_name}': GEMINI_API_KEY not set in .env file.")
        return publisher_name # Return original if no key

    if verbose:
        print(f"\n[i] Normalizing '{publisher_name}' with AI...")
    
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={API_KEY}"
    
    prompt = (
        "You are a data normalization expert specializing in book publishers. "
        "Your task is to return the official, canonical name for the following publisher. "
        "Return ONLY the publisher's name and nothing else. "
        f"Publisher: \"{publisher_name}\""
    )

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json()
        
        normalized_name = result['candidates'][0]['content']['parts'][0]['text'].strip()
        
        # A simple cleanup for the AI's response
        normalized_name = re.sub(r'^"|"$', '', normalized_name) # Remove surrounding quotes
        
        if verbose:
            print(f"    -> AI suggestion: '{normalized_name}'")

        cache[publisher_name] = normalized_name # Save new result to cache
        return normalized_name

    except requests.exceptions.RequestException as e:
        if verbose:
            print(f"\n[!] API Error for '{publisher_name}': {e}")
        return publisher_name # Return original on error
    except (KeyError, IndexError) as e:
        if verbose:
            print(f"\n[!] Could not parse AI response for '{publisher_name}': {e}")
        return publisher_name

def get_epub_metadata(file_path, cache, verbose=False):
    """Extracts and normalizes metadata from an EPUB file."""
    try:
        book = epub.read_epub(file_path)
        publisher = book.get_metadata('DC', 'publisher')[0][0] if book.get_metadata('DC', 'publisher') else 'N/A'
        metadata = {
            'title': book.get_metadata('DC', 'title')[0][0] if book.get_metadata('DC', 'title') else 'N/A',
            'authors': [author[0] for author in book.get_metadata('DC', 'creator')] if book.get_metadata('DC', 'creator') else [],
            'publisher': publisher,
            'publisher_normalized': normalize_publisher_with_ai(publisher, cache, verbose)
        }
        return metadata
    except Exception as e:
        if verbose:
            print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def get_pdf_metadata(file_path, cache, verbose=False):
    """Extracts and normalizes metadata from a PDF file."""
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            meta = reader.metadata
            if not meta:
                return {'title': 'N/A', 'authors': [], 'publisher': 'N/A', 'publisher_normalized': 'N/A'}
            
            authors = [meta.author] if meta.author else []
            publisher = meta.producer or 'N/A'

            metadata = {
                'title': meta.title or 'N/A',
                'authors': authors,
                'publisher': publisher,
                'publisher_normalized': normalize_publisher_with_ai(publisher, cache, verbose)
            }
            return metadata
    except Exception as e:
        if verbose:
            print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def main():
    """Main function to find and process files, collecting their metadata."""
    args = sys.argv[1:]
    verbose = '-v' in args or '--verbose' in args
    args = [arg for arg in args if arg not in ('-v', '--verbose')]

    if args:
        target_directory = args[0]
    else:
        target_directory = os.getcwd()

    if not os.path.isdir(target_directory):
        print(f"Error: The specified path '{target_directory}' is not a valid directory.")
        return

    print(f"Scanning for files in: {target_directory}")
    
    publisher_cache = load_cache()
    all_metadata = {}
    
    try:
        files_to_process = [f for f in os.listdir(target_directory) if f.lower().endswith(('.epub', '.pdf'))]
        total_files = len(files_to_process)

        if total_files == 0:
            print(f"No .epub or .pdf files found in '{target_directory}'.")
            return

        for i, filename in enumerate(files_to_process):
            file_path = os.path.join(target_directory, filename)
            
            book_meta = None
            if filename.lower().endswith('.epub'):
                book_meta = get_epub_metadata(file_path, publisher_cache, verbose)
            elif filename.lower().endswith('.pdf'):
                book_meta = get_pdf_metadata(file_path, publisher_cache, verbose)
            
            if book_meta:
                all_metadata[filename] = book_meta
            
            processed_count = i + 1
            percent = (processed_count / total_files) * 100
            bar_length = 40
            filled_length = int(bar_length * processed_count // total_files)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            
            sys.stdout.write(f'\rProgress: |{bar}| {processed_count}/{total_files} ({percent:.1f}%)')
            sys.stdout.flush()

        print() 
        print("\n--- All Collected Metadata ---")
        print(json.dumps(all_metadata, indent=2))

    finally:
        print("\nSaving publisher cache...")
        save_cache(publisher_cache)
        print("Done.")

if __name__ == "__main__":
    main()



