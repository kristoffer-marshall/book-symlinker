import os
import sys
import json
import csv
import time
import requests
from dotenv import load_dotenv  # You may need to run: pip install python-dotenv
from PyPDF2 import PdfReader
from ebooklib import epub
import ebooklib
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
load_dotenv()  # Load variables from the .env file into the environment
API_KEY = os.getenv("GEMINI_API_KEY")
METADATA_CACHE_FILE = 'metadata_cache.json'
PUBLISHER_CACHE_FILE = 'publisher_cache.json'
DEFAULT_PROMPT_FILE = 'prompt.txt'
DEFAULT_RULES_FILE = 'publisher_rules.csv'

def show_help():
    """Prints the help message and usage information."""
    print("Usage: python read_meta.py [directory] [options]")
    print("\nExtracts metadata from .epub and .pdf files in a specified directory.")
    print("\nIf no directory is provided, the current working directory is used.")
    print("\nOptions:")
    print("  [directory]             Optional path to the directory to scan.")
    print("  -h, --help              Show this help message and exit.")
    print("  -t, --threads NUMBER    Set the number of threads to use for processing.")
    print("                          (default: number of CPU cores)")
    print("  -p, --prompt FILE       Path to a custom prompt file for the AI.")
    print(f"                         (default: {DEFAULT_PROMPT_FILE})")
    print("  -o, --output FILE       Path to the output JSON file.")
    print("                          (default: metadata-processed.json)")
    print("  -r, --rules FILE        Path to a custom publisher normalization CSV file.")
    print(f"                         (default: {DEFAULT_RULES_FILE})")
    print("  -v, --verbose           Enable verbose output to see detailed progress and errors.")
    print("  --force-reload          Ignore the cache and re-process all files.")
    print("  --ai                    Enable AI to normalize publisher names (requires API key).")


def load_cache(cache_file):
    """Loads a generic cache from a JSON file, handling empty or corrupt files."""
    if os.path.exists(cache_file):
        if os.path.getsize(cache_file) == 0:
            return {}
        with open(cache_file, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}

def save_cache(cache_data, cache_file):
    """Saves a generic cache to a JSON file."""
    with open(cache_file, 'w') as f:
        json.dump(cache_data, f, indent=2)

def load_rules_from_csv(filepath, verbose=False):
    """Loads normalization rules from a CSV file where all fields are quoted."""
    rules = []
    try:
        with open(filepath, 'r', newline='') as f:
            # Use QUOTE_ALL to specify that all fields are wrapped in quotes
            reader = csv.reader(f, quoting=csv.QUOTE_ALL)
            for row in reader:
                if not row: continue
                canonical_name = row[0]
                # The csv reader automatically handles removing the quotes
                keywords = [k.lower() for k in row[1:] if k]
                rules.append((keywords, canonical_name))
        if verbose: print(f"[i] Loaded {len(rules)} normalization rules from '{filepath}'.")
    except FileNotFoundError:
        if verbose: print(f"[!] Rules file not found at '{filepath}'. No rules loaded.")
    return rules

def normalize_publishers_batch_ai(publisher_list, prompt_template, verbose=False):
    """
    Normalizes a list of publisher names in a single batch API call.
    Returns a dictionary mapping original names to normalized names.
    """
    all_normalized_maps = {}
    chunk_size = 50  # Process 50 publishers per API call
    
    if not publisher_list:
        return all_normalized_maps

    if verbose:
        print(f"\n[i] Normalizing {len(publisher_list)} unique publisher(s) with AI in chunks of {chunk_size}...")

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={API_KEY}"
    headers = {'Content-Type': 'application/json'}
    
    # Split the list into chunks
    chunks = [publisher_list[i:i + chunk_size] for i in range(0, len(publisher_list), chunk_size)]
    
    for i, chunk in enumerate(chunks):
        if verbose:
            print(f"  - Processing chunk {i+1} of {len(chunks)}...")

        publisher_json_string = json.dumps(chunk)
        prompt = prompt_template.format(publisher_json_string=publisher_json_string)
        payload = {"contents": [{"parts": [{"text": prompt}]}]}

        try:
            # Increased timeout to 90 seconds for more resilience
            response = requests.post(api_url, json=payload, headers=headers, timeout=90)
            response.raise_for_status()
            result = response.json()
            ai_response_text = result['candidates'][0]['content']['parts'][0]['text'].strip()
            json_str = ai_response_text.strip('` \n').removeprefix('json').strip()
            normalized_map_chunk = json.loads(json_str)
            all_normalized_maps.update(normalized_map_chunk)
        except requests.exceptions.RequestException as e:
            if verbose: print(f"\n[!] API Error during batch normalization on chunk {i+1}: {e}")
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            if verbose: print(f"\n[!] Could not parse AI batch response for chunk {i+1}: {e}\n    Raw response was: {ai_response_text}")

    if verbose and all_normalized_maps:
        print("[i] AI normalization successful.")
        
    return all_normalized_maps

def extract_epub_metadata(file_path, verbose=False):
    """Extracts raw metadata from an EPUB file."""
    try:
        book = epub.read_epub(file_path)
        return {
            'title': book.get_metadata('DC', 'title')[0][0] if book.get_metadata('DC', 'title') else 'N/A',
            'authors': [author[0] for author in book.get_metadata('DC', 'creator')] if book.get_metadata('DC', 'creator') else [],
            'publisher': book.get_metadata('DC', 'publisher')[0][0] if book.get_metadata('DC', 'publisher') else 'N/A',
        }
    except Exception as e:
        if verbose: print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def extract_pdf_metadata(file_path, verbose=False):
    """Extracts raw metadata from a PDF file."""
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            meta = reader.metadata
            if not meta: return {'title': 'N/A', 'authors': [], 'publisher': 'N/A'}
            return {
                'title': meta.title or 'N/A',
                'authors': [meta.author] if meta.author else [],
                'publisher': meta.producer or 'N/A',
            }
    except Exception as e:
        if verbose: print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def process_file(file_path, verbose=False):
    """Processes a single file to extract its metadata for thread execution."""
    filename = os.path.basename(file_path)
    book_meta = None
    if filename.lower().endswith('.epub'):
        book_meta = extract_epub_metadata(file_path, verbose)
    elif filename.lower().endswith('.pdf'):
        book_meta = extract_pdf_metadata(file_path, verbose)
    
    if book_meta:
        book_meta['filename'] = filename
    return book_meta

def check_file_cache(file_path, cached_entry, verbose=False):
    """Checks a single file against the cache using modification time."""
    if verbose: print(f"\n  - Checking: {os.path.basename(file_path)}...")
    try:
        current_mtime = os.path.getmtime(file_path)
    except FileNotFoundError:
        if verbose: print(f"    File not found for cache check: {os.path.basename(file_path)}.")
        return ('MISS', file_path, None)

    if cached_entry and cached_entry.get('mtime') == current_mtime:
        if verbose: print(f"    Cache HIT for {os.path.basename(file_path)}.")
        return ('HIT', file_path, cached_entry['metadata'])
    else:
        if verbose: print(f"    Cache MISS for {os.path.basename(file_path)}.")
        return ('MISS', file_path, None)

def main():
    """Main function to find and process files, collecting their metadata."""
    args = sys.argv[1:]

    if '-h' in args or '--help' in args:
        show_help()
        return

    use_ai = '--ai' in args
    verbose = '-v' in args or '--verbose' in args
    force_reload = '--force-reload' in args
    args = [arg for arg in args if arg not in ('-v', '--verbose', '--force-reload', '--ai')]

    # Defaults
    num_threads = os.cpu_count() or 4
    prompt_filepath = DEFAULT_PROMPT_FILE
    output_filename = 'metadata-processed.json'
    rules_filepath = DEFAULT_RULES_FILE
    
    # Argument parsing
    positional_args = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ('-t', '--threads'):
            try:
                num_threads = int(args[i + 1])
                if num_threads <= 0: raise ValueError
                i += 2
            except (ValueError, IndexError):
                print(f"Error: {arg} requires a positive integer."); return
        elif arg in ('-p', '--prompt'):
            try:
                prompt_filepath = args[i + 1]
                i += 2
            except IndexError:
                print(f"Error: {arg} requires a file path argument."); return
        elif arg in ('-o', '--output'):
            try:
                output_filename = args[i + 1]
                i += 2
            except IndexError:
                print(f"Error: {arg} requires a file path argument."); return
        elif arg in ('-r', '--rules'):
            try:
                rules_filepath = args[i + 1]
                i += 2
            except IndexError:
                print(f"Error: {arg} requires a file path argument."); return
        else:
            positional_args.append(arg)
            i += 1

    target_directory = positional_args[0] if positional_args else os.getcwd()

    if not os.path.isdir(target_directory):
        print(f"Error: The specified path '{target_directory}' is not a valid directory."); return

    metadata_cache = load_cache(METADATA_CACHE_FILE)
    publisher_cache = load_cache(PUBLISHER_CACHE_FILE)
    rules = load_rules_from_csv(rules_filepath, verbose)
    
    try:
        all_files_in_dir = [os.path.join(target_directory, f) for f in os.listdir(target_directory) if f.lower().endswith(('.epub', '.pdf'))]
        if not all_files_in_dir:
            print(f"No .epub or .pdf files found in '{target_directory}'."); return

        raw_metadata_list = []
        files_to_process = []

        print("Step 1: Checking file cache...")
        if force_reload:
            print("  - Force reload enabled, skipping cache check.")
            files_to_process = all_files_in_dir
        else:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {executor.submit(check_file_cache, fp, metadata_cache.get(fp), verbose): fp for fp in all_files_in_dir}
                
                checked_count = 0
                total_files = len(all_files_in_dir)
                for future in as_completed(futures):
                    status, file_path, data = future.result()
                    if status == 'HIT':
                        raw_metadata_list.append(data)
                    else: # MISS
                        files_to_process.append(file_path)
                    
                    checked_count += 1
                    percent = (checked_count / total_files) * 100
                    bar = '█' * int(percent / 2) + '-' * (50 - int(percent / 2))
                    sys.stdout.write(f'\rChecking: |{bar}| {checked_count}/{total_files} ({percent:.1f}%)')
                    sys.stdout.flush()
            print("\nCache check complete.")

        total_to_process = len(files_to_process)
        if total_to_process > 0:
            print(f"Found {total_to_process} new or modified file(s). Processing with {num_threads} threads...")
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {executor.submit(process_file, f, verbose): f for f in files_to_process}
                processed_count = 0
                for future in as_completed(futures):
                    file_path = futures[future]
                    book_meta = future.result()
                    if book_meta:
                        raw_metadata_list.append(book_meta)
                        try:
                            mtime = os.path.getmtime(file_path)
                            metadata_cache[file_path] = { 'mtime': mtime, 'metadata': book_meta }
                        except FileNotFoundError:
                            if verbose: print(f"\n[!] Could not get mtime for {os.path.basename(file_path)}, file not found.")

                    processed_count += 1
                    percent = (processed_count / total_to_process) * 100
                    bar = '█' * int(percent / 2) + '-' * (50 - int(percent / 2))
                    sys.stdout.write(f'\rProgress: |{bar}| {processed_count}/{total_to_process} ({percent:.1f}%)')
                    sys.stdout.flush()
            print("\nExtraction complete.")
        else:
            print("No new or modified files to process. Loading all metadata from cache.")

        all_publishers = {meta['publisher'] for meta in raw_metadata_list if meta.get('publisher') and meta['publisher'] != 'N/A'}
        
        print("\nStep 2: Normalizing publisher names...")
        publisher_map = {}
        publishers_for_ai = []
        rules_normalized_count = 0
        
        for name in all_publishers:
            if name in publisher_cache:
                publisher_map[name] = publisher_cache[name]
                if name != publisher_cache[name]: # Count cached normalizations that were from rules
                    # This logic is imperfect but gives a decent estimate
                    lower_name = name.lower()
                    is_rule_match = False
                    for keywords, _ in rules:
                        if any(kw in lower_name for kw in keywords):
                            rules_normalized_count += 1
                            is_rule_match = True
                            break
                continue

            lower_name = name.lower()
            found_rule = False
            for keywords, canonical in rules:
                if any(kw in lower_name for kw in keywords):
                    publisher_map[name] = canonical
                    publisher_cache[name] = canonical
                    if name != canonical:
                        rules_normalized_count += 1
                    found_rule = True
                    break
            if not found_rule:
                publishers_for_ai.append(name)
        
        ai_results = {}
        ai_normalized_count = 0
        if publishers_for_ai:
            if use_ai:
                if not API_KEY:
                    print("[!] AI normalization skipped: GEMINI_API_KEY not set in .env file.")
                else:
                    try:
                        with open(prompt_filepath, 'r') as f:
                            prompt_template = f.read()
                        ai_results = normalize_publishers_batch_ai(publishers_for_ai, prompt_template, verbose)
                        publisher_map.update(ai_results)
                        publisher_cache.update(ai_results)
                        ai_normalized_count = sum(1 for original, normalized in ai_results.items() if original != normalized)
                    except FileNotFoundError:
                        print(f"\n[!] Prompt file not found at '{prompt_filepath}'. Skipping AI normalization.")
            elif verbose:
                 print(f"[i] {len(publishers_for_ai)} publisher(s) could be normalized. Run with --ai to enable.")
        
        print("Normalization complete.")
        
        print("\n--- Normalization Stats ---")
        print(f"Publishers normalized by rules: {rules_normalized_count}")
        if use_ai:
            print(f"Publishers normalized by AI:    {ai_normalized_count}")
        print("---------------------------")


        print("\nStep 3: Assembling final results...")
        final_metadata = {}
        for raw_meta in raw_metadata_list:
            publisher = raw_meta['publisher']
            normalized = publisher_map.get(publisher, publisher)
            
            if verbose and publisher != normalized:
                source = "(AI)" if publisher in ai_results else "(rule)"
                print(f"  [v] Normalized '{raw_meta['filename']}': '{publisher}' -> '{normalized}' {source}")

            final_metadata[raw_meta['filename']] = {
                'title': raw_meta['title'],
                'authors': raw_meta['authors'],
                'publisher': publisher,
                'publisher_normalized': normalized
            }
        print("Assembly complete.")
        
        try:
            with open(output_filename, 'w') as f:
                json.dump(final_metadata, f, indent=2)
            print(f"\n--- All Collected Metadata written to {output_filename} ---")
        except IOError as e:
            print(f"\n[!] Error writing to output file '{output_filename}': {e}")

    except KeyboardInterrupt:
        print("\n\n[!] Keyboard interrupt received. Exiting gracefully.")
        sys.exit(130)
    finally:
        print("\nSaving caches...")
        save_cache(metadata_cache, METADATA_CACHE_FILE)
        save_cache(publisher_cache, PUBLISHER_CACHE_FILE)
        print("Done.")

if __name__ == "__main__":
    main()


