import os
import sys
import json
from PyPDF2 import PdfReader
from ebooklib import epub
import ebooklib

def get_epub_metadata(file_path, verbose=False):
    """
    Extracts title, author, and publisher metadata from an EPUB file.
    """
    try:
        book = epub.read_epub(file_path)
        metadata = {
            'title': book.get_metadata('DC', 'title')[0][0] if book.get_metadata('DC', 'title') else 'N/A',
            'authors': [author[0] for author in book.get_metadata('DC', 'creator')] if book.get_metadata('DC', 'creator') else [],
            'publisher': book.get_metadata('DC', 'publisher')[0][0] if book.get_metadata('DC', 'publisher') else 'N/A'
        }
        return metadata
    except Exception as e:
        if verbose:
            # Print a newline to avoid overwriting the progress bar, then show the error
            print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def get_pdf_metadata(file_path, verbose=False):
    """
    Extracts title, author, and producer (as publisher) metadata from a PDF file.
    """
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            meta = reader.metadata
            if not meta:
                return {'title': 'N/A', 'authors': [], 'publisher': 'N/A'}
            
            authors = [meta.author] if meta.author else []

            metadata = {
                'title': meta.title or 'N/A',
                'authors': authors,
                'publisher': meta.producer or 'N/A' # Using producer as a substitute for publisher
            }
            return metadata
    except Exception as e:
        if verbose:
            # Print a newline to avoid overwriting the progress bar, then show the error
            print(f"\n[!] Error processing {os.path.basename(file_path)}: {e}")
        return None

def main():
    """
    Main function to find and process files, collecting their metadata.
    Accepts an optional command-line argument for the target directory
    and a verbose flag (-v or --verbose) to show errors.
    """
    args = sys.argv[1:]
    verbose = '-v' in args or '--verbose' in args

    # Remove verbose flags to find the directory path argument
    args = [arg for arg in args if arg not in ('-v', '--verbose')]

    if args:
        target_directory = args[0]
    else:
        target_directory = os.getcwd()

    if not os.path.isdir(target_directory):
        print(f"Error: The specified path '{target_directory}' is not a valid directory.")
        return

    print(f"Scanning for files in: {target_directory}")

    all_metadata = {}
    
    files_to_process = [f for f in os.listdir(target_directory) if f.lower().endswith(('.epub', '.pdf'))]
    total_files = len(files_to_process)

    if total_files == 0:
        print(f"No .epub or .pdf files found in '{target_directory}'.")
        return

    for i, filename in enumerate(files_to_process):
        file_path = os.path.join(target_directory, filename)
        
        book_meta = None
        if filename.lower().endswith('.epub'):
            book_meta = get_epub_metadata(file_path, verbose)
        elif filename.lower().endswith('.pdf'):
            book_meta = get_pdf_metadata(file_path, verbose)
        
        if book_meta:
            all_metadata[filename] = book_meta
        
        processed_count = i + 1
        percent = (processed_count / total_files) * 100
        bar_length = 40
        filled_length = int(bar_length * processed_count // total_files)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        # Write progress bar. Error messages will appear on separate lines.
        sys.stdout.write(f'\rProgress: |{bar}| {processed_count}/{total_files} ({percent:.1f}%)')
        sys.stdout.flush()

    print() # Final newline after progress bar completes

    print("\n--- All Collected Metadata ---")
    print(json.dumps(all_metadata, indent=2))

if __name__ == "__main__":
    main()


