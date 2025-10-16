import os
import json
from PyPDF2 import PdfReader
from ebooklib import epub
import ebooklib

def get_epub_metadata(file_path):
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
        print(f"  Could not process EPUB {os.path.basename(file_path)}: {e}")
        return None

def get_pdf_metadata(file_path):
    """
    Extracts title, author, and producer (as publisher) metadata from a PDF file.
    """
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            meta = reader.metadata
            if not meta:
                return {'title': 'N/A', 'authors': [], 'publisher': 'N/A'}
            
            # PDF metadata fields can be inconsistent; we fall back gracefully.
            authors = [meta.author] if meta.author else []

            metadata = {
                'title': meta.title or 'N/A',
                'authors': authors,
                'publisher': meta.producer or 'N/A' # Using producer as a substitute for publisher
            }
            return metadata
    except Exception as e:
        print(f"  Could not process PDF {os.path.basename(file_path)}: {e}")
        return None

def main():
    """
    Main function to find and process files, collecting their metadata.
    """
    current_directory = os.getcwd()
    print(f"Scanning for files in: {current_directory}\n")

    all_metadata = {}

    for filename in os.listdir(current_directory):
        file_path = os.path.join(current_directory, filename)
        
        book_meta = None
        if filename.lower().endswith('.epub'):
            book_meta = get_epub_metadata(file_path)
        elif filename.lower().endswith('.pdf'):
            book_meta = get_pdf_metadata(file_path)
        
        if book_meta:
            all_metadata[filename] = book_meta

    print("\n--- All Collected Metadata ---")
    # Use json.dumps for a clean, readable printout of the final dictionary
    print(json.dumps(all_metadata, indent=2))


if __name__ == "__main__":
    main()


