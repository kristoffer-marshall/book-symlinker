# **Ebook Metadata Extractor & Symlinker**

This Python script is a powerful command-line tool designed to scan a directory of .epub and .pdf files, extract their metadata, intelligently normalize publisher names, and create an organized library of symlinks for easy browsing.  
It uses a multi-threaded approach for speed, caches results to make subsequent runs faster, and can optionally leverage the Google Gemini AI to normalize publisher names that don't match local rules.

## **Features**

* **Metadata Extraction:** Reads Title, Author(s), and Publisher from .epub and .pdf files.  
* **Intelligent Caching:** Caches file metadata and publisher normalizations, only re-processing files that have been modified.  
* **Multi-threaded Processing:** Uses multiple threads to check the cache and process files concurrently, significantly speeding up operations on large libraries.  
* **Publisher Normalization:**  
  * Uses a customizable publisher\_rules.csv for fast, local normalization of common publisher names.  
  * Optionally uses the **Google Gemini AI** to normalize publishers that are not found in the local rules.  
* **Title Sanitization:** Automatically detects and replaces junk titles (e.g., "Microsoft Word \- Document1.docx") with the book's filename.  
* **Symlink Organization:** Creates a structured library of symlinks in by\_title/ and by\_publisher/ directories, allowing you to browse your collection by title or by normalized publisher name.  
* **Flexible & Safe:**  
  * Includes a test mode (--symlink-test) to preview changes without creating any files.  
  * Supports both relative (default) and absolute symlinks.  
  * Gracefully handles interruptions (Ctrl+C) by saving cache files before exiting.

## **Prerequisites**

Before running the script, you need to install the required Python libraries.  
pip install PyPDF2 ebooklib python-dotenv requests

## **Configuration**

The script uses a few external files for configuration.

### **1\. API Key (Optional)**

If you plan to use the AI normalization feature (--ai), you must provide a Google Gemini API key.

1. Create a file named .env in the same directory as the script.  
2. Add your API key to this file:  
   GEMINI\_API\_KEY="YOUR\_API\_KEY\_HERE"

   *Note: The .env file is included in .gitignore to prevent you from accidentally committing your key to version control.*

### **2\. Publisher Rules**

Create a publisher\_rules.csv file to define your static normalization rules. This is much faster than using the AI for common publishers.

* The first column is the **normalized (canonical)** name.  
* All subsequent columns on the same row are the variations that should be mapped to the canonical name.  
* All values must be enclosed in double quotes.

**Example publisher\_rules.csv:**  
"O'Reilly Media","O'Reilly","O’Reilly Media, Inc."  
"Packt Publishing","Packt","Packt Pub","Packt publishing"  
"Unknown Publisher","N/A","null","Adobe Acrobat"

### **3\. AI Prompt (Optional)**

You can customize the prompt sent to the Gemini AI by editing the prompt.txt file.

## **Usage**

Run the script from your terminal, pointing it to the directory containing your e-books.

### **Basic Usage**

To scan the current directory and create symlinks:  
python read\_meta.py

To scan a specific directory:  
python read\_meta.py /path/to/your/ebooks

### **Command-Line Options**

| Flag | Short | Description |
| :---- | :---- | :---- |
| \[directory\] |  | Optional path to the directory to scan. |
| \--help | \-h | Show the help message and exit. |
| \--threads NUMBER | \-nt | Set the number of threads for processing. |
| \--prompt FILE | \-p | Path to a custom prompt file for the AI. |
| \--output FILE | \-o | Path to the output JSON file (default: metadata-processed.json). |
| \--rules FILE | \-r | Path to a custom publisher normalization CSV file. |
| \--symlink-test | \-s | **Dry run.** Show what symlinks would be created without making changes. |
| \--verbose | \-v | Enable verbose output for detailed progress and errors. |
| \--absolute |  | Create absolute symlinks instead of relative ones. |
| \--force-reload |  | Ignore the file metadata cache and re-read all files. |
| \--force-normalize |  | Ignore the publisher cache and re-normalize all publishers. |
| \--ai |  | **Enable AI** to normalize publisher names (requires API key). |

### **Example Workflow**

1. **Initial Run (Dry Run):**  
   * See what the script will do without making changes.  
   * Run with \--force-reload and \--force-normalize the first time to build caches from scratch.

python read\_meta.py /path/to/books \-v \-s \--force-reload \--force-normalize

2. **AI Normalization (Dry Run):**  
   * Add the \--ai flag to see how the AI would normalize the remaining publishers.

python read\_meta.py /path/to/books \-v \-s \--ai

3. **Live Run:**  
   * Once you're happy with the test output, remove the \-s flag to create the symlinks and generate the final JSON output.

python read\_meta.py /path/to/books \-v \--ai

4. **Subsequent Runs:**  
   * The script will now be very fast, as it will only process new or changed files.

python read\_meta.py /path/to/books  
