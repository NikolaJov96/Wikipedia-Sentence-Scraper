import os
import time

from sentence_finder import SentenceFinder
from wikipedia_scraper import WikipediaScraper


if __name__ == '__main__':
    """
    Scrape Wikipedia on a desired language and parse articles for desired data
    """

    language = 'en'

    # Prepare folders
    log_directory = 'log'
    if not os.path.exists(log_directory):
        os.mkdir(log_directory)
    language_directory = os.path.join(log_directory, language)
    if not os.path.exists(language_directory):
        os.mkdir(language_directory)

    # Initialize the scraper
    wikipedia_scraper = WikipediaScraper(
        language=language,
        num_download_workers=8,
        log_dir=os.path.join(language_directory, 'scraper_logs')
    )

    # Initialize the parser
    parsing_workers = 8
    sentence_finder = SentenceFinder(
        language=language,
        num_parsing_workers=parsing_workers,
        output_dir=os.path.join(language_directory, 'parser_output')
    )

    # Iterate through articles and look for sentences
    for response, soup, title in wikipedia_scraper:
        if title is None:
            # Next page not ready, wait a bit
            time.sleep(0.1)
            continue

        sentence_finder.find_sentences(title, response.content)

    # Clean up after the whole Wikipedia is analyzed
    sentence_finder.kill_jobs()
    wikipedia_scraper.kill_jobs()
