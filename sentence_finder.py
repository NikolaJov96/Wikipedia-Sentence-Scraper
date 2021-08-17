import multiprocessing
import os
import re
import time

from bs4 import BeautifulSoup


class SentenceFinder:
    """
    Class for parallel sentence extraction from the web page content
    """

    def __init__(self, language='en', num_parsing_workers=1, output_dir=None):
        """
        Initialize parameters and member objects and spawn parser processes
        """

        # Parallel parsing worker management
        self.proc_manager = multiprocessing.Manager()
        self.num_parsing_workers = num_parsing_workers
        self.parser_jobs = []
        self.process_lock = multiprocessing.Lock()

        # Received pages for parsing
        self.available_pages = self.proc_manager.list()

        # Get the appropriate sentence matcher for the language
        self.sentence_matcher = r'.*'
        if language == 'en':
            self.sentence_matcher = r'(last|took|take).* (day)'
        elif language == 'sr':
            self.sentence_matcher = r'(traja|траја).* (dan|дан)'

        # Output log dir
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

        # Start the parsing processes
        for i in range(self.num_parsing_workers):
            output_file = os.path.join(self.output_dir, 'w{}.txt'.format(i)) if self.output_dir is not None else None
            p = multiprocessing.Process(
                target=SentenceFinder.parsing_worker,
                args=(i, output_file, self.available_pages, self.process_lock, self.sentence_matcher))
            self.parser_jobs.append(p)
            p.start()

    def find_sentences(self, title, response_content):
        """
        Add new page to be parsed
        """

        self.process_lock.acquire()
        self.available_pages.append((title, response_content))
        self.process_lock.release()

    def kill_jobs(self):
        """
        Terminates all active parsing jobs, should be called on the sentence finder when no longer needed
        """

        for p in self.parser_jobs:
            p.kill()

    @staticmethod
    def parsing_worker(worker_id, output_file, available_pages, process_lock, sentence_matcher):
        """
        Worker for fetching available pages and parsing sentences
        """

        pages_visited = 0
        while True:
            # Lock while checking and getting available pages
            process_lock.acquire()

            if len(available_pages) > 0:
                title, response_content = available_pages.pop()
                # Done with shared variable access
                process_lock.release()

                # Get the article content
                soup = BeautifulSoup(response_content, 'html.parser')
                content_text = soup.find(id="mw-content-text").text

                # Replace blanks with spaces
                content_text = re.sub('\n', ' ', content_text)
                content_text = re.sub('\r', ' ', content_text)
                content_text = re.sub('\t', ' ', content_text)

                # Remove consecutive spaces
                content_text = content_text.strip()
                content_text = re.sub(' +', ' ', content_text)

                # Find all sentences
                sentences = re.findall(r'([A-Z][^.!?]*[.!?])', content_text)
                # Find all appropriate sentences
                sentences = list(filter(lambda s: re.search(sentence_matcher, s, flags=re.I), sentences))

                pages_visited += 1

                # Log progress to the console
                print('{} - {} - {}'.format(worker_id, pages_visited, title))
                for sentence in sentences:
                    print(sentence)

                # Log to the output file
                if output_file is not None and len(sentences) > 0:
                    with open(output_file, 'ab') as out_file:
                        for sentence in sentences:
                            # Support non latin alphabets
                            line = '{} - {}\n'.format(title, sentence)
                            out_file.write(line.encode('utf8'))
            else:
                # Unlock the previous lock
                process_lock.release()

                # Sleep a bit if no soups are available
                time.sleep(0.1)
