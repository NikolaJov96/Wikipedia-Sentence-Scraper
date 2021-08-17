import multiprocessing
import os
import pickle
import time
import requests

from bs4 import BeautifulSoup


class WikipediaScraper:
    """
    Iterator class for scraping wikipedia pages on a given language, featuring parallel page downloading
    """

    BASE_WIKIPEDIA_URL = 'https://{}.wikipedia.org'
    WIKI_PAGE_TAG = '/wiki/'
    DOWNLOADED_RESPONSES_TO_BUFFER = 100
    PROGRESS_FILES = ['progress1', 'progress2']

    def __init__(self, initial_url=None, language='en', num_download_workers=1, log_dir=None):
        """
        Initialize parameters and member objects
        """

        # Parallel download worker management
        self.proc_manager = multiprocessing.Manager()
        self.num_download_workers = num_download_workers
        self.page_download_jobs = []
        self.progress_log_job = None
        self.process_lock = multiprocessing.Lock()

        # Base wikipedia url in a given language
        self.wikipedia_url = WikipediaScraper.BASE_WIKIPEDIA_URL.format(language)
        # Url to start the scraping from
        self.initial_url = initial_url if initial_url is not None else self.wikipedia_url
        # Links to pages waiting to be downloaded
        self.links_to_visit = self.proc_manager.list()
        # Links to pages processed or currently being processed by a download worker
        self.visited_links = self.proc_manager.list()
        # Pages currently being downloaded
        self.currently_downloading = self.proc_manager.list()
        # Cached downloaded pages
        self.downloaded_responses = self.proc_manager.dict()

        # Path to the progress save directory
        self.log_dir = log_dir
        if self.log_dir is not None and not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)

    def __del__(self):
        """
        Stop all spawned processes
        """

        self.kill_jobs()

    def __iter__(self):
        """
        Initializes iteration through the wikipedia pages and spawns download and logging processes
        """

        # Kill potential existing download workers
        self.kill_jobs()
        self.page_download_jobs = []
        self.progress_log_job = None

        # Clear link buffers
        self.links_to_visit[:] = []
        self.visited_links[:] = []
        self.currently_downloading[:] = []
        self.downloaded_responses.clear()

        # Check for the saved progress and load it
        if self.log_dir is None or not self.load_progress_file():
            self.links_to_visit.append(self.initial_url)

        # Initialize new download worker processes
        for i in range(self.num_download_workers):
            p = multiprocessing.Process(
                target=WikipediaScraper.page_download_worker,
                args=(self.links_to_visit, self.visited_links, self.currently_downloading,
                      self.downloaded_responses, self.process_lock))
            self.page_download_jobs.append(p)
            p.start()

        # Initialize progress log saving process
        if self.log_dir is not None:
            self.progress_log_job = multiprocessing.Process(
                target=WikipediaScraper.progress_log_worker,
                args=(self.links_to_visit, self.visited_links, self.currently_downloading,
                      self.log_dir, self.process_lock))
            self.progress_log_job.start()

        return self

    def __next__(self):
        """
        Generate the next http response, page soup and page title tuple
        """

        # Lock access to the shared variables while checking them
        self.process_lock.acquire()

        # Check if the whole wikipedia has been processed and terminate the iteration
        if len(self.links_to_visit) == 0 and \
                len(self.currently_downloading) == 0 and \
                len(self.downloaded_responses) == 0:
            self.kill_jobs()
            self.process_lock.release()
            raise StopIteration

        # Check if pages will be available but are still being downloaded and return none
        if len(self.downloaded_responses) == 0:
            self.process_lock.release()
            return None, None, None

        # Get a response from non-empty download buffer
        url = list(self.downloaded_responses.keys())[0]
        response = self.downloaded_responses[url]
        del self.downloaded_responses[url]

        # Done with shared variable access
        self.process_lock.release()

        # Create the page soup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find new links to scrape
        self.parse_links(soup)

        return response, soup, soup.find(id="firstHeading").text

    def kill_jobs(self):
        """
        Terminates all active downloading jobs, should be called on the iterator when no longer needed
        """

        for p in self.page_download_jobs:
            p.kill()

        if self.progress_log_job is not None:
            self.progress_log_job.kill()

    @staticmethod
    def page_download_worker(links_to_visit, visited_links, currently_downloading, downloaded_responses, process_lock):
        """
        Static method representing page download worker
        """

        while True:

            # Lock while checking and getting available links to visit
            process_lock.acquire()

            if len(links_to_visit) > 0 and len(downloaded_responses) < WikipediaScraper.DOWNLOADED_RESPONSES_TO_BUFFER:
                # Links available, get one and start processing it
                url = links_to_visit.pop()
                currently_downloading.append(url)
                visited_links.append(url)
                # Done with shared variable access
                process_lock.release()

                # Try downloading it, should never fail unless wikipedia website changes its structure
                try:
                    response = requests.get(url=url)
                    downloaded_responses[url] = response
                except requests.exceptions.ConnectionError:
                    print('error getting', url)

                # Lock while removing url from currently downloading
                process_lock.acquire()
                currently_downloading.remove(url)
                process_lock.release()
            else:
                # Unlock the previous lock
                process_lock.release()

                # Sleep a bit if no links are available
                time.sleep(0.1)

    def parse_links(self, soup):
        """
        Finds valid, not already visited wikipedia page links in the soup, to be downloaded later
        """

        # Find links and filter out ones that do not represent valid pages
        valid_wiki_page_sub_links = list(filter(
            lambda link:
                link.has_attr('href') and
                link['href'].startswith(WikipediaScraper.WIKI_PAGE_TAG) and
                ':' not in link['href'],
            soup.find(id="bodyContent").find_all("a")))

        # Found links are relative, so add the base wikipedia url in front
        valid_wiki_page_links = [self.wikipedia_url + link['href'] for link in valid_wiki_page_sub_links]

        # Add the not already encountered ones
        self.process_lock.acquire()
        self.links_to_visit += [
            link for link in valid_wiki_page_links if
            link not in self.visited_links and
            link not in self.currently_downloading and
            link not in self.downloaded_responses.keys()
        ]
        self.process_lock.release()

    @staticmethod
    def progress_log_worker(links_to_visit, visited_links, currently_downloading, log_dir, process_lock):
        """
        Worker for constant updating of the progress logs
        """

        # Get alternating log file names
        progress_files = [os.path.join(log_dir, file_name) for file_name in WikipediaScraper.PROGRESS_FILES]
        next_progress_file_id = 0

        time.sleep(10.0)

        while True:

            progress_file = progress_files[next_progress_file_id]
            next_progress_file_id = (next_progress_file_id + 1) % len(progress_files)

            # Lock while copying data to store
            process_lock.acquire()
            copy_links_to_visit = list(links_to_visit) + list(currently_downloading)
            copy_visited_links = list(visited_links)
            process_lock.release()

            # Store the progress data
            with open(progress_file, 'wb') as out_file:
                progress = {
                    'timestamp': time.time(),
                    'links_to_visit': copy_links_to_visit,
                    'visited_links': copy_visited_links
                }
                pickle.dump(progress, out_file)

            # Give other processes time to bread
            time.sleep(10.0)

    def load_progress_file(self):
        """
        Loads progress from saved file if it exists, returns success flag
        """

        if self.log_dir is None:
            return False

        progress_files = [os.path.join(self.log_dir, file_name) for file_name in WikipediaScraper.PROGRESS_FILES]

        # Load all available progress files
        progresses = []
        for progress_file in progress_files:
            try:
                with open(progress_file, 'rb') as in_file:
                    progress = pickle.load(in_file)
                progresses.append(progress)
            except EOFError:
                pass
            except FileNotFoundError:
                pass

        if len(progresses) == 0:
            return False

        # Get the freshest one
        newest_progress = sorted(progresses, key=lambda p: p['timestamp'], reverse=True)[0]

        # Load the progress
        self.links_to_visit[:] = newest_progress['links_to_visit']
        self.visited_links[:] = newest_progress['visited_links']
        print('scraper progress file loaded, links_to_visit: {}, visited_links: {}'
              .format(len(self.links_to_visit), len(self.visited_links)))

        return True


if __name__ == '__main__':
    """
    Quick test of the scraper, print out titles of pages scraped in 10 seconds
    """

    wikipedia_scraper = WikipediaScraper(num_download_workers=4)
    start_time = time.time()
    pages_count = 0
    for _, _, title in wikipedia_scraper:
        if title is not None:
            print(title)
            pages_count += 1
        else:
            time.sleep(0.01)
        if time.time() - start_time > 10:
            break
    wikipedia_scraper.kill_jobs()
    print('pages visited: {}'.format(pages_count))
