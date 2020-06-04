from multiprocessing.dummy import Pool as ThreadPool

class TheWireScrapper:

    def __init__(self):
        self.url ="https://thewire.in/wp-json/thewire/v2/posts/more/?typename=editors-pick&per_page={PER_PAGE}&page={PAGE_NUM}&type=opinion"
