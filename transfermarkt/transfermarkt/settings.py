# Scrapy settings for transfermarkt project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "transfermarkt"

SPIDER_MODULES = ["transfermarkt.spiders"]
NEWSPIDER_MODULE = "transfermarkt.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = "transfermarkt (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    "transfermarkt.middlewares.TransfermarktSpiderMiddleware": 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    "transfermarkt.middlewares.TransfermarktDownloaderMiddleware": 543,
# }
# Enable the scrapy-user-agents middleware
DOWNLOADER_MIDDLEWARES = {
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
    # Optionally, disable Scrapy's default User-Agent middleware
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    "transfermarkt.pipelines.TransfermarktPipeline": 300,
# }

ITEM_PIPELINES = {
    "transfermarkt.pipelines.LeaguePipeline": 300,
    "transfermarkt.pipelines.TeamPipeline": 301,
    "transfermarkt.pipelines.TeamDetailsPipeline": 302,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 10
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Enable logging
LOG_ENABLED = True

# Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = 'DEBUG'

# Set log file path
LOG_FILE = 'scrapy_log.txt'

# Optional: Set log file encoding
LOG_ENCODING = 'utf-8'

# Optional: Set log format
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'

# Optional: Set log dateformat
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'


FEEDS = {
    'leagues.json': {
        'format': 'json',
        'encoding': 'utf8',
        'store_empty': False,
        'item_classes': ['transfermarkt.items.LeagueItem'],
        'fields': None,
        'indent': 4,
    },
    'teams.json': {
        'format': 'json',
        'encoding': 'utf8',
        'store_empty': False,
        'item_classes': ['transfermarkt.items.TeamItem'],
        'fields': None,
        'indent': 4,
    },
    'teams_details.json': {
        'format': 'json',
        'encoding': 'utf8',
        'store_empty': False,
        'item_classes': ['transfermarkt.items.TeamDetailsItem'],
        'fields': None,
        'indent': 4,
    }
}
