import datetime
import csv

SCRAP_META = [['scrap_meta', 'guid'], ['scrap_meta', 'date_start'],
            ['scrap_meta', 'maincategory_url'], ['scrap_meta', 'spider_country'],
            ['scrap_meta', 'spider_date_end'], ['scrap_meta', 'spider_marketplace'],
            ['scrap_meta', 'spider_name'], ['scrap_meta', 'spider_version'],
            ['scrap_meta', 'title'], ['scrap_meta', 'spider_date_start']]

def log_error(LOG_FILEPATH, error):
  el = [datetime.datetime.now().strftime('%D %T'), error]
  with open(LOG_FILEPATH, 'a', newline='') as f:
      cw = csv.writer(f)
      #print(Fore.RED + error)
      cw.writerow(el)
