import datetime
import csv

def log_error(LOG_FILEPATH, error):
  el = [datetime.datetime.now().strftime('%D %T'), error]
  with open(LOG_FILEPATH, 'a', newline='') as f:
      cw = csv.writer(f)
      #print(Fore.RED + error)
      cw.writerow(el)
