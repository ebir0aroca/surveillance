try:
  import pandas as pd  
  import datetime
  import os
  import csv

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
    exit()
    
    
class Application:
  # From github to local
  #   data/transformers/categories_translate.csv
  #   data/transformers/store_brands.csv
  # 
  # From Drive
  #   root/transform_logs/log.csv
  #   root/out/complete_{date}/dbs/
  #   root/out/complete_{date}/logs/
  #   root/out/complete_{date}/logs/output-D({date}})-T({time}).json
  #
  SCRAP_PATH = ""   
  ROOT_PATH = ""
  LOG_FILEPATH = ""
  TRANSFORMERS_PATH = ""
  DBS_PATH = ""
  MAIN_DB_FILEPATH = ""
  log_problems = True
  overwrite_main_database = True
  

  def __init__(self, root_path, scrap_path, main_database_filename):
    self.ROOT_PATH = root_path
    self.SCRAP_PATH = scrap_path

    self.LOG_FILEPATH = os.path.join(self.ROOT_PATH, 'transform_logs', 'log.csv')
    self.TRANSFORMERS_PATH = os.path.join(self.ROOT_PATH, "data/transformers/") 
    self.DBS_PATH = os.path.join(self.SCRAP_PATH, 'dbs')
    self.MAIN_DB_FILEPATH = os.path.join(self.DBS_PATH, main_database_filename)

  def categories_translate_transf(self): 
    return pd.read_csv(os.path.join(self.TRANSFORMERS_PATH, "categories_translate.csv"))  

  def store_brands_list(self):
    #Categories transformer configuration file loaded by:  Marketplace, Country, Language
    store_brands_transf = pd.read_csv(os.path.join(self.TRANSFORMERS_PATH, "store_brands.csv"))  
    return store_brands_transf['storebrand_name'].values.tolist()


  def create_infrastucture(self):
    if self.overwrite_main_database:
      if os.path.exists(self.MAIN_DB_FILEPATH):
        os.remove(self.MAIN_DB_FILEPATH)
      else:
        print("Cannot delete as it doesn't exists")
    
    #@markdown After execution creates folder 'dbs' if not exists
    if not os.path.isdir(self.DBS_PATH):
      os.mkdir(self.DBS_PATH)
    # deletes the main database if required
    # creates the main database if not exists
    if not os.path.exists(self.MAIN_DB_FILEPATH):
        main_database = pd.DataFrame()
        print(f'Database file created: {self.MAIN_DB_FILEPATH}')
        main_database.to_csv(self.MAIN_DB_FILEPATH)

  def log_error(error):
    el = [datetime.datetime.now().strftime('%D %T'), error]
    with open(self.LOG_FILEPATH, 'a', newline='') as f:
        cw = csv.writer(f)
        cw.writerow(el)
        
  def get_scrap_filelist(self):
    scrap_list = []
    for scrap_file_name in next(os.walk(self.SCRAP_PATH), (None, None, []))[2]:
        scrap_list.append(os.path.join(self.SCRAP_PATH, scrap_file_name)) 
    return scrap_list

SCRAP_META = [['scrap_meta', 'guid'], ['scrap_meta', 'date_start'],
            ['scrap_meta', 'maincategory_url'], ['scrap_meta', 'spider_country'],
            ['scrap_meta', 'spider_date_end'], ['scrap_meta', 'spider_marketplace'],
            ['scrap_meta', 'spider_name'], ['scrap_meta', 'spider_version'],
            ['scrap_meta', 'title'], ['scrap_meta', 'spider_date_start']]



