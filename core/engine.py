try:
  import pandas as pd  
  import numpy as np  
  import datetime
  import os
  import csv
  import re

except ModuleNotFoundError as m_error:
    print(str(m_error))
    print('please install the required module and try again...')
    input('press enter to exit....')
    exit()
    
from enum import Enum
class Error_Level(Enum):
  Informative = 0
  Warning = 1
  Critical = 2


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
  SCRAP_FOLDER = ""
  SCRAP_PATH = ""   
  ROOT_DATA_PATH = ""
  ROOT_CODE_PATH = ""
  LOG_FILEPATH = ""
  TRANSFORMERS_PATH = ""
  DBS_PATH = ""
  MAIN_DB_FILENAME = "main_database.csv"
  MAIN_DB_FILEPATH = ""
  OVERWRITE_MAIN_DB = True
  SHOW_ERROR_LEVEL = Error_Level.Warning #equals and greater
  main_database = pd.DataFrame()
  
  
  SCRAP_META = [['scrap_meta', 'guid'], ['scrap_meta', 'date_start'],
            ['scrap_meta', 'maincategory_url'], ['scrap_meta', 'spider_country'],
            ['scrap_meta', 'spider_date_end'], ['scrap_meta', 'spider_marketplace'],
            ['scrap_meta', 'spider_name'], ['scrap_meta', 'spider_version'],
            ['scrap_meta', 'title'], ['scrap_meta', 'spider_date_start']]


  # Product Data model
  PRODUCT_DATAMODEL = {
                "sku":str,
                "title":str,
                "category":str,
                "source_category_url":str,
                "product_pos_in_page":int,
                "product_page":int,
                "product_url":str,
                "confs":str,
                "hasVariants":bool,
                "reviews_rating":float,
                "reviews_count":int,
                "currency":str,
                "img_url":str,
                "img_urls":str,
                "seller":str,  
                "price":float,
                "EAN":str,
                "description":str,
                "isAvailableInShop":bool,
                "isConfigurable":bool,
                "isStoreBrand":bool,  
                "isAvailableOnline":bool,
                "isSpecialPrice":bool,
                "onlineShippingCost":str,
                "onlineShippingLeadtime":str,
                "clickCollectLeadtime":str,
                "clickAndCollectState":str,
                "clickAndCollectAvailableQuantity":str,
                "deliveryTimeText":str,
                "specialPrice":float,
                "brand":str,
                "reviews":str,
                "scrap_meta.guid":str,
                "scrap_meta.maincategory_url":str,
                "scrap_meta.spider_country":str,
                "scrap_meta.spider_date_start":str,
                "scrap_meta.spider_date_end":str,
                "scrap_meta.spider_marketplace":str,
                "scrap_meta.spider_name":str,
                "scrap_meta.spider_version":str,
                "scrap_meta.title":str
  }
  #"cct":float  

  def __init__(self):
    pass
  
  def init(self):
    self.LOG_FILEPATH = os.path.join(self.ROOT_DATA_PATH, 'transform_logs', 'log.csv')
    self.TRANSFORMERS_PATH = os.path.join(self.ROOT_CODE_PATH, "data/transformers/") 
    self.SCRAP_PATH = os.path.join(self.ROOT_DATA_PATH, self.SCRAP_FOLDER) 
    self.DBS_PATH = os.path.join(self.SCRAP_PATH, 'dbs')
    self.MAIN_DB_FILEPATH = os.path.join(self.DBS_PATH, self.MAIN_DB_FILENAME)
  
  
  def remove_chars(s):
    s2 = re.sub(r'[^0-9.,]+', '', s)
    return s2.replace(",", ".")
  
  def get_categories_translate_transf(self): 
    return pd.read_csv(os.path.join(self.TRANSFORMERS_PATH, "categories_translate.csv"))  

  def get_store_brands_list(self):
    #Categories transformer configuration file loaded by:  Marketplace, Country, Language
    store_brands_transf = pd.read_csv(os.path.join(self.TRANSFORMERS_PATH, "store_brands.csv"))  
    return store_brands_transf['storebrand_name'].values.tolist()


  def create_infrastucture(self):
    if self.OVERWRITE_MAIN_DB:
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
        self.main_database = pd.DataFrame()
        self.main_database.to_csv(self.MAIN_DB_FILEPATH)
        print(f'Database file created: {self.MAIN_DB_FILEPATH}')

  def add_log_error(self, error, level):
    # 0: informative
    # 1: mistakes
    # 2: error
    # 3: critical
    el = [level, datetime.datetime.now().strftime('%D %T'), error]
    with open(self.LOG_FILEPATH, 'a', newline='') as f:
        cw = csv.writer(f)
        cw.writerow(el)
    if(level.value>=self.SHOW_ERROR_LEVEL.value):
        print(error)
        
        
  def get_scrap_filelist(self):
    scrap_list = []
    for scrap_file_name in next(os.walk(self.SCRAP_PATH), (None, None, []))[2]:
        scrap_list.append(os.path.join(self.SCRAP_PATH, scrap_file_name)) 
    return scrap_list

  def translate_categories(self, scrap_df, scrap_filepath, categories_translate_tf): 
    # 1.1 Splits Breadcrumbs into categories for a given scrap dataframe  
    for i, row in scrap_df.iterrows():
      if(type(scrap_df.at[i,'breadcrumbs'])==list):
          for j in range(len(scrap_df.at[i,'breadcrumbs'])):
            scrap_df.at[i,'category'+str(j)] = scrap_df.at[i,'breadcrumbs'][j]


    # 1.2. Translate categories from different Marketplace, Country and Language
    # marketplace                     (e.g. hornbach)
    #   country                       (e.g. ch)
    #     target_col_name             (e.g. category)
    #       target_col_value          (e.g. Illuminated bathroom mirror)
    # 			  rule_name               (e.g. R48)
    #  results ---> index             (e.g. 111)
    marketplace, country= scrap_df.tail(1)["scrap_meta.spider_marketplace"].values[0], scrap_df.tail(1)["scrap_meta.spider_country"].values[0]    
    result_df = pd.DataFrame()
    self.add_log_error("Marketplace and country from the scrap data: " + marketplace + "." + country, 0)
    
    rules0 = (categories_translate_tf['marketplace']==marketplace) & (categories_translate_tf['country']==country)
    target_col_names = categories_translate_tf[rules0]['target_col_name'].unique()

    for target_col_name in target_col_names:
      rules1 = (rules0) & (categories_translate_tf['target_col_name']==target_col_name)
      target_col_values = categories_translate_tf[rules1]['target_col_value'].unique()

      for target_col_value in target_col_values:
        rules2 = (rules1) & (categories_translate_tf['target_col_value']==target_col_value)
        rule_names = categories_translate_tf[rules2]['rule_name'].unique()
        
        for rule_name in rule_names:
          rules3 = (rules2) & (categories_translate_tf['rule_name']==rule_name)
          filters = categories_translate_tf[rules3]['filter_name'].index.values
          
          #each new rule
          isFirst = True
          joined_rules = True
          avoidRule = False
          
          for filter in filters:
            filter_name   = categories_translate_tf.loc[filter]['filter_name']
            filter_value  = categories_translate_tf.loc[filter]['filter_value']
            #print("           {} {} {} {} {} {} {}".format(marketplace,	country,	target_col_name,	target_col_value,	rule_name, filter_name,	filter_value))
            
            joined_df = pd.DataFrame()

            if isFirst:
              joined_rules = True 
              isFirst = False
            
            #Checks ...
            if filter_name in scrap_df:
              avoidRule = False
              joined_rules = (joined_rules) & (scrap_df[filter_name] == filter_value)

            else:
              avoidRule = True 
              self.add_log_error(":::RULE IS WRONG. THE FILTER_NAME DOES NOT CORRESPOND WITH ANY SCRAPPED COLUMN :::\n" + 
                  "    Please check the configuration \n" +
                  "    Scrap path: {}\n".format(scrap_filepath) +                                 
                  "    Rule Name: {}\n".format(rule_name) +
                  "    Scrapper {}.{} \n".format(marketplace, country) +
                  "        Target: ['{}'] == '{}']  \n".format(target_col_name, target_col_value) +
                  "        Filter: ['{}'] == '{}'] \n".format(filter_name, filter_value), Error_Level.Warning)
          
          if not avoidRule:
            #create a copy to prevent pandas' SettingWithCopyWarning
            joined_df = scrap_df[joined_rules].copy() 
            
            if len(joined_df) !=  0:
              joined_df.loc[:, target_col_name] =  target_col_value
              
            else:
              self.add_log_error(":::THE RESULT IS EMPTY, MAKE SURE THERE'S NO DATA IN THE WEBSITE:::\n" + 
                  "    Scrap path: {}\n".format(scrap_filepath) +   
                  "    Rule Name: {}\n".format(rule_name) +
                  "    Scrapper {}.{} \n".format(marketplace, country) +
                  "        Target: ['{}'] == '{}']  \n".format(target_col_name, target_col_value) +
                  "        Filter: ['{}'] == '{}'] \n".format(filter_name, filter_value), Error_Level.Warning)
              
            result_df = result_df.append(joined_df)

    #engine.add_log_error
    return result_df

  def set_default_values(self, scrap_df, scrap_filepath):   
    scrap_df.loc[:,"creation_date"] = pd.to_datetime(scrap_df['scrap_meta.spider_date_start']).dt.date
    scrap_df['img_url'] = ''
    scrap_df.loc[:,'img_url'] = ''
    scrap_df.loc[:,'seller'] = 'Store'
    scrap_df.loc[:,'isStoreBrand'] = False
    scrap_df.loc[:,'currency'] = scrap_df['currency'].replace('€', 'EUR')

    #NaN values
    scrap_df.loc[:,'brand'] = scrap_df['brand'].fillna('')
    scrap_df.loc[:,'specialPrice'] = scrap_df['specialPrice'].replace(np.nan,  "0.00")
    scrap_df.loc[:,'specialPrice'] = scrap_df['specialPrice'].apply(Application.remove_chars)
    scrap_df.loc[:,'specialPrice'] = scrap_df['specialPrice'].replace("", "0.00")   
    scrap_df.loc[:,'price'] = scrap_df['price'].replace(np.nan, 0.00) 
    scrap_df.loc[:,'price'] = scrap_df['price'].apply(Application.remove_chars)  
    scrap_df.loc[:,'price'] = scrap_df['price'].replace("",  0.00)
    scrap_df.loc[:,'reviews_rating'] = scrap_df['reviews_rating'].replace("",  0.00) 
    scrap_df.loc[:,'reviews_rating'] = scrap_df['reviews_rating'].replace(np.nan, 0.00)
    scrap_df.loc[:,'reviews_count'] = scrap_df['reviews_count'].replace("", 0)
    scrap_df.loc[:,'reviews_count'] = scrap_df['reviews_count'].replace(np.nan, 0.00)

    return scrap_df


  def set_isStoreBrand(self, scrap_df, scrap_filepath, store_brands):
    scrap_df.loc[:,'isStoreBrand']=scrap_df['brand'].isin(store_brands)
    return scrap_df


  def delete_irrelevant_data(self, scrap_df, scrap_filepath):
    # Some data clean up
    del_columns = [col for col in scrap_df if 'Unnamed' in col]
    scrap_df.drop(columns=del_columns, inplace=True)

    #delete not used cols
    del_columns = [col for col in scrap_df if 'specs.' in col]
    scrap_df.drop(columns=del_columns, inplace=True)
    del_columns = [col for col in scrap_df if 'custom.' in col]
    scrap_df.drop(columns=del_columns, inplace=True)

    # drop categories
    scrap_df.drop(columns=['category0', 'category1', 'category2', 'category3', 'category4', 'category5', 'category6'],
                inplace=True, errors='ignore')
    
    return scrap_df
  

  def set_data_types(self, scrap_df, scrap_filepath, product_datamodel):
    return scrap_df.astype(product_datamodel)

