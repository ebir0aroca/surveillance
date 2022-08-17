# SURVEILLANCE
Pipeline:
1. Scrapping process
2. Cleaning each scrap & translate categories 
3. Consolidation in single database


## Data Structure
---------------------------
* MyDrive/root/	
	* /out/complete_{date} #folder with all scrap to be cleaned and consolidated			
		* ----all scraped json files---
		* /scrap_logs/
			* log.csv
		* /dbs/
			* maindatabase.csv
	* analyze_marketplaces.ipynb

## Code Structure
---------------------------
* /content//content/surveillance
  * /core
  	* engine.py
  * /data
  	* mining.py
  	* standards.py

