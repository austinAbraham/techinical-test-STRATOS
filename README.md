# STRATOS Technical Test Solution
The solution answers three questions regarding stocks provided.
1. Which stock has had the greatest relative increase in price in this period?
2. If you had invested $1 million at the beginning of this period by purchasing $10,000 worth of shares in every company in the list equally, how much would you have today?
3. Which stock had the highest CAGR during the period?



## Overview
The solution has been designed in such a way that it can accept user arguments to update parameters to the analysis to allow customs period start and end dates as well as being able to specify csv file to be used. 

The solution reads user arguments and uses default values where it is not provided. The data file is read and a list of tickers is generated from it. The list is then used to call the Polygon API for all necessary inforation in the mentioned period. The data is parsed and returned as a set which is used to create a list of sets and will be used to create a PySpark dataframe. This data frame is used to generate the relative change the stock value. This dataframe will then be used to answer the three questions by making use of PySpark aggregations and transformations.

The aggregates will then be used to create a report that will be generated and stored with a timestamp in the filename. It will be available in the `reports` folder.



## Option to run customs datasets and dates
The solution can accept a total of 4 user arguments, of which 3 user arguments that can be used to run differnt data set for differnt dates.
- --file-location (Optional): Location of the CSV file to be used for analysis. Default value is `data/stocks.csv`
- --api-key (Required): The Polygon.io API key. Rate limts have been set assuming a paid API key would be used. Polygon.io recommeds a maximum of 100 requests per second(https://polygon.io/knowledge-base/article/what-is-the-request-limit-for-polygons-restful-apis), it has been limited to 80 in the solution.
- --start-date (Optional): Custom start date for analysis in `YYYY-MM-DD`
- --end-dates (Optional): Custom end date for analysis in `YYYY-MM-DD`



## Deplotment instructions

1. Install VirtualEnv                   
    `pip install virtual env`
2. Create Python virtual environment. Python 3.11 is recommended.    
    `virtualenv venv --python=python3.11`
3. Start virtual environment            
    `source venv/bin/activate`
4. Install requirements.txt
    `pip install -r requirements.txt`
5. Run test(s)
    `pytest .`
6. Run analysis
    `python src/main.py --file-location < Custom CSV file location (OPTIONAL) > --api-key <Polygon.io API key> --start-date < Custom CSV start date (OPTIONAL)> --end-date < Custom CSV end date (OPTIONAL) >`        



## Output Report
Reports generated can be found in the `reports` folder as a text( eg. `report(2024-03-18 03:15:15.286996).txt` )
Reports will be of the following format
```
Analysis Report
===============

Report generated at 2024-03-18 03:15:15.287147

Analysis period start: 2022-06-01
Analysis period end: 2022-06-02

MSFT had the greatest relative increase during the period.
Relative increase in value: $-1.66%

Buying $10,000 worth of shares of each compamy would have a total value of $19,448.37 at the period end.

AAPL had the highest CAGR gain over the period with 1.040%
```



## Notes
- The instruction in the test for the thrid question, specifies CAGR for the year 2023 
`During 2023, which stock had the highest compounding  monthly growth rate (monthly CAGR)?`
This question can be answered by running the followinf command, `python src/main.py --api-key <Polygon.io API key> --start-date 2023-01-01 --end-date 2023-12-31`.The solution was wriiten in such a it would be reusable and would offer more flexibility, and explicitly answering this question in each run would take away the reusable nature of the solution as the year 2023 would have to be hard coded as a condition.
- The solution has minimal testing currently implemented as the most relevvant elements of teh solution are APIR calls and simple PySpark tranformations. If more detailed testing in requred in any cirumstances, unit tests can be created can be written using sample data frames in each step of the transformation. 