This project contains of the following steps:
1) reads restaurant data from CSV files
2) reads weather data from Parquet files
3) populates the restaurant data with location information
4) joins the restaurant data with the weather data
5) the joined data is saved in Parquet files.

   
The entrance point for this project is epamRestaurant.py file.
It is supposed to be that the restaurant data has much less size than the weather data.
The test was done with the following data:  1997 records of the restaurant data, 100 million records of the weather data.
The external OpenCage service (https://opencagedata.com/) is utilized to get Geocoding information. 
