This project reads data from:
1) restaurant data from CSV files
2) weather data from Parquet files
populates the restaurant data with location information and joins with the weather data.
The joined data will be saved in Parquet files.

   
The entrance point for this project is epamRestaurant.py file.
It is supposed to be that the restaurant data has much less size than the weather data.
The test was done with the following data:  1997 records of the restaurant data, 100 million records of the weather data.
The external OpenCage service (https://opencagedata.com/) is utilized to get Geocoding information. 
