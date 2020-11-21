- We use DynamoDB table to store data for a travel agency that is booking reserverations. An external system is already collecting the reservation information and exporting a CSV file details:
    - Customer ID
    - City
    - Date
    - Customer specific notes that were taken at the time of the reservation
- Our application retrieves the reservation data CSV files from an S3 bucket and loads the data into a DynamoDB table. We then create and use secondary indexes to query the data and print a count of reservation for a particular city.

Lucid chart:
