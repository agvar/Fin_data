The project uses a Kaggle dataset in order to calculate a few Key perfomance Indicators that Peer to Peer lending organizations use.The KPIs used are from the following link-
https://www.lightico.com/blog/lending-kpis-most-important/

The dataset was loaded into an AWS S3 bucket and a pyspark notebook was created over an AWS EMR cluster. After the Exploratory Data Analysis on a subset of the files, the files were loaded using a pyspark job.
The Kaggle dataset that was the source of data had 2 large files from 2007 to 2018, one for accepted amd the other for rejected loans. These datasets were split into separate files for each quater. The python script is used to split the files.
Once the files are split, the files are loaded into an S3 bucket, from where the pyspark job processes them. The pyspark job cleanses the data and loads into a hive table

##### Purpose:
###### Create a pyspark job that cleanses data- replaces Null and NaN with zeros, standardizes addresses
###### Implement SCD type 2 on the quaterly file loads, by creating dimension and fact tables in Hive
###### Use Apache Airflow(Amazon Managed Workflows for Apache Airflow) to process quaterly files and sends alerts if the load fails
###### Check P2P KPIs in order to decide if this is an organization I want to invest in
###### Predict if a user would be granted a loan, if he would be predict his interest rate(based on his liabilities and credit ratings)

##### Pull Through Rate
This KPI measures pipeline efficiency by dividing total funded loans by the number of applications submitted during a defined period. This metric provides important insights into workflow efficiency, the quality of applications submitted, level of customer service, interest rate competitiveness and the suitability of a potential customer’s profile.
##### Application Approval Rate
This metric is calculated by dividing the amount of approved applications by the amount of submitted applications.
A low application approval rate means that a lender is investing too much time and money on unqualified borrower applications. Loan pipelines with a substandard application approval rate can be expedited by streamlining the document gathering and review processes.
##### Net Charge-Off Rate
The net charge-off rate is the the difference between gross charge-offs and any subsequent recoveries of delinquent debt. This KPI effectively represents that amount of debt a lender believes it will never collect compared to average receivables. Debt that is unlikely to be recovered is often written off and classified as gross charge-offs. To calculate the net charge-off value, any money that is eventually recovered on a debt is subsequently subtracted from the gross charge-offs.
##### Customer Acquisition Cost
This key financial measurement is the ratio of a borrower’s lifetime value to a borrower’s acquisition cost. These costs include but aren’t limited to research, marketing and advertising. Ideally, the customer acquisition cost should be greater than one since a borrower isn’t profitable if the cost to acquire is greater than the profit they will bring to a lender. This KPI is used by lenders to help determine how much of its resources can be profitably spent on a particular customer
##### Average Number of Conditions Per Loan
This KPI is especially relevant for lenders seeking to enhance their CX. According to the International Monetary Fund (IMF), the average number of conditions per loan is 26.8. And this study by the IMF also found that the number of conditions on loan applications is increasing. The loan application process is hindered by a proliferation of conditions, adversely affecting a lender’s ability to provide a fast and seamless customer experience.


##### DataSet used :
The dataset used in the project was downloaded from kaggle :
https://www.kaggle.com/wordsforthewise/lending-club
#### File Formats
##### rejected_2007_to_2018Q4.csv.gz
(Rejected loans from 2007 to 2018 -dataset for all accepted loans from 2007 to 2018 Q4 )
##### accepted_2007_to_2018Q4.csv.gz
(Accepted loans from 2007 to 2018 -dataset for all accepted loans from 2007 to 2018 Q4 )
The 2 Kaggle files which were large datasets with data from 2007 to 2018, were split into separate files by quater.

##### Data cleansing steps: 
###### Handle nulls, Nan Values across all fields 
###### remove rows that have missing data- check on all fields being null or Nan 
###### Ensure all calculation fields are float or numeral by removing spaces , removing symbols- '%','years' etc and converting
###### Validate data (eg- states vs zipcode data)- fix incorrect data
###### Check for duplicates: Use this as an opportunity to check for duplicates (based on name, email address, phone number, etc.), and do so at the start of the process. This will reduce both the amount of data you have to scrub as well as making data validation (at the end of the process) much simpler. 
###### Standardize Formats: Review the formats being used for applicable data types (dates, phone numbers, postal codes etc) and set and apply a standard format to all. 
###### Special Characters & Accents: Depending on the file format and encoding chosen, special characters can display weirdly once the data is exported. For example, the term “Açaí” might display as “AÃ§aÃ,”­ so make sure to do a check and clean as needed
###### Add fields that are a combination of multiple fields for easier analysis(like flags)
###### Remove leading and trailing spaces for all string fields
