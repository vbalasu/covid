from chalicelib import cross_cloud
#cross_cloud.s3_to_gcs('trifacta-covid-trifactabucket-q1itzd5kh96', 'aws-trifacta-covid-trifactabucket-q1itzd5kh96', 'Prosper Strategic Insights - Coronavirus Covid-12Apr2020.xlsx')
cross_cloud.gcs_to_s3('covid_data_raw', 'gcs-covid-data-raw', 'un_raw/CASE_DATA/UN_Cases_04142020.csv')