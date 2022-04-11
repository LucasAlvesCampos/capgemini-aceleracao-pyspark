import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

'''schema_online_retail = StructType([
    StructField("InvoiceNo",  StringType(),  True),
    StructField("StockCode", StringType(),  True),
    StructField("Description",  StringType(),   True),
    StructField("Quantity",  IntegerType(),   True),
    StructField("InvoiceDate",  TimestampType(), True),
    StructField("UnitPrice",   FloatType(), True),
    StructField("CustomerID",  IntegerType(),  True),
	StructField("Country",  StringType(),  True)
])'''

REGEX_ALPHA    = r'[a-zA-Z]+'
REGEX_INTEGER  = r'[0-9]+'
REGEX_FLOAT    = r'[0-9]+\.[0-9]+'
REGEX_ALPHANUM = r'[0-9a-zA-Z]+'
REGEX_EMPTY_STR= r'[\t ]+$'
REGEX_SPECIAL  = r'[!@#$%&*\(\)_]+'
REGEX_NNUMBER  = r'^N[1-9][0-9]{2,3}([ABCDEFGHJKLMNPRSTUVXWYZ]{1,2})'
REGEX_NNUMBER_INVALID = r'(N0.*$)|(.*[IO].*)'
REGEX_TIME_FMT = r'^(([0-1]?[0-9])|(2[0-3]))([0-5][0-9])$'
REGEX_STOCK_CODE = r'^[0-9]{5}[A-z]*$'
REGEX_INVOICE_NO = r'^[0-9]{6}$'
REGEX_UNIT_PRICE = r'^[0-9]*,[0-9]+$|^[0-9]*$'
REGEX_CUSTOMER_ID = r'^[0-9]{5}$'

def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) == ''))


def invoiceNo_qa(dataframe):
	dataframe = dataframe.withColumn("qa_invoiceno",
						F.when(check_empty_column('InvoiceNo'), 'M')
						 .when(F.col('InvoiceNo').startswith('C'), 'Canceled')
						 .when(~F.col('InvoiceNo').rlike(REGEX_INVOICE_NO), 'F'))
	return dataframe
	#dataframe.filter(F.col('qa_invoiceno')=='M').show()
	#dataframe.filter(F.col('qa_invoiceno')=='Canceled').show()
	#dataframe.filter(F.col('qa_invoiceno')=='F').show()

def stockCode_qa(dataframe):
	dataframe = dataframe.withColumn("qa_stockcode",
						F.when(check_empty_column('StockCode'), 'M')
						 .when(~F.col('StockCode').rlike(REGEX_STOCK_CODE), 'Nominal'))
	return dataframe
	#dataframe.filter(F.col('qa_stockcode') == 'M').show()
	#dataframe.filter(F.col('qa_stockcode') == 'Nominal').show()
	
def description_qa(dataframe):
	dataframe = dataframe.withColumn("qa_description",
						F.when(check_empty_column('Description'), 'M'))
	return dataframe
	#dataframe.filter(F.col('qa_description') == 'M').show()


def quantity_qa(dataframe):
	dataframe = dataframe.withColumn("qa_quantity",
						F.when(check_empty_column('Quantity'), 'M')
						 .when(~F.col('Quantity').rlike(REGEX_INTEGER), 'F'))
	return dataframe
	#dataframe.filter(F.col('qa_quantity') == 'M').show()
	#dataframe.filter(F.col('qa_quantity') == 'F').show()
	

def invoicedate_qa(dataframe):
	dataframe = dataframe.withColumn("qa_invoicedate",
						F.when(check_empty_column('InvoiceDate'), 'M'))
	return dataframe
	#dataframe.filter(F.col('qa_invoicedate') == 'M').show()
	

def unitprice_qa(dataframe):
	dataframe = dataframe.withColumn("qa_unitprice",
						F.when(check_empty_column('UnitPrice'), 'M')
						 .when(~F.col('UnitPrice').rlike(REGEX_UNIT_PRICE), 'F'))
	return dataframe
	#dataframe.filter(F.col('qa_unitprice') == 'M').show()
	#dataframe.filter(F.col('qa_unitprice') == 'F').show()
	
def customerid_qa(dataframe):
	dataframe = dataframe.withColumn("qa_customerid",
						F.when(check_empty_column('CustomerID'), 'M')
						 .when(~F.col('CustomerID').rlike(REGEX_CUSTOMER_ID), 'F'))
	return dataframe
    
	#dataframe.filter(F.col('qa_customerid') == 'M').show()
	#dataframe.filter(F.col('qa_customerid') == 'F').show()


def country_qa(dataframe):
	dataframe = dataframe.withColumn("qa_country",
						F.when(check_empty_column('Country'), 'M'))
	return dataframe
	#dataframe.filter(F.col('qa_country') == 'M').show()

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	
	df = invoiceNo_qa(df)
	df = stockCode_qa(df)
	df = description_qa(df)
	df = quantity_qa(df)
	df = invoicedate_qa(df)
	df = unitprice_qa(df)
	df = customerid_qa(df)
	df = country_qa(df)

	df.toPandas()