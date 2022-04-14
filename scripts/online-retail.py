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

# Funcoes auxiliares

def check_empty_column(col):
    return (F.col(col).isNull() | (F.col(col) == ''))


# Qualidade

def invoiceNo_qa(dataframe):
	dataframe = dataframe.withColumn("qa_invoiceno",
						F.when(check_empty_column('InvoiceNo'), 'M')
						 .when(F.col('InvoiceNo').startswith('C'), 'Canceled')
						 .when(~F.col('InvoiceNo').rlike(REGEX_INVOICE_NO), 'F'))
	return dataframe
	#dataframe.filter(F.col('qa_invoiceno')=='M').show()
	#dataframe.filter(F.col('qa_invoiceno')=='Canceled').show()
	#dataframe.filter(F.col('qa_invoiceno')=='F').show()
	#dataframe.filter(F.col('qa_invoiceno').isNull()).show()

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


# Transformacoes

def transform_unitprice(dataframe):
	dataframe = dataframe.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'), ',', ".").cast('float'))	
	return dataframe

def transform_invoicedate(dataframe):	
	dataframe = dataframe.withColumn('InvoiceDate', F.concat(F.col('InvoiceDate'),F.lit(':00')))
	dataframe = dataframe.withColumn("InvoiceDate", F.from_unixtime(F.unix_timestamp(F.col("InvoiceDate"),'d/M/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss').cast('timestamp'))  
	return dataframe


if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

# Qualidade	
df = invoiceNo_qa(df)
df = stockCode_qa(df)
df = description_qa(df)
df = quantity_qa(df)
df = invoicedate_qa(df)
df = unitprice_qa(df)
df = customerid_qa(df)
df = country_qa(df)

# Transformacao
df = transform_unitprice(df)
df = transform_invoicedate(df)

# Pergunta 1

def pergunta_1(df):
	(df.filter((F.col("StockCode").startswith("gift_0001")) & (F.col("qa_invoiceno").isNull()))
								.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('Valor Total de Vendas'))
								.show(100))


#pergunta_1(df)

# Pergunta 2

def pergunta_2(df):
	df = df.withColumn('month', F.date_format(F.col("InvoiceDate"), "MM"))

	(df.filter((F.col("StockCode").startswith("gift_0001")) & (F.col("qa_invoiceno").isNull()))
								.groupBy('month')
								.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('Valor Total de Vendas por mes'))
								.orderBy(F.col('month'))
								.show())

#pergunta_2(df)


# Pergunta 3

def pergunta_3(df):
	(df.filter((F.col("StockCode") == ("S")) & (F.col("qa_invoiceno").isNull()))
                              .groupBy('StockCode')
							  .agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('Valor Total de Vendas'))
							  .orderBy(F.col('Valor Total de Vendas').desc())
							  .show())

#pergunta_3(df)


 # Pergunta 4

def pergunta_4(df):
	(df.filter(F.col('qa_invoiceno').isNull())
                        .groupBy("Description").agg(F.sum(F.col('Quantity')))                      
						.orderBy(F.col('sum(Quantity)').desc())
						.show(1))


#pergunta_4(df)

# Pergunta 5

# Ainda nao consegui
def pergunta_5(df):
	df = df.withColumn('month', F.date_format(F.col("InvoiceDate"), "MM"))

	df = (df.filter(F.col('qa_invoiceno').isNull())
	                                     .groupBy('StockCode','month')
										 .agg(F.sum(F.col('Quantity')))										 
									     .orderBy(F.col('sum(Quantity)').desc()))	
	
	df_max_per_month = df.groupBy('month').max('sum(Quantity)')

	df_max_per_month = df_max_per_month.join(df.alias('b'), 
								F.col('b.sum(Quantity)') == F.col('max(sum(Quantity))'),
								"left").select('b.month','StockCode','sum(Quantity)')

	df_max_per_month.orderBy('month').show()


#pergunta_5(df)


# Pergunta 6

def pergunta_6(df):
	df = df.withColumn('hour', F.date_format(F.col("InvoiceDate"), "HH"))

	(df.filter(F.col("qa_invoiceno").isNull())
									.groupBy('hour')
									.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('Valor Total de Vendas por Hora'))
									.orderBy(F.col('Valor Total de Vendas por Hora').desc())
									.show(1))
#pergunta_6(df)

# Pergunta 7

def pergunta_7(df):
	df = df.withColumn('month', F.date_format(F.col("InvoiceDate"), "MM"))

	(df.filter(F.col("qa_invoiceno").isNull())
								.groupBy('month')
								.agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('Valor Total de Vendas por Mes'))
								.orderBy(F.col('Valor Total de Vendas por Mes').desc())
								.show(1))

#pergunta_7(df)

# Pergunta 8

def pergunta_8(df):
	df = df.withColumn('month', F.date_format(F.col("InvoiceDate"), "MM"))

	df = (df.filter(F.col('qa_invoiceno').isNull())
	                                     .groupBy('StockCode','month')
										 .agg(F.round(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('Valor de vendas'))										 
									     .orderBy(F.col('Valor de vendas').desc()))	
	
	df_max_per_month = df.groupBy('month').max('Valor de vendas')

	df_max_per_month = df_max_per_month.join(df.alias('b'), 
								F.col('b.Valor de vendas') == F.col('max(Valor de vendas)'),
								"left").select('b.month','StockCode','Valor de vendas')

	df_max_per_month.orderBy('month').show()

#pergunta_8(df)


# Pergunta 9

def pergunta_9(df):
	df = (df.filter((F.col('qa_invoiceno').isNull()) & (F.col('StockCode') != 'PADS'))
	                                     .groupBy('Country')
										 .agg(F.round(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('Valor de vendas'))										 
									     .orderBy(F.col('Valor de vendas').desc()).show(1))
#pergunta_9(df)

# Pergunta 10

def pergunta_10(df):
	df = (df.filter((F.col('qa_invoiceno').isNull()) & (F.col('StockCode') == 'M'))
	                                     .groupBy('Country')
										 .agg(F.round(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('Valor de vendas'))										 
									     .orderBy(F.col('Valor de vendas').desc()).show(1))

#pergunta_10(df)

# Pergunta 11

def pergunta_11(df):
	df = (df.filter(F.col('qa_invoiceno').isNull())
	                                     .groupBy('InvoiceNo')
										 .agg(F.round(F.sum(F.col('Quantity') * F.col('UnitPrice')), 2).alias('Valor de vendas'))										 
									     .orderBy(F.col('Valor de vendas').desc()).show(1))
#pergunta_11(df)

# Pergunta 12

def pergunta_12(df):
	df = (df.filter(F.col('qa_invoiceno').isNull())
	                                     .groupBy('InvoiceNo')
										 .agg(F.round(F.sum(F.col('Quantity')), 2).alias('Numero de itens'))										 
									     .orderBy(F.col('Numero de itens').desc()).show(1))

#pergunta_12(df)

# Pergunta 13

def pergunta_13(df):
	df = (df.filter(F.col('qa_invoiceno').isNull())
	                                     .groupBy('CustomerID')
										 .agg(F.count(F.col('CustomerID')).alias('Numero de compras'))										 
									     .orderBy(F.col('Numero de compras').desc()).show(1))

#pergunta_13(df)