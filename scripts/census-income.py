from itertools import count
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
schema_census_income = StructType([
	StructField('age', IntegerType(), True),
	StructField('workclass', StringType(), True),
	StructField('fnlwgt', IntegerType(), True),
	StructField('education', StringType(), True),
	StructField('education-num', IntegerType(), True),
	StructField('marital-status', StringType(), True),
	StructField('ocupation', StringType(), True),
	StructField('relashionship', StringType(), True),
	StructField('race', StringType(), True),
	StructField('sex', StringType(), True),
	StructField('capital-gain', IntegerType(), True),
	StructField('capital-loss', IntegerType(), True),
	StructField('hours-per-week', IntegerType(), True),
	StructField('native-country', StringType(), True),
	StructField('income', StringType(), True)
])

def question_1(df):
	df = (df.withColumn('workclass',
	              F.when(F.col('workclass').rlike("\?"), None)
				  .otherwise(F.col('workclass'))))
	

	(df.where(F.col('workclass').isNotNull() & (F.col('income').rlike(">50K")))
	   .groupBy('workclass', 'income')
	   .count()
	   .orderBy(F.col('count').desc())
	   .show())
	   
def question_2(df):
	(df.where((F.col('race').isNotNull()) & (F.col('hours-per-week').isNotNull()))
	   .groupBy('race')
	   .agg(F.round(F.avg(F.col('hours-per-week')),2).alias('Media de horas por semana'))
	   .orderBy(F.col('Media de horas por semana').desc())
	   .show())

def question_3(df):
	
	homens = df.where(F.col('sex') == ' Male').count()
	mulheres = df.where(F.col('sex') == ' Female').count()

	print(f"O numero total e: {(df.where(F.col('sex').isNotNull()).count())}")
	print(f"O numero de Homens e: {homens}")
	print(f"O numero de Mulheres e: {mulheres}")
	print(f"A proporcao e de: {homens/mulheres} homens para cada mulher")
	

def question_5(df):
	df = (df.withColumn('ocupation',
	              F.when(F.col('ocupation').rlike("\?"), None)
				  .otherwise(F.col('ocupation'))))
	
	(df.where((F.col('ocupation').isNotNull()) & (F.col('hours-per-week').isNotNull()))
	   .groupBy('ocupation')
	   .agg(F.round(F.avg(F.col('hours-per-week')),2).alias('Media de horas por semana'))
	   .orderBy(F.col('Media de horas por semana').desc())
	   .show())

def question_6(df):
	df = (df.withColumn('ocupation',
	              F.when(F.col('ocupation').rlike("\?"), None)
				  .otherwise(F.col('ocupation'))))

	(df.where((F.col('ocupation').isNotNull()) & (F.col('education').isNotNull()))
	   .groupBy('education', 'ocupation')
	   .count()
	   .orderBy(F.col('count').desc())
	   .dropDuplicates(['education'])
	   .show())

def question_7(df):
	df = (df.withColumn('ocupation',
	              F.when(F.col('ocupation').rlike("\?"), None)
				  .otherwise(F.col('ocupation'))))

	(df.where((F.col('ocupation').isNotNull()) & (F.col('sex').isNotNull()))
	   .groupBy('sex','ocupation')
	   .count()
	   .orderBy(F.col('count').desc())
	   .dropDuplicates(['sex'])
	   .show())


def question_8(df):
	(df.where((F.col('education').isNotNull()) & (F.col('education') == " Doctorate")& (F.col('race').isNotNull()))
	   .groupBy('race','education')
	   .count()
	   .orderBy(F.col('count').desc())
	   .dropDuplicates(['race'])
	   .show())

def question_9(df):
	(df.where((F.col('workclass') == ' Self-emp-not-inc') | (F.col('workclass') == ' Self-emp-inc'))
       .groupBy('workclass', 'education', 'sex', 'race')
       .count()
       .orderBy(F.col('count').desc())
       .dropDuplicates(['workclass'])
       .show())

def question_10(df):
	casados = df.where(F.col('marital-status').contains('Married')).count()
	nao_casados = df.where(~F.col('marital-status').contains('Married')).count()

	print(f"O numero total e: {(df.where(F.col('sex').isNotNull()).count())}")
	print(f"O numero de casados e: {casados}")
	print(f"O numero de nao casados e: {nao_casados}")
	print(f"A proporcao e de: {casados/nao_casados} casados para nao casados")
	
def question_11(df):

	df = df.withColumn('marital-status',
	  F.when(~F.col('marital-status').contains('Married'), "Unmarried")
	   .otherwise(F.col('marital-status')))

	df = (df.where(F.col('marital-status') == 'Unmarried')
       .groupBy('marital-status', 'race')
       .count()
       .orderBy(F.col('count').desc())       
       .show(1))


if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/census-income/census-income.csv"))
	

	#question_1(df)
	#question_2(df)
	#question_3(df)
	#question_5(df)
	#question_6(df)
	#question_7(df)
	#question_8(df)
	#question_9(df)
	#question_10(df)
	#question_11(df)