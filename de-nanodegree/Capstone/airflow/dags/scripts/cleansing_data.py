import argparse
import os

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import when, count, col, isnan, sum, max, min, expr, desc, isnull, round, lit, avg, first
from pyspark.sql.functions import datediff, year, month, dayofmonth, quarter, format_number, upper
from pyspark.sql.types import IntegerType

def getImmigration(input_loc):
    """ Read Immigration information
    
    Args:
      input_loc       : input folder in EMR HDFS
      
    Returns:
      df_immigration  : dataframe contains Immigration information
    
    """
    
    # read immigration file
    filePath = os.path.join(input_loc, 'i94_apr16_sub.sas7bdat')
    df_immigration = spark.read.format('com.github.saurfang.sas.spark').load(filePath)
    df_immigration.write.mode('overwrite').parquet("sas_data")
    df_immigration=spark.read.parquet("sas_data")

    # cast to IntegerType
    cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94mode','i94bir','i94visa', 'biryear']

    for column in cols:
        df_immigration = df_immigration.withColumn(column,round(df_immigration[column]).cast(IntegerType()))
        
    # columns that need to be removed
    cols = ['count', 'dtadfile', 'entdepa','entdepd','matflag','dtaddto','admnum']

    # drop these columns
    df_immigration = df_immigration.drop(*cols)        

    # convert SAS date format to standard date format
    df_immigration = df_immigration.withColumn('arrival_date', expr("date_add('1960-01-01', arrdate)"))
    df_immigration = df_immigration.withColumn('departure_date', expr("date_add('1960-01-01', depdate)"))    

    # remove rows where duration_stay < 0
    # keep rows where duration_stay isnull as departure_date might be empty
    df_immigration = df_immigration.withColumn('duration_stay', datediff(col('departure_date'),col('arrival_date')))
    df_immigration = df_immigration.filter('duration_stay > 0 or duration_stay is null')
    
    # replace Null values in Mode of Transportation
    df_immigration = df_immigration.fillna({'i94mode':9})

    # valid gender
    list_gender = ['F', 'M']
 
    # replace invalid code with N/A
    df_immigration = df_immigration.withColumn('gender', 
                       when(df_immigration.gender.isin(list_gender),df_immigration['gender']).otherwise('N/A'))

    # columns with over 30% missing values
    cols = ['visapost', 'occup', 'entdepu','insnum']

    # drop these columns
    df_immigration = df_immigration.drop(*cols)
    
    # drop rows where Age Is Null or Age < 0
    df_immigration = df_immigration.filter('i94bir is not null and i94bir >= 0')    

    return df_immigration


def getState(input_loc):
    """ Read State information
    
    Args:
      input_loc   : input folder in EMR HDFS
      
    Returns:
      df_state    : dataframe contains State information
    
    """
    filePath = os.path.join(input_loc, 'State_Name.csv')
    df_state = spark.read.csv(filePath, sep=",", header=True, inferSchema=True)
    
    return df_state

def getCountry(input_loc):
    """ Read State information
    
    Args:
      input_loc     : input folder in EMR HDFS
      
    Returns:
      df_country    : dataframe contains Country information
    
    """
    filePath = os.path.join(input_loc, 'Country_Name.csv')
    df_country = spark.read.csv(filePath, sep=",", header=True, inferSchema=True)
    
    return df_country

def getTemperature(input_loc):
    """ Read Global Temperature information
    
    Args:
      input_loc      : input folder in EMR HDFS
      
    Returns:
      df_temperature : dataframe contains Global Temperature
    
    """
    filePath = os.path.join(input_loc, 'GlobalLandTemperaturesByCity.csv')
    df_temperature = spark.read.csv(filePath, header=True, inferSchema=True)

    # Remove all dates prior to 2010
    df_temperature = df_temperature.filter(df_temperature.dt >= '2010-01-01') 
    
    # Aggregate by Country
    df_temperature = df_temperature.groupby(["Country"]).agg(avg("AverageTemperature").alias("AverageTemperature"),
                                        first("Latitude").alias("Latitude"),
                                        first("Longitude").alias("Longitude")
                                       )    

    # uppercase Country so later could be joined with Country dimension table
    df_temperature = df_temperature.withColumn('Country',upper(col('Country'))) \
                         .withColumn('AverageTemperature',round('AverageTemperature',2))
    
    return df_temperature
    
def getPort(input_loc):
    """ Read Port information
    
    Args:
      input_loc     : input folder in EMR HDFS
      
    Returns:
      df_port       : dataframe contains Port information
    
    """
    filePath = os.path.join(input_loc, 'Port_Name.csv')
    df_port = spark.read.csv(filePath, sep=",", header=True, inferSchema=True)
    
    df_port = df_port.withColumnRenamed("PortCode","port_code") \
                     .withColumnRenamed("PortName","port_name")

    return df_port

def getDemographic(input_loc):
    """ Read Demographic information
    
    Args:
      input_loc      : input folder in EMR HDFS
      
    Returns:
      df_demographic : dataframe contains Demographic information
    
    """
    filePath = os.path.join(input_loc, 'us-cities-demographics.csv')
    df_demographic = spark.read.csv(filePath, sep=";", header=True, inferSchema=True)

    # remove rows based on missing values
    df_demographic = df_demographic.na.drop(subset=("Male Population","Female Population",
                                                    "Number of Veterans","Foreign-born",
                                                    "Average Household Size"))
    
    # pivot operation
    groupcol = ('City', 'State', 'Median Age', 'Male Population', 'Female Population', 
                'Total Population', 'Number of Veterans', 
                'Foreign-born', 'Average Household Size', 'State Code')
    aggrcol = sum('Count')
    df_demographic = df_demographic.groupBy(*groupcol).pivot("Race").agg(aggrcol)    

    # group by State
    df_demographic = df_demographic.groupBy("State Code","State").agg( \
        avg("Median Age").alias("avg_medianage"),
        sum("Male Population").alias("total_male"),
        sum("Female Population").alias("total_female"),
        sum("Total Population").alias("total_population"),
        sum("Number of Veterans").alias("total_veteran"),
        sum("Foreign-born").alias("total_foreignborn"),
        sum("American Indian and Alaska Native").alias("total_americannative"),
        sum("Asian").alias("total_asian"),
        sum("Black or African-American").alias("total_african"),
        sum("Hispanic or Latino").alias("total_hispanic"),
        sum("White").alias("total_white"),
        min("Average Household Size").alias("min_avghousesize"),
        max("Average Household Size").alias("max_avghousesize")                                                         
    )

    # upper case and rounding
    df_demographic = df_demographic.withColumn('State',upper(col('State'))) \
                     .withColumn('avg_medianage',round('avg_medianage',2))

    return df_demographic

def getCalendar(df_immigration):
    """ Generate Calendar dimension table based on Immigration
    
    Args:
      df_immig     : dataframe contain Immigration information
      
    Returns:
      df_calendar  : dataframe contains Calendar information
    
    """
    # create dimension date, combine arrival and departure date
    df_temp  = df_immigration.select(col("arrival_date").alias("date_deparr")).distinct()
    df_temp2 = df_immigration.select(col("departure_date").alias("date_deparr")).distinct()
    df_calendar = df_temp.union(df_temp2)
    df_calendar = df_calendar.distinct()

    # derived year, month, day from date
    df_calendar = df_calendar.withColumn("year_deparr", year(col("date_deparr"))) \
                             .withColumn("month_deparr", month(col("date_deparr"))) \
                             .withColumn("day_deparr", dayofmonth(col("date_deparr"))) \
                             .withColumn("quarter_deparr", quarter(col("date_deparr")))
    
    # remove rows based on missing values
    df_calendar = df_calendar.na.drop(subset=("date_deparr"))
    
    return df_calendar

def getModeTrans():
    """ Return Mode of Transportation information
    
    Args:
      None
      
    Returns:
      df_mode_trans       : dataframe contains Mode of Transport
    
    """
    df_mode_trans = spark.createDataFrame([(1, "Air"), (2, "Sea"), (3, "Land"),
                                           (9, "Not reported")], ("mode_code", "mode_name"))
    df_mode_trans = df_mode_trans.withColumn("mode_code",col("mode_code").cast(IntegerType()))
    
    return df_mode_trans

def getVisaCat():
    """ Return Visa Category
    
    Args:
      None
      
    Returns:
      df_visa_cat       : dataframe contains Visa Category
    
    """
    df_visa_cat = spark.createDataFrame([(1, "Business"), (2, "Pleasure"), 
                                         (3, "Student")], ("visacat_code", "visa_category"))
    df_visa_cat = df_visa_cat.withColumn("visacat_code",col("visacat_code").cast(IntegerType()))
    
    return df_visa_cat

def data_cleansing(input_loc, output_loc):
    """ EMR get raw data from S3, process it using Spark.
        Clean data from this process will be copied into S3.
        The clean data are ready to be consumed by Redshift.
    
    Args:
      input_loc   : input folder in EMR HDFS
      output_loc  : output folder in EMR HDFS
      
    Returns:
      None
    
    """
    # get Country dimension table
    df_country = getCountry(input_loc)
    
    # get Port dimension table 
    df_port = getPort(input_loc)    
    
    # get State dimension table
    df_state = getState(input_loc)
    
    # get Mode of Transportation
    df_mode_trans = getModeTrans()
    
    # get Visa Category
    df_visa_cat = getVisaCat()
    
    # get Temperature
    df_temperature = getTemperature(input_loc)
   
    # get Demographic dimension table
    df_demographic = getDemographic(input_loc)
    
    # get Immigration fact table
    df_immigration = getImmigration(input_loc)
    
    # get Calendar dimension table
    df_calendar = getCalendar(df_immigration)
    
    # collect valid code of State
    list_state = df_state.select("StateCode").rdd.flatMap(lambda x: x).collect()
    
    # replace invalid State code with 99 (All Other Codes)
    df_immigration = df_immigration.withColumn('i94addr',
                   when(df_immigration.i94addr.isin(list_state),df_immigration['i94addr']).otherwise('99'))    

    # Left Join df_country with df_temperature
    df_country = df_country.join(df_temperature, df_country['CountryName']==df_temperature['Country'],how='left')

    # drop these columns
    cols = ['Country']
    df_country = df_country.drop(*cols)
    
    # rename columns align with Redshift
    df_country = df_country.withColumnRenamed("CountryCode","country_code") \
                           .withColumnRenamed("CountryName","country_name") \
                           .withColumnRenamed("AverageTemperature","avg_temperature") \
                           .withColumnRenamed("Latitude","latitude") \
                           .withColumnRenamed("Longitude","longitude")
    
    # Left Join df_state with df_demographic
    df_state = df_state.join(df_demographic,df_state['StateCode'] == df_demographic['State Code'],
                             how='left')

    # drop these columns
    cols = ['State Code', 'State']
    df_state = df_state.drop(*cols)
    
    # rename column so it will align with Redshift schema
    df_state = df_state.withColumnRenamed("StateCode","state_code") \
                       .withColumnRenamed("StateName","state_name")    
    
    # put Country data as json file into hdfs output path
    filePath = os.path.join(output_loc, 'country.parquet')
    df_country.write.mode("overwrite").parquet(filePath)
    
    # put Port data as json file into hdfs output path
    filePath = os.path.join(output_loc, 'port.parquet')
    df_port.write.mode("overwrite").parquet(filePath)    
    
    # put Mode Transport as json file into hdfs output path
    filePath = os.path.join(output_loc, 'mode_transport.parquet')
    df_mode_trans.write.mode("overwrite").parquet(filePath)
    
    # put Visa Category as json file into hdfs output path
    filePath = os.path.join(output_loc, 'visa_category.parquet')
    df_visa_cat.write.mode("overwrite").parquet(filePath)
    
    # put Calendar dimension table as json file into hdfs output path
    filePath = os.path.join(output_loc, 'calendar.parquet')
    df_calendar.write.mode("overwrite").parquet(filePath)    
    
    # put State data as json file into hdfs output path
    filePath = os.path.join(output_loc, 'state.parquet')
    df_state.write.mode("overwrite").parquet(filePath)    
    
    # put immigration data as json file into hdfs output path
    filePath = os.path.join(output_loc, 'immigration.parquet')
    df_immigration.write.mode("overwrite").parquet(filePath)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/input")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder\
        .appName("Capstone Data Cleansing")\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    data_cleansing(input_loc=args.input, output_loc=args.output)
