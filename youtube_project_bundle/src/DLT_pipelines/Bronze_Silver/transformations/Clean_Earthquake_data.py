# from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
    count_if,
    from_json,
    explode,
    from_unixtime,
    current_timestamp,
)
from pyspark.sql.types import *
import dlt
catalog_name=spark.conf.get('catalog_name')
volume_path = f"/Volumes/{catalog_name}/bronze/youtube_earthquak_data"
primary_key='id'
properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("updated", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("sig", StringType()),
        StructField("code", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("type", StringType()),
        StructField("title", StringType()),
        StructField("felt", StringType()),
    ]
)
geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])
features_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("geometry", geometry_schema),
        StructField("properties", properties_schema),
    ]
)
schema = ArrayType(features_schema)


@dlt.view(name="earthquake_data_vw")
def earthquake_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )
    df = df.withColumn("Parsed_data", from_json(col("features"), schema))
    df = df.select(explode(col("Parsed_data")).alias("features"),col('_load_timestamp').alias('_load_timestamp'))
    df = df.select(
        "features.properties.*",
        "features.id",
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),'_load_timestamp'
        
    )

    df = (
        df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp"))
        .withColumn("mag", col("mag").cast("double"))
        .withColumn("nst", col("nst").cast("double"))
        .withColumn("sig", col("sig").cast("double"))
        .withColumn("tsunami", col("tsunami").cast("double"))
        .withColumn("tsunami", col("tsunami").cast("double"))
    )

    return df


dlt.create_streaming_table(name='earthquake_data_final')
dlt.apply_changes(
    target='earthquake_data_final',
    source='earthquake_data_vw',
    keys=[primary_key],
    sequence_by='_load_timestamp',
    stored_as_scd_type='1'
)
