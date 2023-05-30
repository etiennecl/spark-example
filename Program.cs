using System;
using Microsoft.Spark;
using Microsoft.Spark.Sql;

//using Microsoft.Spark;

namespace SparkExample
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            //var spark = SparkSession.Builder().GetOrCreate();
            
            var spark = SparkSession
                .Builder()
                .Master("local[*]")
                .AppName("connectors")
                .Config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .Config("spark.sql.catalog.dlf", "org.apache.iceberg.spark.SparkCatalog")
                .Config("spark.sql.catalog.dlf.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
                .Config("spark.sql.catalog.dlf.uri", "http://rest:8181")
                .Config("spark.sql.catalog.dlf.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .Config("spark.sql.catalog.dlf.s3.endpoint", "http://minio:9000")
                .Config("spark.sql.defaultCatalog", "demo")
                .Config("spark.eventLog.enabled", "true")
                .Config("spark.eventLog.dir", "/home/iceberg/spark-events")
                .Config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
                .GetOrCreate();
            // spark.SparkContext.Reference.Jvm
            spark.SparkContext.SetLogLevel("ERROR");
            
            // Mock data from https://mockaroo.com/
            var df = spark.Read().Json("facilities.json");
            df.Show();
            df.Write().Parquet("nom_fichier00.parquet");
            
            // var spark = SparkSession.Builder().AppName("Connectors").Config(new SparkConf()).Master("spark://ca1df3e51fb2:7077").GetOrCreate();
            // var df = spark.Read().Json("brunetPharmacies.json");
            // df.Show();
            //
            // var t = "";
            //var df = spark.CreateDataFrame();
            //df.Show();
            //df.Write().Parquet("nom_fichier.parquet");
        }
    }
}