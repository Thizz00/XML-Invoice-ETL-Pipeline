import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import glob

def main():

    path = "/opt/spark/data/correctly_validated/*.xml"
    
    if not glob.glob(path):
        print(f"No XML files in {path}. Pipeline exits without errors.")
        return 

    spark = SparkSession.builder.appName("XML_to_DB") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memoryOverhead", "512m") \
        .getOrCreate()
    
    addr = StructType([StructField("Street", StringType()), StructField("City", StringType()), StructField("Zip", StringType()), StructField("Country", StringType())])
    party = StructType([StructField("Name", StringType()), StructField("TaxID", StringType()), StructField("Address", addr)])
    tots = StructType([StructField("Net", DecimalType(15,2)), StructField("VAT", DecimalType(15,2)), StructField("Gross", DecimalType(15,2))])

    schema = StructType([
        StructField("_corrupt_record", StringType()),
        StructField("InvoiceNumber", StringType()),
        StructField("InvoiceType", StringType()),
        StructField("IssueDate", DateType()),
        StructField("DueDate", DateType()),
        StructField("Currency", StringType()),
        StructField("Vendor", party),
        StructField("Buyer", party),
        StructField("Items", StructType([StructField("Item", ArrayType(StructType([
            StructField("Description", StringType()), StructField("Quantity", IntegerType()),
            StructField("UnitPrice", DecimalType(15,2)), StructField("VAT", IntegerType())
        ])))])),
        StructField("Totals", tots)
    ])

    df = spark.read.format("xml").option("rowTag", "Invoice").schema(schema)\
        .option("columnNameOfCorruptRecord", "_corrupt_record").load(path)\
        .withColumn("file_name", F.element_at(F.split(F.input_file_name(), "/"), -1)).cache()

    if not df.head(1): return spark.stop()

    df.select("file_name", F.when(F.col("_corrupt_record").isNull(), "success").otherwise("error").alias("status")) \
        .distinct().write.mode("overwrite").json("/opt/spark/data/audit_results")

    valid = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    now = F.current_timestamp()

    head = valid.selectExpr("InvoiceNumber as invoice_number", "InvoiceType as invoice_type", "IssueDate as issue_date", 
                            "DueDate as due_date", "Currency", "Vendor.Name as vendor_name", "Vendor.TaxID as vendor_taxid", 
                            "Buyer.Name as buyer_name", "Buyer.TaxID as buyer_taxid", "Totals.Net as net_total", 
                            "Totals.VAT as vat_total", "Totals.Gross as gross_total").withColumn("processed_at", now)

    items = valid.select("InvoiceNumber", F.explode("Items.Item").alias("i")).selectExpr(
        "InvoiceNumber as invoice_number", "i.Description as description", "i.Quantity as quantity", 
        "i.UnitPrice as unit_price", "i.VAT as vat_rate").withColumn("processed_at", now)

    url = os.getenv("POSTGRES_URL")
    db_opts = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASS"),
        "driver": "org.postgresql.Driver",
        "batchsize": "5000", 
        "rewriteBatchedInserts": "true"
    }

    try:
        head.write.jdbc(url, "stg_headers", "overwrite", db_opts)
        items.write.jdbc(url, "stg_items", "overwrite", db_opts)
        
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(url, db_opts["user"], db_opts["password"])
        stmt = conn.createStatement()

        stmt.execute("""
            BEGIN;
            INSERT INTO invoices1 SELECT * FROM stg_headers ON CONFLICT (invoice_number) DO NOTHING;
            
            INSERT INTO invoice_items (invoice_number, description, quantity, unit_price, vat_rate, processed_at)
            SELECT s.invoice_number, s.description, s.quantity, s.unit_price, s.vat_rate, s.processed_at 
            FROM stg_items s 
            WHERE EXISTS (SELECT 1 FROM invoices1 i WHERE i.invoice_number = s.invoice_number) 
            AND NOT EXISTS (
                SELECT 1 FROM invoice_items e 
                WHERE e.invoice_number = s.invoice_number 
                AND e.description = s.description 
                AND e.unit_price = s.unit_price
            );
            COMMIT;
        """)
        conn.close()
    except Exception as e: print(f"DB Error: {e}")
    
    spark.stop()

if __name__ == "__main__": main()