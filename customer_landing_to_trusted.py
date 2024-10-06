import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1728142263995 = glueContext.create_dynamic_frame.from_catalog(database="haunctdb", table_name="customer_landing", transformation_ctx="CustomerLanding_node1728142263995")

# Script generated for node Share with Research
SqlQuery5206 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SharewithResearch_node1728142319582 = sparkSqlQuery(glueContext, query = SqlQuery5206, mapping = {"myDataSource":CustomerLanding_node1728142263995}, transformation_ctx = "SharewithResearch_node1728142319582")

# Script generated for node Customer Trusted
CustomerTrusted_node1728142576406 = glueContext.getSink(path="s3://haunct-data-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1728142576406")
CustomerTrusted_node1728142576406.setCatalogInfo(catalogDatabase="haunctdb",catalogTableName="customer_trusted")
CustomerTrusted_node1728142576406.setFormat("json")
CustomerTrusted_node1728142576406.writeFrame(SharewithResearch_node1728142319582)
job.commit()