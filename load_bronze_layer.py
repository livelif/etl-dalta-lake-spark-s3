
class LoadBronzeLayer:
    def __init__(self, json_path, output_path, spark):
        self.json_path = json_path
        self.output_path = output_path
        self.spark = spark
    
    def loadTable(self):
        self.raw_data = self.spark.read.json(self.json_path)
        return self
    
    def write_bronze_layer(self):
        self.raw_data.write.mode("overwrite").parquet(self.output_path)
        
        