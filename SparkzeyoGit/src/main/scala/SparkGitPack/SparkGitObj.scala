package SparkGitPack

object SparkGitObj {
  
  ef main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc =new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
    
     println("==========Task 2- Flatten with multiple explodes=======================")
    
    val rawdf=spark.read.option("multiline","true").json("d:/data/files/MultiArrays.json")
    
    rawdf.show(false)
    rawdf.printSchema()
    
    println("============Flatten Array=======")
    val flaten_df=rawdf.withColumn("Students",explode(col("Students")))
    flaten_df.show(false) 
    flaten_df.printSchema()
    
    println("Flatten Nested Array")
    val nested_array_df=flaten_df.withColumn("components",explode(col("Students.user.components")))
    
    nested_array_df.show(false)
    nested_array_df.printSchema()
    
    val final_df=nested_array_df.select(
        
                              col("Students.user.address.Permanent_address").alias("UPermanent_address"),
                              col("Students.user.address.temporary_address").alias("Utemporary_address"),
                              col("Students.user.gender"),
                              col("Students.user.name.first"),
                              col("Students.user.name.last"),
                              col("Students.user.name.title"),
                              col("address.Permanent_address"),
                              col("address.temporary_address"),
                              col("first_name"),
                              col("second_name"),
                              col("components")
                              
                             )
    
          final_df.show()
          final_df.printSchema()
        
    
    
    
  }
}
  
