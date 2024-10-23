package main.java.com.Ana;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class Walmart {
	
	public static void main(String[] args) {


	    SparkSession spark = SparkSession.builder().appName("Walmart Sales Analysis").config("spark.master", "local").getOrCreate();
	    spark.sparkContext().setLogLevel("WARN");

		Dataset<Row> Walmart = spark.read()
				.format("csv")
				.option("sep", ",")
				.option("inferSchema", "true")
				.option("header", "true")
				.load("C:/Users/ana/Documents/PDS/walmart.csv");


		Dataset<Row> single = Walmart.filter(col("Marital_Status").equalTo(0));
		//samci.show(50,false);

		System.out.println("First query");

		Dataset<Row> avgPurchase = single.groupBy("Occupation", "City_Category", "User_ID")
				.agg(avg("Purchase").alias("Average_Spend"))
				.orderBy("Occupation");

		Dataset<Row> result1 = avgPurchase.groupBy("City_Category")
				.agg(functions.max(functions.struct("Average_Spend", "Occupation", "User_ID")).alias("maxSpend"))
				.select("City_Category", "maxSpend.*")
				.orderBy("Average_Spend");

		result1.show();
		System.out.println("//////////////////////////////////////////////////////////////////////");

		System.out.println("Second query");

		Dataset<Row> counted = Walmart.groupBy("Product_ID", "Age", "City_Category")
				.count()
				.orderBy("Product_ID", "Age", "City_Category");

		Dataset<Row> result2 = counted.groupBy("Product_ID")
				.agg(functions.max(functions.struct("count", "Age", "City_Category")).alias("maxCount"))
				.select("Product_ID", "maxCount.*")
				.orderBy(desc("count"));

		result2.show();
		System.out.println("//////////////////////////////////////////////////////////////////////");

		System.out.println("Third query\n");


		System.out.println("The most frequently purchased occupational products for men:");

		Dataset<Row> malePurchases = Walmart.filter(col("Gender").equalTo("M"));

		Dataset<Row> maleGrouped = malePurchases.groupBy("Occupation", "Product_ID")
				.agg(count("Product_ID").alias("ProductCount"))
				.orderBy(desc("ProductCount"));

		maleGrouped.show(false);

		System.out.println("The most frequently purchased products by profession for women:");

		Dataset<Row> femalePurchases = Walmart.filter(col("Gender").equalTo("F"));

		Dataset<Row> femaleGrouped = femalePurchases.groupBy("Occupation", "Product_ID")
				.agg(count("Product_ID").alias("ProductCount"))
				.orderBy(desc("ProductCount"));

		femaleGrouped.show(false);

		System.out.println("//////////////////////////////////////////////////////////////////////");



		System.out.println("Fourth query");

		Dataset<Row> filteredData = Walmart.filter((col("Gender").equalTo("M").and(col("Marital_Status").notEqual(1)))
						.or(col("Gender").equalTo("F")))
				.orderBy("Marital_Status");

		Dataset<Row> averagePurchase = filteredData.groupBy("City_Category","Age","Gender")
				.agg(avg("Purchase").alias("AvgPurchase"))
				.orderBy("City_Category","Age");

		Dataset<Row> pivotedData = averagePurchase.groupBy("City_Category","Age")
				.pivot("Gender")
				.agg(first("AvgPurchase"))
				.withColumn("Difference", abs(expr("M - F")))
				.select("City_Category","Age","Difference")
				.orderBy("City_Category","Age");

		String[] joinColumns = {"City_Category","Age"};
		Dataset<Row> result4 = pivotedData.join(averagePurchase, joinColumns)
				.orderBy("City_Category", "Age");

		result4.show(50, false);


	    spark.close();
	    //spark.sparkContext().stop();
	}
}