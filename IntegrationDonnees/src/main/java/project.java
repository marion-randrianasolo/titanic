import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class project {

	public static void main(String[] args) {
		// Create a Spark session
		SparkSession sparkSession  = SparkSession.builder().appName("reneigh").master("local").getOrCreate();
		
		// Collect Data
		Dataset<Row> data = sparkSession.read()
				.format("csv")
				.option("header", "true")
				.option("delimiter", ';')
				.load("/home/marionlaury/Téléchargements/les-arbres.csv");
		
		// show 20 top results
		data.show();
		
		// Aggregation
		Dataset<Row> nbArbresByArr = data.groupBy("arrondissement").count();
		nbArbresByArr.show();
		
	}

}
