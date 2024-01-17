import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Titanic {

	public static void main(String[] args) {
		SparkSession sparkSession  = SparkSession.builder().appName("Titanic Data").master("local[3]").getOrCreate();
		sparkSession.sparkContext().setLogLevel("WARN");
		
		String dataFile = "/home/marionlaury/Téléchargements/train.csv";
		
		Dataset<Row> df = sparkSession.read()
				.format("csv")
				.option("header", "true")
				.option("delimiter", ",")
				.load(dataFile);
		
		df.show();
		
	
		//System.out.println("?pmbre de l")
		
		df.describe().show();
	}

}
