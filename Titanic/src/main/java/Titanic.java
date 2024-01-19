import java.util.Arrays;

import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Titanic {

	public static void main(String[] args) {
		
		// Create a spark session
		SparkSession sparkSession  = SparkSession.builder().appName("Titanic Data").master("local[3]").getOrCreate();
		sparkSession.sparkContext().setLogLevel("WARN");
		
		// data from Kaggle
		String dataFile = "C:\\Users\\mario\\Downloads\\train.csv";
		
		//Collect Data into spark
		Dataset<Row> df = sparkSession.read()
				.format("csv")
				.option("header", "true")
				.option("delimiter", ",")
				.load(dataFile);
		
		df.show();
		
		// Additional information
		System.out.println("Nombre de lignes : "+df.count());
		System.out.println("Colonnes : "+Arrays.toString(df.columns()));
		System.out.println("Types de données : "+Arrays.toString(df.dtypes()));

		// Main stats
		df.describe().show();
		
		// Data preparation and feature engineering
		Dataset<Row> dataset = df.select(df.col("Survived").cast("float"),
				df.col("Pclass").cast("float"),
				df.col("Sex"),
				df.col("Age").cast("float"),
				df.col("Fare").cast("float"),
				df.col("Embarked")
			);
		
		dataset.show();
		
		// Display all rows with Age is null
		dataset.filter("Age is null").show();
		
		// Replace ? with null
		for (String columnName : dataset.columns() ) {
			dataset = dataset.withColumn(columnName,
					functions.when(dataset.col(columnName).equalTo("?"), null).otherwise(dataset.col(columnName)));
		}

		// Deletes rows with null values
		dataset = dataset.na().drop();
		
		dataset.show();
		
		// Indexing Sex column
		StringIndexerModel indexerSex = new StringIndexer()
				.setInputCol("Sex")
				.setOutputCol("Gender")
				.setHandleInvalid("keep")
				.fit(dataset);
		dataset = indexerSex.transform(dataset);
		
		//Indexing Embarked column 
		StringIndexerModel indexerEmbarked = new StringIndexer()
				.setInputCol("Embarked")
				.setOutputCol("Boarded")
				.setHandleInvalid("keep")
				.fit(dataset);
		dataset = indexerEmbarked.transform(dataset);
		
		dataset.show();
		dataset.describe().show();
		
		System.out.println("Types de données : "+Arrays.toString(dataset.dtypes()));
		
		// Drop unnecessary columns
		dataset = dataset.drop("Sex");
		dataset = dataset.drop("Embarked");
		dataset.show();
		
		// Create Features column
		// Select necessary columns
		String[] requiredFeatures = {"Pclass", "Age", "Fare", "Gender", "Boarded"};
		
		Column[] selectedColumns = new Column[requiredFeatures.length];
		for (int i = 0; i < requiredFeatures.length; i++) {
			selectedColumns[i] = dataset.col(requiredFeatures[i]);
		}
		
		// VecorAssembler to assemble features
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(requiredFeatures)
				.setOutputCol("features");
		
		// Transform the data
		Dataset<Row> transformedData = assembler.transform(dataset);
		
		// Display the transformed data
		transformedData.show();
		
		// Modeling
		// Create two sets : one for training and one for testing
		Dataset<Row>[] split = transformedData.randomSplit(new double[] {0.8,0.2});
		Dataset<Row> trainingData = split[0];
		Dataset<Row> testData = split[1];
		
		trainingData.describe().show();
		testData.describe().show();
		
		// Using Random Forest Classifier
		
		// Initialize the RandomForestClassifier
		RandomForestClassifier rf = new RandomForestClassifier()
				.setLabelCol("Survived")
				.setFeaturesCol("features")
				.setMaxMemoryInMB(5);
		
		// Create the model
		RandomForestClassificationModel rfModel = rf.fit(trainingData);
		
		// Display the parameters of the model
		System.out.println("Random Forest Model Parameters:\n" + rfModel.explainParams());
		
		// This will give us something called a transformer
		// And finally, we predict using the test dataset :
		Dataset<Row> predictions = rfModel.transform(testData);
		
		predictions.show();
		
		// Evaluate our model 
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("Survived")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");
		
		// Display the parameters of the evaluator
		System.out.println("Evaluator Parameters:\n" + evaluator.explainParams());

		// And to get the accuracy we do : 
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test Accuracy = "+accuracy); // -> 0.819
		
		try {
			Thread.sleep(86400000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		sparkSession.stop();
		
		
	}

}
