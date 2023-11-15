import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SortGensortData {
    public static void main(String[] args) {
        String inputFile = args[0]; // input file path
        String outputFile = args[1]; // output file path

        SparkConf sparkConf = new SparkConf().setAppName("SortGensortData");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> input = sc.textFile(inputFile);

        // Counting input records
        long inputCount = input.count();
        System.out.println("Number of input records: " + inputCount);

        // Sort by the first 10 characters of each line
        JavaRDD<String> sortedData = input.sortBy(
            new Function<String, String>() {
                public String call(String s) {
                    return s.length() >= 10 ? s.substring(0, 10) : s;
                }
            }, true, sc.defaultParallelism());

        // Counting sorted records
        long sortedCount = sortedData.count();
        System.out.println("Number of sorted records: " + sortedCount);

        // Save the sorted data to the output file
        sortedData.saveAsTextFile(outputFile);

        sc.close();
    }
}
