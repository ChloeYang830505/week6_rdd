package Inverted;

import Inverted.dto.WordAndFileMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author YangChunping
 * @version 1.0
 * @date 2022/4/12 15:00
 * @description
 */
public final class InvertedIndex {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

//        if(args.length < 1){
//            System.err.println("Usage: java invertedIndex");
//            System.exit(1);
//        }
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[*]");
        sparkConf.set("spark.app.name", "localrun");
        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
//        for(String f : fileList){
//            JavaRDD<String> lines = sparkContext.textFile(f);
//            JavaRDD<Object> files1;
//            files1 = lines.flatMap(x-> Arrays.stream(x.split(" ")).forEach(y->{
//                WordAndFileMapper mapper = new WordAndFileMapper();
//                mapper.setNumber(f);
//                mapper.setWord(y);
//            }));
//        }
        JavaPairRDD<String, String> fileNameContentsRDD = javaSparkContext.wholeTextFiles("src/main/java/source/word", 1);
        JavaPairRDD<String, String> wordFileNameRDD = fileNameContentsRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, String>, String, String>) fileNameContentPair -> {
            String fileName = getFileName(fileNameContentPair._1());
            String content = fileNameContentPair._2();
            String [] lines = content.split("[\r\n]");
            List<Tuple2<String, String>> fileNameWordPairs = new ArrayList<>(lines.length);
            for(String line : lines){
                String [] wordsInCurrentLine = line.split(" ");
                fileNameWordPairs.addAll(Arrays.stream(wordsInCurrentLine).map(word -> new Tuple2<>(word, fileName)).collect(Collectors.toList()));
            }
            return fileNameWordPairs.iterator();
        });

        JavaPairRDD<Tuple2<String, String>, Integer> wordFileNameCountPerPairs = wordFileNameRDD.mapToPair(wordFileNamePair -> new Tuple2<>(wordFileNamePair, 1))
                .reduceByKey(Integer::sum);
        JavaPairRDD<String, Tuple2<String, Integer>> wordCountPerFileNamePairs = wordFileNameCountPerPairs.mapToPair(wordFileNameCountPerPair -> new Tuple2<>(wordFileNameCountPerPair._1._1, new Tuple2<>(wordFileNameCountPerPair._1._2, wordFileNameCountPerPair._2)));
        JavaPairRDD<String, String> result = wordCountPerFileNamePairs.groupByKey().mapToPair(wordCountPerFileNamePairIterator -> new Tuple2<>(wordCountPerFileNamePairIterator._1, StringUtils.join(wordCountPerFileNamePairIterator._2.iterator(), ','))).sortByKey();
        for(Tuple2<String, String> pair : result.collect()) {
            System.out.printf("\"%s\", {%s}%n", pair._1, pair._2);
        }
    }

    private static String getFileName(String s) {
        return s.substring(s.lastIndexOf('/') + 1   );
    }


}
