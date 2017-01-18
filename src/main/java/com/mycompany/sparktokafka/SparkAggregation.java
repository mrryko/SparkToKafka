package com.mycompany.sparktokafka;

import static java.lang.Character.isDigit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

/**
 *
 * @author adrian
 */
public class SparkAggregation {

    public static Map<String, Long> aggregateData(String textFile, String appName, String masterName)
            throws NullPointerException,ArrayIndexOutOfBoundsException, InvalidInputException{
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile(textFile).cache().filter(v -> isDigit(v.charAt(0)));//cheks is line started with ip

        Map<String, List<String>> mpp = data.collect().parallelStream()
                .map(string -> string.split(","))
                .collect(Collectors.groupingBy(e -> e[5],
                        Collectors.mapping(e -> e[7], Collectors.toCollection(ArrayList::new)))); //splits rdd

        Map<String, Long> mapResult = mpp.entrySet()
                .parallelStream()
                .sorted(Map.Entry.comparingByValue((v1,v2)->Integer.valueOf(v2.size()).compareTo(Integer.valueOf(v1.size()))))
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().count(),
                        (a, b) -> a, //or throw an exception
                                    LinkedHashMap::new));   //counts number of search words from each country 
        mapResult.forEach((k, v) -> System.out.println("country : " + k + " number of words : " + v));
        return mapResult;

    }

}
