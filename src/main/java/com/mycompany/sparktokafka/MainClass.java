package com.mycompany.sparktokafka;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import kafka.common.FailedToSendMessageException;
import org.apache.hadoop.mapred.InvalidInputException;

public class MainClass {

    public static void main(String[] args) {
        String textFile = "/home/data/Documents/uservisits";
        String appNameForSpark = "Spark Aggregation Application";
        String masterNameForSpark = "local";
        String bootstrap = "localhost:9092";
        String groupId = "test-group2";
        String topicName = "sspark";
        
        Map<String, Long> mapResult = new LinkedHashMap<>();
        
        try {
            mapResult = SparkAggregation.aggregateData(textFile, appNameForSpark, masterNameForSpark);
        } catch (NullPointerException|ArrayIndexOutOfBoundsException ex) {
            System.err.println("check if file is valid. error: " + ex);
        }
        catch (InvalidInputException ex) {
            System.err.println("check if name of file is valid: " + ex);
        }
        
        KafkaWriter kWriter = new KafkaWriter(bootstrap); 
        
        try {
            kWriter.writeAsJson(mapResult, topicName);
        } catch (FailedToSendMessageException ex) {
            System.err.println(ex.toString());
        }        
    }
    
}
