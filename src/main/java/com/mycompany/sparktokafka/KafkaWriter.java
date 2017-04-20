package com.mycompany.sparktokafka;

import java.util.Map;
import java.util.Properties;
import kafka.common.FailedToSendMessageException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaWriter {
    private static Producer<String, String> producer;

    public KafkaWriter(String servers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }
    
    /*method writes aggregated map into kafka in simple format*/
    public void writeAsJson(Map<String, Long> map, String topicName) throws FailedToSendMessageException {
        Map<String, Long> data = map;
            data.forEach((k, v)
                    -> producer.send(new ProducerRecord<String, String>
        (topicName, String.format("{\"countryCode\":\"%s\",\"numberOfWords\":\"%d\"}", k, v))));
            System.out.println("dataset is probably written to topic" + topicName);
            producer.close();
    }
}
