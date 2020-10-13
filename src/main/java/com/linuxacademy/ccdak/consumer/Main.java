package com.linuxacademy.ccdak.consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {

    public static void main(String[] args) {
       // System.out.println("Hello, World!");
    	
    	Properties props = new Properties();
    	props.put("bootstrap.servers", "localhost:9092");
    	props.put("group.id", "group1");
    	props.put("enable.auto.commit", "true");
    	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	
    	KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
    	
    	consumer.subscribe(Arrays.asList("inventory_purchases"));
    	
    	try
    	{
    		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/home/cloud_user/output/output.dat",true));
    		
    		while(true)
    		{
    			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    			for(ConsumerRecord<String, String> record: records)
    			{
    				String recordString= "Key="+record.key()+",Value="+record.value()
    				+",topic="+record.topic()+",partition="+record.partition()+",offset="+record.offset();
    				bufferedWriter.write(recordString+"\n");
    			}
    			consumer.commitSync();
    			bufferedWriter.flush();
    		}
    	}
    	catch (IOException e) {
			// TODO: handle exception
    		throw new RuntimeException(e);
		}
    	
    	
    }

}
