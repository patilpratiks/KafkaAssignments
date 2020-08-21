package com.ibm.csvapp.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.ibm.csvapp.constants.IKafkaConstants;
import com.ibm.csvapp.consumer.ConsumerCreator;
import com.ibm.csvapp.pojo.CustomObject;
import com.ibm.csvapp.producer.ProducerCreator;
import com.ibm.csvapp.utils.CSVFormatter;

public class App {
	
	private static String IP_CSV_FILE_NAME = null;
	private static String OP_CSV_FILE_NAME = null;
	private static CSVFormatter csvFormatter =null;
	private static FileWriter fileWriter = null;
	private static BufferedWriter bufferedWriter = null;
	
	public static void main(String[] args) {
	
		IP_CSV_FILE_NAME ="C:\\CSVFiles\\kafkainput.csv";
		OP_CSV_FILE_NAME="C:\\CSVFiles\\\\output.csv";
	/*	
	if (args != null){           
		
	    IP_CSV_FILE_NAME = args[0];
	}
	*/
		
    runProducer();
    try {
		Thread.sleep(2000);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    runConsumer();
	}
	
	static void runConsumer() {
		
		System.out.println("Starting Consumer..");
		
		File file = new File(OP_CSV_FILE_NAME);
		if (!file.exists()) {
		     try {
				file.createNewFile();
				fileWriter = new FileWriter(file);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		
		bufferedWriter= new BufferedWriter(fileWriter);
						
		Consumer<Long, CustomObject> consumer = ConsumerCreator.createConsumer();
		int noMessageToFetch = 0;		
		while (true) {
			final ConsumerRecords<Long, CustomObject> consumerRecords = consumer.poll(100);
			//System.out.println(consumerRecords.count());			
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {								
				String csvRecord=record.value().getPassengerId()+","+record.value().getSurvived()+","+record.value().getPclass()+","+record.value().getName()+","+record.value().getSex()+","+record.value().getAge()+","+record.value().getSibsp()+","+record.value().getParch()+","+record.value().getTicket()+","+record.value().getFare()+","+record.value().getCabin()+","+record.value().getEmbarked()+"\n";
				try {
					bufferedWriter.write(csvRecord);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			});
			consumer.commitAsync();						
		}		
		  try{
		      if(bufferedWriter!=null)
		    	  bufferedWriter.close();
		   }catch(Exception ex){
		       System.out.println("Error in closing the BufferedWriter"+ex);
		    }
		  consumer.close();
		  
		  System.out.println("Consumer Execution Completed..");
	}
	
	static void runProducer() {
			System.out.println("Starting Producer..");
			Producer<Long, CustomObject> producer = ProducerCreator.createProducer();
		    csvFormatter =new CSVFormatter();        
            List<CustomObject> csvRecordsList=csvFormatter.getCSVRecords(IP_CSV_FILE_NAME);
            
            for(CustomObject csvRecords:csvRecordsList)
            {
            	final ProducerRecord<Long, CustomObject> csvRecord = new ProducerRecord<Long, CustomObject>(IKafkaConstants.TOPIC_NAME,
    					csvRecords);
            	try {
            	RecordMetadata metadata = producer.send(csvRecord).get();
            	            	
            	} catch (ExecutionException e) {
    				System.out.println("Error in sending record");
    				System.out.println(e);
    			} catch (InterruptedException e) {
    				System.out.println("Error in sending record");
    				System.out.println(e);
    			}	
            } 
            System.out.println("Producer Execution Completed..");            
	}
}
