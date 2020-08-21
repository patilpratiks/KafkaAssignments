package com.ibm.csvapp.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.ibm.csvapp.pojo.CustomObject;

public class CSVFormatter {

	public List<CustomObject> getCSVRecords(String file)
	{
		List<CustomObject> csvRecordsList=new ArrayList<>();
		Path pathToFile=Paths.get(file);
		
		try(BufferedReader br =Files.newBufferedReader(pathToFile))
		{
			String row=br.readLine();
			
			while((row = br.readLine() ) != null)
			{
				String [] attributes=row.split(",");						
				CustomObject custobj=getOneRecord(attributes);
				csvRecordsList.add(custobj);
			}				
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		
		return csvRecordsList;
	}
	
	public CustomObject getOneRecord(String[] attributes)
	{	
		CustomObject singleRecord=new CustomObject(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4],attributes[5],attributes[6],attributes[7],attributes[8],attributes[9],attributes[10],attributes[11]);
		return singleRecord;
	}		
}
