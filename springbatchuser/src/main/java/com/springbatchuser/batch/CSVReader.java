package com.springbatchuser.batch;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Time;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import com.springbatchuser.model.User;

public class CSVReader implements ResourceAwareItemReaderItemStream<User> {
	private Resource resource;

    private File file=null;

    private CSVParser csvParser;

    private Reader reader;

    private List<CSVRecord> csvRecords;

    private long noOfRecords=0;

    private int currentRecord=0;

    
	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		try {
            file=resource.getFile();
            reader=new FileReader(file);
            CSVFormat csvFormat=CSVFormat.DEFAULT.withDelimiter(',');
            csvParser=new CSVParser(reader,csvFormat.withHeader("id","name","dept","salary").withFirstRecordAsHeader());
            csvRecords= csvParser.getRecords();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
		

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws ItemStreamException {
		resource=null;
        file=null;
        reader=null;
        currentRecord=0;
		
	}

	@Override
	public User read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		
		System.out.println("current record "+currentRecord);
		
		
		while(currentRecord<csvRecords.size()){
            CSVRecord csvRecord=csvRecords.get(currentRecord);
            User userReport=new User();
            userReport.setId(Integer.valueOf(csvRecord.get("id")));
            userReport.setDept(csvRecord.get("dept"));
            userReport.setName(csvRecord.get("name"));
            userReport.setSalary(Integer.valueOf(csvRecord.get("salary")));
         
            currentRecord++;
            return userReport;
        }
		
		return null;
		
        
	}

	@Override
	public void setResource(Resource resource) {
		this.resource=resource;
		
	}

}
