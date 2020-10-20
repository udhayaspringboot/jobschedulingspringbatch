package com.springbatchuser.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.springbatchuser.batch.DBWriter;
import com.springbatchuser.batch.Processor;
import com.springbatchuser.model.User;

@Configuration
@EnableBatchProcessing
@EnableScheduling
public class SpringBatchConfig {

	@Autowired
	private DBWriter dbWriter;
	@Autowired
	private Processor processor;
	
	List<Resource> lisSrc = new ArrayList<>();
	List<Resource> lisDes = new ArrayList<>();
	
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Value("file:f:/afiles/useslist/user*.csv")
	private Resource[] inputResources;
	/*
	 * @Bean public Job job() {
	 * 
	 * 
	 * 
	 * 
	 * Step step1 = stepBuilderFactory.get("ETL-file-load2") .<User,
	 * User>chunk((inputResources.length)) .processor(itemProcessor)
	 * .writer(dbWriter).build();
	 * 
	 * 
	 * 
	 * return jobBuilderFactory.get("ETL-Load") .incrementer(new RunIdIncrementer())
	 * .start(step)//.next(step1) .build();
	 * 
	 */
	
	@Bean
    public Job job() {
        return jobBuilderFactory
                .get("job")
                .incrementer(new RunIdIncrementer())
                .start(step())
                .build();
    }
	@Bean
    
    public Step step ()
    {
    return stepBuilderFactory.get("step")
    .<User, User>chunk((3))
    
    .reader(multiResourceItemReader2())
    
    .processor(processor)
    
    .writer(dbWriter)
    .taskExecutor(taskExecutor())
    .build();
    }
    
	/*
	 * @Bean public Step step1() { return stepBuilderFactory.get("step1").<User,
	 * User>chunk(5) .reader(multiResourceItemReader()) .writer(writer()) .build();
	 * }
	 */
    
    
    
    
    

   
    @Bean
    @Primary
   // @StepScope
    public FlatFileItemReader<User> reader() 
    {
        // Create reader instance
        FlatFileItemReader<User> reader = new FlatFileItemReader<User>();
        // Set number of lines to skips. Use it if file has header rows.
        System.out.println("get file names in flat file "+reader.toString());
        reader.setLinesToSkip(1);
        System.out.println(reader.toString());
        // Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper<User>() {
            {
                // 3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[] { "id","name","dept","salary" });
                    }
                });
                // Set values in User class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
                    {
                        setTargetType(User.class);
                    }
                });
            }
        });
       // System.out.println("reader is "+reader.toString());
      // reader.open(new ExecutionContext());
        return reader;
    }
    
    @Bean
	@Qualifier
	@StepScope
	public MultiResourceItemReader<User> multiResourceItemReader2() {
		MultiResourceItemReader<User> resourceItemReader = new MultiResourceItemReader<User>();
		ClassLoader cl = this.getClass().getClassLoader();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
      //  Resource[] res = null;
  
		try {
			Resource[] resources = resolver.getResources("file:f:/afiles/useslist/user*.csv" );
			for (Resource resource : resources) {
				System.out.println("resource names FIRST" + resource.getFilename());
				lisSrc.add(resource);
			}
			resourceItemReader.setResources(resources);
			resourceItemReader.setDelegate(reader());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
		
		
		
		System.out.println("new file " + resourceItemReader.getCurrentResource());
		
		return resourceItemReader;
    }

	@Bean
	@Qualifier
	@StepScope
	public MultiResourceItemReader<User> multiResourceItemReader() {
		MultiResourceItemReader<User> resourceItemReader = new MultiResourceItemReader<User>();
		if(lisSrc.size() == 0) {
		
		for (Resource resource : inputResources) {
			System.out.println("resource names FIRST" + resource.getFilename());
			lisSrc.add(resource);
		}
		resourceItemReader.setResources(inputResources);
		resourceItemReader.setDelegate(reader());
		
		
		System.out.println("new file " + resourceItemReader.getCurrentResource());
		
		return resourceItemReader;
		}else
		{
			ClassLoader cl = this.getClass().getClassLoader();
            ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(cl);
            Resource[] res = null;
        try {
			Resource[] resources = resolver.getResources("file:f:/afiles/useslist/user*.csv" );
            SpringBatchConfig sfg = new SpringBatchConfig();
			
			for (Resource resource : resources) {
			System.out.println("resource names SECOND " + resource.getFilename());
			lisDes.add(resource);
		}resourceItemReader.setResources(resources);
		resourceItemReader.setDelegate(reader());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
			int size = lisDes.size()-lisSrc.size()-1;
			//List<Resource> sbList = lisDes.subList((lisDes.size()-lisSrc.size()-1), lisDes.size());
			 List<Resource> subLisRes = lisDes.subList(lisSrc.size(),(lisDes.size()));
			 
			 for (Resource resource : subLisRes) {
			
				 System.out.println("SUBLIST"+resource.getFilename());
			}
			 res = subLisRes.toArray(res);
			
			
			
			System.out.println("new file " + resourceItemReader.getCurrentResource());
			
			return resourceItemReader;
			
			
		}
		//return null;
	}

	/*
	 * @Bean //@Scheduled(fixedDelay = 10) public MultiResourceItemReader<User>
	 * itemReader() {
	 * 
	 * MultiResourceItemReader<User> flatFileItemReader = new
	 * MultiResourceItemReader<>(); List<FileSystemResource> fileSystemResources =
	 * new ArrayList<>();
	 * 
	 * try { Stream<Path> stream = Files.list(Paths.get("F:\\afiles\\useslist\\"));
	 * stream.forEach(x -> { fileSystemResources.add(new
	 * FileSystemResource(x.toFile())); }); Resource[] resources = {}; resources =
	 * fileSystemResources.toArray(resources);
	 * 
	 * flatFileItemReader.setResources(resources);
	 * flatFileItemReader.setDelegate(csvReader());
	 * flatFileItemReader.setStrict(Boolean.FALSE); } catch (IOException e) {
	 * e.printStackTrace(); } return flatFileItemReader; }
	 */
	/*
	 * @Bean public LineMapper<User> lineMapper() {
	 * 
	 * DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();
	 * DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
	 * 
	 * lineTokenizer.setDelimiter(","); lineTokenizer.setStrict(false);
	 * lineTokenizer.setNames(new String[]{"id", "name", "dept", "salary"});
	 * 
	 * BeanWrapperFieldSetMapper<User> fieldSetMapper = new
	 * BeanWrapperFieldSetMapper<>(); fieldSetMapper.setTargetType(User.class);
	 * 
	 * defaultLineMapper.setLineTokenizer(lineTokenizer);
	 * defaultLineMapper.setFieldSetMapper(fieldSetMapper);
	 * 
	 * return defaultLineMapper; }
	 */
	@Bean
	public TaskExecutor taskExecutor() {
		/*
		 * SimpleAsyncTaskExecutor asyncTaskExecutor = new
		 * SimpleAsyncTaskExecutor("spring_batch");
		 * asyncTaskExecutor.setConcurrencyLimit((inputResources.length / 2 + 1));
		 */
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(10);
		taskExecutor.afterPropertiesSet();
                    taskExecutor.getActiveCount();
                     
                   
		return taskExecutor;
	}

	/*
	 * @Bean
	 * 
	 * @StepScope public CSVReader csvReader() { return new CSVReader(); }
	 */

}
