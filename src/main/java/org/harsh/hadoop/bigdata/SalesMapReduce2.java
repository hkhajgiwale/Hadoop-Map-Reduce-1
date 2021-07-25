package org.harsh.hadoop.bigdata;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SalesMapReduce2 {
	/*
	 * Logic for sorting country, item_type and year field
	 * 
	 * */
	private static class Sales implements WritableComparable<Sales> {
		Text country;
		Text item_type;
		IntWritable year;
		
		/*
		 * Returning hashcode for country, item_type and year items
		 * */
		@Override
		public int hashCode() {
			return Objects.hash(country, item_type, year);
		}
		
		/*
		 * Comparison logic with other object for sorting goes here
		 * */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Sales other = (Sales) obj;
			return Objects.equals(country, other.country) && Objects.equals(item_type, other.item_type)
					&& Objects.equals(year, other.year);
		}

		/*
		 * Constructor logic 
		 * */
		public Sales(Text country, Text item_type, IntWritable year) {
			this.country = country;
			this.item_type = item_type;
			this.year = year;
			
		}
		
		/*
		 * Initialization logic
		 *  */
		public Sales() {
			this.country = new Text();
			this.item_type = new Text();
			this.year = new IntWritable();
		}

		/*
		 * Writing the data to object
		 * */
		public void write(DataOutput out) throws IOException {
			this.country.write(out);
			this.item_type.write(out);
			this.year.write(out);
		}
		
		/*
		 * Reading fields using DataInput
		 *  */
		public void readFields(DataInput in) throws IOException {
			this.country.readFields(in);
			this.item_type.readFields(in);
			this.year.readFields(in);
		}
		
		/*
		 * This does sorting for all the three different fields one by one in the order of mention
		 * */
		public int compareTo(Sales pop) {
			if (pop == null) {
				return 0;
			}
			
			int countryCount = country.compareTo(pop.country);
			int itemTypeCount = item_type.compareTo(pop.item_type);
			int yearCount = year.compareTo(pop.year);
			
			if(countryCount != 0) {
				return countryCount;
			}
			
			else if (itemTypeCount != 0) {
				return itemTypeCount;
			}
			else {
				return yearCount;
			}
		}
		
		/*
		 * Overriding toString method to print the data
		 * */
		@Override
		public String toString() {
			return country.toString() + " : " + item_type.toString() + " " + year.toString();
		}
	}
	
	/*
	 * Custom partitioner created here and Partitioning data based on the hashkey
	 * 
	 * */
	public class SalesPartitioner extends Partitioner<Sales, DoubleWritable> {
		
	    @Override
	    public int getPartition(Sales key, DoubleWritable value, int i) {
	        return Math.abs(key.hashCode()) % i;
	    }
	}
	
	/*
	 * Mapper implementation done here
	 * */
	public static class SalesMapper2 extends Mapper<Object, Text, Sales, IntWritable> {
		/*
		 * Objects of SalesGetCsvDatac, csv fields and output for mapper created here
		 * */
		SalesGetCsvData csv = new SalesGetCsvData();
		Sales sales = new Sales();
		Text countryText = new Text();
		Text itemTypeText = new Text();
		IntWritable year = new IntWritable();
		IntWritable unitsSold = new IntWritable();
			
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * Getting field values using getCsvData method from SalesGetCsv class
			 * */
        	String[] csvData = csv.getCsvData(value);
			String country = csv.getCountry(csvData);
			String itemType = csv.getItemType(csvData);
			int orderYear = csv.getOrderDate(csvData);
			int unitSold = csv.getUnitsSold(csvData);countryText.set(new Text(country));
			
			/*
			 * Setting the values for fields
			 * */
			itemTypeText.set(new Text(itemType));
			year.set(orderYear);
			unitsSold.set(unitSold);
			
			/*
			 * Written to context are two fields
			 * 1. Sales object comprises of three fields: country, item_type and year. This is the key to reducer
			 * 2. Units sold this is the value to reducer
			 * */
			context.write(new Sales(countryText, itemTypeText, year),  unitsSold);
		}
	}
	
	/*
	 * Implementation of reducer goes here
	 * 
	 * */
	public static class SalesReducer2 extends Reducer<Sales, IntWritable, Sales, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Sales key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		    int unitSold = 0; 
		    
		    /*
		     * Sales is the key and units sold as the variable is the value where iteration is performed
		     * */
		    for (IntWritable val: values) {
		    	unitSold += val.get();
		    }
		    
		    /*
			 * Total count of units sold
			 * */
			result.set(unitSold);
			
			/*
			 * Context has two fields written
			 * 1. Sales object that comes as the output from mapper is written as the first field
			 * 2. Value of total units sold
			 *  */
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		//Everytime new directory is created for output hence, deleting the existing directory
		FileUtils.deleteDirectory(new File("/home/hadoop/eclipse/geosales_mapreduce/output-que-2"));
		Configuration conf =  new Configuration(); //Hadoop job config
		Job job = Job.getInstance(conf, "SalesMapReduce2"); //Hadoop job config instance created
		job.setJarByClass(org.harsh.hadoop.bigdata.SalesMapReduce2.class); //Name of the main class for creating the JAR file
		job.setMapperClass(SalesMapper2.class); //Name of the mapper class that executes the mapping logic
		job.setCombinerClass(SalesReducer2.class); //Data with same keys are combined here and passed to the reducer
		job.setReducerClass(SalesReducer2.class); //Name of the reducer class that executes the reducer logic
		job.setOutputKeyClass(Sales.class); //DataType of output key emitted. Here Sales class object emitted that consists of country, item, year
		job.setOutputValueClass(IntWritable.class);//DataType of output value emitted. Here IntWritable as sum of units sold is of integer type
		job.setPartitionerClass(SalesPartitioner.class); //Invoking custom partitioner class
		FileInputFormat.addInputPath(job, new Path(args[0])); //First Argument where we pass the csv file
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "-que-2")); //Second argument where we pass absolute path of the location with folder name. Here -que-2 is output since it is answer of question 2
		System.exit(job.waitForCompletion(true) ? 0 : 1); //Completion of job tracked here
	}
}
