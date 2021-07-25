package org.harsh.hadoop.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
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

/*
 *
 * Question: Find the max and min units_sold in any order for each year by country for a given item type. Use a custom partitioner class instead of default hash based.
 *
 * */


public class SalesMapReduce3 {
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
		 * Comparison logic with other object goes here
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
	public static class SalesMapper3 extends Mapper<Object, Text, Sales, IntWritable> {
		/*
		 * Objects of SalesGetCsvData, csv fields and output for mapper created here
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
			int unitSold = csv.getUnitsSold(csvData);

			/*
			 * Setting the values for fields
			 * */
			countryText.set(new Text(country));
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
	public static class SalesReducer3 extends Reducer<Sales, IntWritable, Sales, IntWritable> {
		public void reduce(Sales key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		    int min = Integer.MAX_VALUE, max = 0;
		    Iterator<IntWritable> iterator = values.iterator();
		    
		    
		    /*
		     * Sales is the key and units sold as the variable is the value where iteration is performed
		     * */
		    while(iterator.hasNext()) {
		    	int value = iterator.next().get();
		    	
		    	if (value < min) { //Finding the max units sold for a given year for a type of item sold
		    		min = value;
		    	}
		    	
		    	if (value > max) { //Finding the max units sold for a given year for a type of item sold
		    		max = value;
		    	}
		    }
		    
		    context.write(key, new IntWritable(max)); //Writing sales object as key and max units sold as value
		    context.write(key, new IntWritable(min)); //Writing sales object as key and min units sold as value
		}
	}
	
	public static void main(String[] args) throws Exception {
		//Everytime new directory is created for output hence, deleting the existing directory
		FileUtils.deleteDirectory(new File("/home/hadoop/eclipse/geosales_mapreduce/output-que-3"));
		Configuration conf =  new Configuration(); //Hadoop job config
		Job job = Job.getInstance(conf, "SalesMapReduce3"); //Hadoop job config instance created
		job.setJarByClass(org.harsh.hadoop.bigdata.SalesMapReduce3.class); //Name of the main class for creating the JAR file
		job.setMapperClass(SalesMapper3.class); //Name of the mapper class that executes the mapping logic
		job.setCombinerClass(SalesReducer3.class); //Data with same keys are combined here and passed to the reducer
		job.setReducerClass(SalesReducer3.class); //Name of the reducer class that executes the reducer logic
		job.setOutputKeyClass(Sales.class); //DataType of output key emitted. Here Sales class object emitted that consists of country, item, year
		job.setOutputValueClass(IntWritable.class); //DataType of output value emitted. Here IntWritable as min and max of units sold is of integer type
		job.setPartitionerClass(SalesPartitioner.class); //Invoking custom partitioner class
		FileInputFormat.addInputPath(job, new Path(args[0])); //First Argument where we pass the csv file
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "-que-3")); //Second argument where we pass absolute path of the location with folder name. Here -que-3 is output since it is answer of question 3
		System.exit(job.waitForCompletion(true) ? 0 : 1); //Completion of job tracked here
	}
}
