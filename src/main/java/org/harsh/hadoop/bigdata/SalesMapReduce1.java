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

/*
 *
 * Question: Find Average unit_price by country for a given item type in a certain year
 *
 * */
public class SalesMapReduce1 {
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
			return country.toString() + "\t\t\t" + item_type.toString() + "\t\t\t" + year.toString() + "\t\t\t";
		}
	}
	
	/*
	 * Custom partitioner created here and Partitioning data based on the hashkey
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
	public static class SalesMapper1 extends Mapper<Object, Text, Sales, DoubleWritable> {
		/*
		 * Objects of SalesGetCsvDatac, csv fields and output for mapper created here
		 * */
		SalesGetCsvData csv = new SalesGetCsvData();
		Sales sales = new Sales();
		Text countryText = new Text();
		Text itemTypeText = new Text();
		IntWritable year = new IntWritable();
		DoubleWritable unitPrice = new DoubleWritable();
		
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	/*
			 * Getting field values using getCsvData method from SalesGetCsv class
			 * */
        	String[] csvData = csv.getCsvData(value);
			String country = csv.getCountry(csvData);
			String itemType = csv.getItemType(csvData);
			int orderYear = csv.getOrderDate(csvData);
			double unitPriceAmount = csv.getUnitPrice(csvData);
			
			/*
			 * Setting the values for fields
			 * */
			countryText.set(new Text(country));
			itemTypeText.set(new Text(itemType));
			year.set(orderYear);
			unitPrice.set(unitPriceAmount);
			
			/*
			 * Written to context are two fields
			 * 1. Sales object comprises of three fields: country, item_type and year. This is the key to reducer
			 * 2. Unit price this is the value to reducer
			 * */
			context.write(new Sales(countryText, itemTypeText, year),  unitPrice);
		}
	}
	
	/*
	 * Implementation of reducer goes here
	 * 
	 * */
	public static class SalesReducer1 extends Reducer<Sales, DoubleWritable, Sales, DoubleWritable> {
		public void reduce(Sales key, Iterable<DoubleWritable> values, Context context)  throws IOException, InterruptedException {
		    double unitPrice = 0; 
		    int count = 0;
		    
		    /*
		     * Sales is the key and unit_price as the variable is the value where iteration is performed
		     * */
		    for(DoubleWritable val : values) {
		    	count++;
		    	unitPrice += val.get();
		    }
		    
			/*
			 * Logic for average calculation
			 * */
			double averagePrice = unitPrice/count;
			
			/*
			 * Context has two fields written
			 * 1. Sales object that comes as the output from mapper is written as the first field
			 * 2. Value of Average unit price
			 *  */
			context.write(key, new DoubleWritable(averagePrice));
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		//Everytime new directory is created for output hence, deleting the existing directory
		FileUtils.deleteDirectory(new File("/home/hadoop/eclipse/geosales_mapreduce/output-que-1"));
		Configuration conf =  new Configuration(); //Hadoop job config
		Job job = Job.getInstance(conf, "SalesMapReduce1"); //Hadoop job config instance created
		job.setJarByClass(org.harsh.hadoop.bigdata.SalesMapReduce1.class); //Name of the main class for creating the JAR file
		job.setMapperClass(SalesMapper1.class); //Name of the mapper class that executes the mapping logic
		job.setReducerClass(SalesReducer1.class); //Name of the reducer class that executes the reducer logic
		job.setOutputKeyClass(Sales.class); //DataType of output key emitted. Here Sales class object emitted that consists of country, item, year
		job.setOutputValueClass(DoubleWritable.class); //DataType of output value emitted. Here DoubleWritable as average is of double type
		job.setPartitionerClass(SalesPartitioner.class); //Invoking custom partitioner class
		FileInputFormat.addInputPath(job, new Path(args[0])); //First Argument where we pass the csv file
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "-que-1")); //Second argument where we pass absolute path of the location with folder name. Here -que-1 is output since it is answer of question 1
		System.exit(job.waitForCompletion(true) ? 0 : 1); //Completion of job tracked here
	}
}
