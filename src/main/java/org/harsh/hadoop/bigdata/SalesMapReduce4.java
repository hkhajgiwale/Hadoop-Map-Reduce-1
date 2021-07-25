package org.harsh.hadoop.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesMapReduce4 {
	
	static final int topN = 10; //Finding the top 10 records for every year
	
	/*
	 * Logic for sorting orderId and total_profit field
	 * 
	 * */
	private static class Sales implements WritableComparable<Sales> { 
		int orderId;
		double totalProfit;
		
		/*
		 * Returning hashcode for orderId and total_profit items
		 * */
		@Override
		public int hashCode() {
			return Objects.hash(orderId, totalProfit);
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
			return Objects.equals(orderId, other.orderId) && Objects.equals(totalProfit, other.totalProfit);
		}

		/*
		 * Constructor logic 
		 * */
		public Sales(int orderId, double totalProfit) {
			this.orderId = orderId;
			this.totalProfit = totalProfit;	
		}
		
		/*
		 * Initialization logic
		 *  */
		public Sales(Sales sales) {
			this.orderId = sales.orderId;
			this.totalProfit = sales.totalProfit;
		}
		
		@SuppressWarnings("unused")
		public Sales() {
			
		}
		
		/*
		 * Writing the data to object
		 * */
		public void write(DataOutput out) throws IOException {
			out.writeInt(orderId);
			out.writeDouble(totalProfit);
		}
		
		/*
		 * Reading fields using DataInput
		 *  */
		public void readFields(DataInput in) throws IOException {
			this.orderId = in.readInt();
			this.totalProfit = in.readDouble();
		}
		
		/*
		 * This does sorting for all the two different fields one by one in the order of mention
		 * */
		@Override
		public int compareTo(Sales other) {
			int result = Double.compare(totalProfit, other.totalProfit);
			if (result == 0) {
				result = Integer.compare(orderId, other.orderId);
			}
			return result;
		}
		
		/*
		 * Overriding toString method to print the data
		 * */
		@Override
		public String toString() {
			return orderId + " \t\t" + totalProfit + "\t\t";
		}
	}
	
	/*
	 * Mapper implementation done here
	 * */
	public static class SalesMapper4 extends Mapper<Object, Text, IntWritable, Sales> {
		/*
		 * HashMap to store the ordered profits for all the years
		 * */
		private HashMap<Integer, TreeSet<Sales>> orderedProfitsOfAllYears =  new HashMap<Integer, TreeSet<Sales>>();
		/*
		 * Object of SalesGetCsvData for mapper created here
		 * */
		SalesGetCsvData csv = new SalesGetCsvData();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * Getting field values using getCsvData method from SalesGetCsv class
			 * */
        	String[] csvData = csv.getCsvData(value);
			int year = csv.getOrderDate(csvData);
			double totalProfit = csv.getTotalProfit(csvData);
			int orderId = csv.getOrderId(csvData);
			
			/*
			 * Adding entries to hashmap
			 * Then creating TreeSet object to get the ordered profits as per year and since a set, it is unique
			 * */
			orderedProfitsOfAllYears.putIfAbsent(year, new TreeSet<>());
			TreeSet<Sales> orderedProfits = orderedProfitsOfAllYears.get(year);
			
			/*
			 * Since we have to remove the top 10 records, when added one extra after 10, it is removed
			 * */
			if(orderedProfits.size() >= topN) {
				orderedProfits.remove(orderedProfits.first());
			}
			
			/*
			 * Adding unique ordered profits to the treemap
			 * */
			orderedProfits.add(new Sales(orderId, totalProfit));
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			/*
			 * Fetching entries from HashMap and adding to the context
			 * This output of mapper would be served as input to reducer
			 * */
			for (Map.Entry<Integer, TreeSet<Sales>> entry : orderedProfitsOfAllYears.entrySet()) {
				TreeSet<Sales> sales = entry.getValue();
				int year = entry.getKey();
				for (Sales sale: sales) {
					context.write(new IntWritable(year), sale); 
				}
		    }
		}
	}
	
	public static class SalesReducer4 extends Reducer<IntWritable, Sales, IntWritable, Sales> {
		/*
		 * HashMap where year is the key and sales object that has orderedID and total profit is provided here
		 * */
		private HashMap<Integer, TreeSet<Sales>> orderedProfitsOfAllYears = new HashMap<Integer, TreeSet<Sales>>();
		
		public void reduce(IntWritable key, Iterable<Sales> values, Context context)  throws IOException, InterruptedException {
			
			/*
			 * Adding entries to hashmap if not already present
			 * */
			orderedProfitsOfAllYears.putIfAbsent(key.get(), new TreeSet<>());
			
			/*
			 * Creating tree set to put total profits and orderid that are ordered per year
			 * */
			TreeSet<Sales> orderedProfits = orderedProfitsOfAllYears.get(key.get());
			
			/*
			 * Iterating over sales object to get top 10 records, remove the one that is added after 10 and terminate the loop
			 * */
			for (Sales salesValue: values) {
				if (orderedProfits.size() >= topN) {
					orderedProfits.remove(orderedProfits.first());
				}
				orderedProfits.add(new Sales(salesValue));
			}	
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			/*
			 * Fetching entries from HashMap and adding to the context of reducer
			 * */
			for (Map.Entry<Integer, TreeSet<Sales>> entry : orderedProfitsOfAllYears.entrySet()) {
				TreeSet<Sales> sales = entry.getValue();
				int year = entry.getKey();
				
				for (Sales sale : sales) {
					context.write(new IntWritable(year), sale);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		//Everytime new directory is created for output hence, deleting the existing directory
		FileUtils.deleteDirectory(new File("/home/hadoop/eclipse/geosales_mapreduce/output-que-4"));
		Configuration conf =  new Configuration(); //Hadoop job config
		Job job = Job.getInstance(conf, "SalesMapReduce4"); //Hadoop job config instance created
		job.setJarByClass(org.harsh.hadoop.bigdata.SalesMapReduce4.class); //Name of the main class for creating the JAR file
		job.setMapperClass(SalesMapper4.class); //Name of the mapper class that executes the mapping logic
		job.setCombinerClass(SalesReducer4.class); //Data with same keys are combined here and passed to the reducer
		job.setReducerClass(SalesReducer4.class); //Name of the reducer class that executes the reducer logic
		job.setOutputKeyClass(IntWritable.class); //DataType of output key emitted. Here Sales class object emitted that consists of orderId, totalProfit, year
		job.setOutputValueClass(Sales.class); //DataType of output value emitted.
		FileInputFormat.addInputPath(job, new Path(args[0])); //First Argument where we pass the csv file
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "-que-4")); //Second argument where we pass absolute path of the location with folder name. Here -que-4 is output since it is answer of question 4
		System.exit(job.waitForCompletion(true) ? 0 : 1); //Completion of job tracked here
	}
}
