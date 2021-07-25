package org.harsh.hadoop.bigdata;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.io.Text;


public class SalesGetCsvData {
	
	public String[] getCsvData(Text value) {
		/*
		 * Parsing the CSV File here
		 * Splitting the file based on comma as a delimiter
		 * */
		String valueString = value.toString();
		String[] csvData = valueString.split(",");
		return csvData;
	}

	public String getCountry(String[] csvData) {
		/*
		 * Getting country field from csv
		 */
		return new String(csvData[2]);
	}
		
	public String getItemType(String[] csvData) {
		/*
		 * Getting item_type field from csv
		 */
		return new String(csvData[3]);
	}

	@SuppressWarnings("deprecation")
	public int getOrderDate(String[] csvData) {
		/*
		 * Getting year field from csv
		 * Converting the date to standard format and extracting year out of it
		 * 
		 * */
		Integer year = null;
		try {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
			Date date = format.parse(csvData[6]);
			year = 1900 + date.getYear();
		}
		catch (ParseException e) {
			e.printStackTrace();
		}
		return year;
	}

	public double getUnitPrice(String[] csvData) {
		/*Getting unit_price field from csv*/
		return Double.parseDouble(csvData[10]);
	}
	
	public double getTotalProfit(String[] csvData) {
		/*Getting total_profit field from csv*/
		return Double.parseDouble(csvData[14]);
	}
	
	public int getOrderId(String[] csvData) {
		/*Getting order_id field from csv*/
		return Integer.parseInt(csvData[7]);
	}
	
	public int getUnitsSold(String[] csvData) {
		/*Getting unit_sold field from csv*/
		return Integer.parseInt(csvData[9]);
	}
}
