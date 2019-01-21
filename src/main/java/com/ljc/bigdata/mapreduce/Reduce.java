package com.ljc.bigdata.mapreduce;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.ljc.bigdata.measures.Counters.ANALYTICS;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(Reduce.class);
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		context.getCounter(ANALYTICS.NUM_REDUCERS).increment(1);
	}
	
	@Override
	public void reduce(final Text key, 
			final Iterable<IntWritable> values, Context context){
		/*
		Text key: Each City
		Iterable<DoubleWritable> values: Each Temperature record, one by one
		*/
		context.getCounter(ANALYTICS.NUM_GRUPOS).increment(1);
		
		LOG.info("Ciudad: " + key.toString());
		int total = 0;
		
		for(final IntWritable temp: values){
			total += temp.get();
		}
		
		String salida = "{\"suit\":\"" + key.toString() + "\",\"sum_ranks\":\"" + total + "ºC\"}";
		LOG.info(salida);
		
		try {
			context.write(key, new IntWritable(total));;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Autoca-generated catch block
			LOG.error("Error en el mapeo :InterruptedException: " + e.getMessage());
			e.printStackTrace();
		}
		context.getCounter(ANALYTICS.LINES_WRITTEN).increment(1);
	}
}
