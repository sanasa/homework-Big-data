package activite5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class StubMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	 private Text categorie = new Text();
	 private DoubleWritable vente = new DoubleWritable();
@Override
public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException,NumberFormatException {
	 String[] split = value.toString().split("\t+");
	    categorie.set(split[3]);
	    if (split.length > 2) {
	      
	        vente.set(Double.parseDouble(split[4]));
	        context.write(categorie, vente);
}}}

