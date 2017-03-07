package activte6;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	  private DoubleWritable result = new DoubleWritable();
  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	   double max = 0;
	     for (DoubleWritable valeur : values) {
	        if (valeur.get()>max)
	            max=valeur.get();
	     }
	     result.set(max);
	     context.write(key, result);}

  
}
