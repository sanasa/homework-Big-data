package activite7;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	  private DoubleWritable result = new DoubleWritable();
	  private Text nombre = new Text();
  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	  double somme = 0;
	  int nbr =0;
	     for (DoubleWritable valeur : values) {
	       somme += valeur.get();
	        nbr++;
	     }
	     result.set(somme);
	     nombre.set(Integer.toString(nbr));
	    
	     context.write(nombre, result);
	   }
}