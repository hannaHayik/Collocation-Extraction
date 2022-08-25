import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3 {
	public static int b = 0;

	//Using this function will cause Iterable<Text>values to be NULL (it's an iterator so it's a single pass on the values)
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str += val.toString() + " H3 ";
		return str;
	}

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		public static double Lfunction(double k, double n, double x) {
			return (Math.pow(x, k)) * Math.pow((1.0 - x), (n - k));
		}

		public static double lambdaLog(double c1, double c2, double c12, double N) {
			double res = 0.0;
			try {
				double p = c2 / N;
				double p1 = c12 / c1;
				double p2 = (c2 - c12) / (N - c1);
				res = (-2) * (Math.log(Lfunction(c12, c1, p)) + Math.log(Lfunction(c2 - c12, N - c1, p))
						- Math.log(Lfunction(c12, c1, p1)) - Math.log(Lfunction(c2 - c12, N - c1, p2)));
				if (Double.isInfinite(res) || Double.isNaN(res))
					return 0.0;
				return res;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return res;

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] tmp = value.toString().split("\t");
				String[] prevKey = tmp[0].split(",");
				//String val = tmp[1];
				String w1 = prevKey[0];
				String w2 = prevKey[1];
				String year = prevKey[2];
				Double c1 = Double.valueOf(prevKey[3]);
				Double c2 = Double.valueOf(prevKey[4]);
				Double c12 = Double.valueOf(tmp[1]);
				String NcounterName = "N_" + year;
				//lambdaLog,w1,w2,year	cw1w2
				//multiply by -1 so Hadoop sorts it and in reduce() we multiply by -1 again to get the 1st 100 result DESC
				context.write(new Text(((-1) * lambdaLog(c1, c2, c12, Double.valueOf(context.getCounter("NCounter", NcounterName).getValue()))) + " " + w1 + " " + w2 + " " + year), new Text("" + c12));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		int count = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			count++;
			System.out.println("Reduce(): => key: " + key.toString() + " count: " + count);
			if(count<=100) {
				String[] tmp = key.toString().split(" ");
				Double myVal = Double.valueOf(tmp[0]) * (-1);
				context.write(new Text(tmp[3] + " " + tmp[1] + " " + tmp[2]), new Text(String.valueOf(myVal)));
				//write to output: year,w1,w2	lambdaLog
			}		
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] parti = {"1530", "1540", "1560", "1620", "1670", "1680","1690", "1700", "1750", "1760",
					"1780", "1790", "1800", "1810", "1820", "1830", "1840", "1850", "1860", "1870", "1880",
					"1890", "1900", "1910", "1920", "1930", "1940", "1950", "1960", "1970", "1980", "1990", "2000"};
			String year = key.toString().split(" ")[3];
			for(int i=0; i<parti.length; i++)
				if(parti[i].equalsIgnoreCase(year))
					return (i % numPartitions);
			System.out.println("In Step3 Partitioner!  ---> key: " + key.toString() + "  value: " + value.toString() + " year: " + year);
			return 0;
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			try {
				for (Text txt : values) {
					count += Integer.parseInt(txt.toString());
				}
				context.write(key, new Text(String.valueOf(count)));
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(
						"Error in Step3 Combiner --> key: " + key.toString() + " values: " + printerHanna(values));
			}
		}
	}


}
