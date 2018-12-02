package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
			String key = tokens[i].trim();
			String val = tokens[i + 1];
			map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String value_str = value.toString();
			Map<String, String> value_map = transformXmlToMap(value_str);

			if (value_map.get("Id") != null) {
				Text userId = new Text(value_map.get("Id"));
				Text record = new Text(value_str);
				String userRep = value_map.get("Reputation").toString();
				repToRecordMap.put(Integer.parseInt(userRep), record);
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			int i = 0;

			for (Integer key : repToRecordMap.descendingKeySet()) {
				Text record = repToRecordMap.get(key);
				System.out.println(key + " => " + record);
				Text record_text = new Text(record.toString());
				
				context.write(NullWritable.get(), record_text);
				
				i ++;				
				if (i > 9) {
					break;
				}
			}
		}
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, ImmutableBytesWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Integer> repToRecordMap = new TreeMap<Integer, Integer>();

		public void reduce(NullWritable key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
			try {
				for (Text record_text : records) {
					Map<String, String> record = transformXmlToMap(record_text.toString());

					if (record.get("Id") != null) {

						Integer userId = Integer.parseInt(record.get("Id"));
						Integer userRep = Integer.parseInt(record.get("Reputation"));
						
						System.out.println("Reducer Adding rep " + userRep + " from user " + userId);
						repToRecordMap.put(userRep, userId);
					}
				}

				int i = 0;

				for (Integer repKey : repToRecordMap.descendingKeySet()) {
					i++;
					// create hbase put with rowkey as id
					Put insHBase = new Put(Bytes.toBytes(i));

					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(repKey));
					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(repToRecordMap.get(repKey)));

					context.write(null, insHBase);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
    }

    public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "top ten");
		job.setJarByClass(TopTen.class);

		job.setMapperClass(TopTenMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.waitForCompletion(true);
    }
}
