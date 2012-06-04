/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: ConCmpt.java
 - HCC: Find Connected Components of graph
Version: 2.0
***********************************************************************/

package com.pegasus;

import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


class ResultInfo
{
	public int changed;
	public int unchanged;
};

public class ConCmpt extends Configured implements Tool 
{
    public static int MAX_ITERATIONS = 2048;
	public static int changed_nodes[] = new int[MAX_ITERATIONS];
	public static int unchanged_nodes[] = new int[MAX_ITERATIONS];
	static int iter_counter = 0;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: join matrix elements and vector elements using matrix.dst_id and vector.row_id
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase	implements Mapper<LongWritable, Text, Text, Text>
    {
		private final Text from_node = new Text();
		private final Text to_node = new Text();
		int make_symmetric = 0;

		public void configure(JobConf job) {
			make_symmetric = Integer.parseInt(job.get("make_symmetric"));

			System.out.println("MapStage1 : make_symmetric = " + make_symmetric);
		}

		public void map (final LongWritable key, 
						final Text value, 
						final OutputCollector<Text, Text> output, 
						final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))							// ignore comments in the edge file
				return;

			final String[] line = line_text.split("\t");
			if( line.length < 2 ) {
				return;
			}

			if( line[1].startsWith("m") ) {							// input sample: 11	msu5
				from_node.set(line[0]);
				output.collect(from_node, new Text(line[1]));
			} else {												// (src, dst) edge
				to_node.set(line[1]);

				output.collect(to_node, new Text(line[0]));			// invert dst and src

				if( make_symmetric == 1 ) {							// make inverse egges
					from_node.set(line[0]);

					if( !((to_node.toString()).equals(from_node.toString())) )
						output.collect(from_node, new Text(line[1]));
				}
			}
		}
	}

    public static class	RedStage1 extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {

		public void configure(JobConf job) {
		}

		public void reduce (final Text key, 
							final Iterator<Text> values, 
							OutputCollector<Text, Text> output, 
							final Reporter reporter) throws IOException
        {
			String component_id_str = "";
			Set<String> from_nodes_set = new HashSet<String>();
			boolean self_contained = false;
			String line="";

			while (values.hasNext()) {
				Text from_cur_node = values.next();
				line = from_cur_node.toString();

				if (line.startsWith("m")) {							// component info
					if( component_id_str.length() == 0 )
						component_id_str = line.substring(3);
				} else {											// edge line
					String from_node = line;
					from_nodes_set.add( from_node );
					if( (key.toString()).equals(from_node))
						self_contained = true;
				}
			}

			if( self_contained == false )							// add self loop, if not exists.
				from_nodes_set.add(key.toString());

			Iterator from_nodes_it = from_nodes_set.iterator();
			while (from_nodes_it.hasNext()) {
				String component_info;
				String cur_key = (String)from_nodes_it.next();

				if( cur_key.equals(key.toString()) ) {
					component_info = "msi" + component_id_str;
					output.collect(new Text(cur_key), new Text(component_info));
				} else {
					component_info = "moi" + component_id_str;
					output.collect(new Text(cur_key), new Text(component_info));
				}
			}
		}
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STAGE 2: merge partial component ids.
	//  - Input: partial component ids
	//  - Output: combined component ids
    ////////////////////////////////////////////////////////////////////////////////////////////////
	public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
		// Identity mapper
		public void map (final LongWritable key, 
						final Text value, 
						final OutputCollector<Text, Text> output, 
						final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");

			output.collect(new Text(line[0]), new Text(line[1]) );
		}
    }

    public static class RedStage2 extends MapReduceBase	implements Reducer<Text, Text, Text, Text>
    {
		public void reduce (final Text key, 
							final Iterator<Text> values, 
							final OutputCollector<Text, Text> output, 
							final Reporter reporter) throws IOException
        {
			//int i;
			String out_val ="ms";
			boolean bSelfChanged = false;
			char changed_prefix = 'x';
			//String complete_cistring = "";
			String cur_min_nodeid = "";
			String self_min_nodeid = "";

			while (values.hasNext()) {
				String cur_ci_string = values.next().toString();
				String cur_nodeid = "";
				try
				{
					cur_nodeid = cur_ci_string.substring(3);
				}
				catch (Exception ex)
				{
					System.out.println("Exception! cur_ci_string=[" + cur_ci_string + "]");
				}

				if( cur_ci_string.charAt(1) == 's' ) {				// for calculating individual diameter
					self_min_nodeid = cur_nodeid;
				}

				if( cur_min_nodeid.equals("")) {
					cur_min_nodeid = cur_nodeid;
				} else {
					if( cur_nodeid.compareTo(cur_min_nodeid) < 0 )
						cur_min_nodeid = cur_nodeid;
				}
			}

			if( self_min_nodeid.equals(cur_min_nodeid) ) {
				changed_prefix = 'f';								// unchanged
			} else
				changed_prefix = 'i';								// changed

			out_val = out_val + changed_prefix + cur_min_nodeid;

			output.collect(key, new Text( out_val ) );
		}
    }


    public static class CombinerStage2 extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
		public void reduce (final Text key, 
							final Iterator<Text> values, 
							final OutputCollector<Text, Text> output, 
							final Reporter reporter) throws IOException
        {
			String out_val ="moi";
			String cur_min_nodeid = "";

			while (values.hasNext()) {
				Text cur_value_text = values.next();
				String cur_ci_string = cur_value_text.toString();
				String cur_nodeid = "";
				try
				{
					cur_nodeid = cur_ci_string.substring(3);
				}
				catch (Exception ex)
				{
					System.out.println("Exception! cur_ci_string=[" + cur_ci_string + "]");
				}

				if( cur_ci_string.charAt(1) == 's' ) {			// for calculating individual diameter
					output.collect(key, new Text(cur_value_text) );
					continue;
				}

				if( cur_min_nodeid.equals("")) {
					cur_min_nodeid = cur_nodeid;
				} else {
					if( cur_nodeid.compareTo(cur_min_nodeid) < 0 )
						cur_min_nodeid = cur_nodeid;
				}
			}

			if( !cur_min_nodeid.equals("")) {
				out_val += cur_min_nodeid;
	
				output.collect(key, new Text( out_val ) );
			}
		}
    }


    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate number of nodes whose component id changed/unchanged.
	//  - Input: current component ids
	//  - Output: number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
		// output : f n		( n : # of node whose component didn't change)
		//          i m		( m : # of node whose component changed)
		public void map (final LongWritable key, 
						final Text value, 
						final OutputCollector<Text, Text> output, 
						final Reporter reporter) throws IOException
		{
			if (value.toString().startsWith("#"))
				return;

			final String[] line = value.toString().split("\t");
			char change_prefix = line[1].charAt(2);

			output.collect(new Text(Character.toString(change_prefix)), new Text(Integer.toString(1)) );
		}
    }

    public static class	RedStage3 extends MapReduceBase	implements Reducer<Text, Text, Text, Text>
    {
		public void reduce (final Text key, 
							final Iterator<Text> values, 
							final OutputCollector<Text, Text> output, 
							final Reporter reporter) throws IOException
		{
			int sum = 0;

			while (values.hasNext()) {
				final String line = values.next().toString();
				int cur_value = Integer.parseInt(line);

				sum += cur_value;
			}

			output.collect(key, new Text(Integer.toString(sum)) );
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path 		= null;
    protected Path curbm_path 		= null;
    protected Path tempbm_path 		= null;
    protected Path nextbm_path 		= null;
	protected Path output_path 		= null;
	protected Path all_vertices		= null;
	protected Path grapherOut_path	= null;
	protected int nreducers 		= 1;
	protected int cur_iter 			= 1;
	protected int make_symmetric 	= 0;		// convert directed graph to undirected graph
	protected String local_output_path;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new ConCmpt(), args);

		System.exit(result);
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {

		Configuration conf = getConf();
		final FileSystem fs = FileSystem.get(conf);
		edge_path 			= new Path(conf.get("edge_path"));
		all_vertices		= new Path(conf.get("all_vertices"));
		curbm_path 			= new Path(conf.get("iteration_state"));
		tempbm_path 		= new Path(conf.get("stage1out"));
		nextbm_path 		= new Path(conf.get("stage2out"));
		output_path 		= new Path(conf.get("stage3out"));
		grapherOut_path		= new Path(conf.get("grapherout"));
		nreducers 			= Integer.parseInt(conf.get("num_reducers"));
		local_output_path 	= conf.get("local_output");


		// initital cleanup
		fs.delete(tempbm_path,true);
		fs.delete(nextbm_path,true);
		fs.delete(output_path,true);
		fs.delete(curbm_path, true);
		fs.delete(grapherOut_path, true);
		FileUtil.fullyDelete(new File(local_output_path));
		fs.mkdirs(curbm_path);
		//fs.mkdirs(grapherOut_path);

		
		FileStatus[] statusArray = fs.listStatus(all_vertices);
		for (int index = 0; index < statusArray.length; index++){
			Path temp = statusArray[index].getPath();
			FileUtil.copy(fs, temp, fs, curbm_path, false, conf);
		}

		make_symmetric = 1;

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");

		// Iteratively calculate neighborhood function. 
		// rotate directory
		for (int i = cur_iter; i < MAX_ITERATIONS; i++) {
			cur_iter++;

			System.out.println("configStage1");
			JobClient.runJob(configStage1());
			System.out.println("configStage2");
			JobClient.runJob(configStage2());
			System.out.println("configStage3");
			JobClient.runJob(configStage3());

			FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));


			// copy neighborhood information from HDFS to local disk, and read it!
			String new_path = local_output_path + "/" + i;
			fs.copyToLocalFile(output_path, new Path(new_path) ) ;
			ResultInfo ri = readIterationOutput(new_path);

			changed_nodes[iter_counter] = ri.changed;
			changed_nodes[iter_counter] = ri.unchanged;

			iter_counter++;

			System.out.println("Hop " + i + " : changed = " + ri.changed + ", unchanged = " + ri.unchanged);
			fs.delete(curbm_path);
			fs.delete(tempbm_path);
			fs.delete(output_path);
			fs.rename(nextbm_path, curbm_path);

			// Stop when the minimum neighborhood doesn't change
			if( ri.changed == 0 ) {
				System.out.println("All the component ids converged. Finishing...");
				fs.rename(curbm_path,grapherOut_path);
				break;
			}
		}
		FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));

		// finishing.
		System.out.println("\n[PEGASUS] Connected component computed.");
		System.out.println("[PEGASUS] Total Iteration = " + iter_counter);
		return 0;
    }

	// read neighborhood number after each iteration.
	public static ResultInfo readIterationOutput(String new_path) throws Exception
	{
		ResultInfo ri = new ResultInfo();
		ri.changed = ri.unchanged = 0;
		String output_path = new_path + "/part-00000";
		String file_line = "";

		try {
			BufferedReader in = new BufferedReader(	new InputStreamReader(new FileInputStream( output_path ), "UTF8"));

			// Read first line
			file_line = in.readLine();

			// Read through file one line at time. Print line # and line
			while (file_line != null){
			    final String[] line = file_line.split("\t");

				if(line[0].startsWith("i")) 
					ri.changed = Integer.parseInt( line[1] );
				else	// line[0].startsWith("u")
					ri.unchanged = Integer.parseInt( line[1] );

				file_line = in.readLine();
			}
			
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ri;//result;
	}

    // Configure stage1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.set("cur_iter", "" + cur_iter);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("ConCmpt_Stage1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, edge_path, curbm_path);  
		FileOutputFormat.setOutputPath(conf, tempbm_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

    // Configure stage2
    protected JobConf configStage2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.set("cur_iter", "" + cur_iter);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("ConCmpt_Stage2");
		
		conf.setMapperClass(MapStage2.class);        
		conf.setReducerClass(RedStage2.class);
		conf.setCombinerClass(CombinerStage2.class);

		FileInputFormat.setInputPaths(conf, tempbm_path);  
		FileOutputFormat.setOutputPath(conf, nextbm_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure stage3
    protected JobConf configStage3 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.setJobName("ConCmpt_Stage3");
		
		conf.setMapperClass(MapStage3.class);        
		conf.setReducerClass(RedStage3.class);
		conf.setCombinerClass(RedStage3.class);

		FileInputFormat.setInputPaths(conf, nextbm_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( 1 );	// This is necessary.

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}

