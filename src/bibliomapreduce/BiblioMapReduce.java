/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bibliomapreduce;

import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author panda
 */
public class BiblioMapReduce {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // Data Seeding to our MongoDB collections
        DataSeeder ds = new DataSeeder();
        ds.SeedMyData();
        
        // Global hadoop configuration
        String out_path = "/home/panda/Desktop/NoSQL Biblio MapReduce/BiblioMapReduce/src/out";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BiblioMR");
        
        // Set Hadoop Job input configuration to the given Hadoop Collection
        MongoConfigUtil.setInputURI(job.getConfiguration(), "mongodb://localhost/biblio.livres");
        
        // More Configuration for the Hadoop Job Mapper/Reducer
        job.setJarByClass(BiblioMapReduce.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(BiblioMapper.class);
        job.setCombinerClass(BiblioReducer.class);
        job.setReducerClass(BiblioReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(com.mongodb.hadoop.MongoInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // To Log the result into the given path
        FileOutputFormat.setOutputPath(job, new Path(out_path));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
