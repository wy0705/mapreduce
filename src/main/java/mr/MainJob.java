package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //创建一个作业: 配置 还有作业名可随意更改~
        Job job = Job.getInstance(super.getConf(), "my_mr_job");
        job.setJarByClass(MainJob.class);
        //设置输入(读取的操作)的类型 TextInputFormat
        job.setInputFormatClass(TextInputFormat.class);
        //设置读取的文件,读取输入文件解析成key，value对
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.1.110:9000/input/wordcount.txt"));

        //设置指定我们的 map阶段
        job.setMapperClass(MyMapper.class);
        //设置map的输出 k2 和v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reducer阶段
        job.setReducerClass(MyReducer.class);
        //设置reducer的输出 k3 和v3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输出文件: 可以是本地/文件系统
        //TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.110:9000/wordcount_out"));
        TextOutputFormat.setOutputPath(job, new Path("D:\\mydata\\out"));

        //执行,返回boolean 结果: true执行成功！
        boolean isok = job.waitForCompletion(true);
        System.out.println(isok);
        return  isok?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        ToolRunner.run(config, new MainJob(), args);
    }
}

