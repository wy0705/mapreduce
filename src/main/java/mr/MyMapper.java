package mr;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
//继承Mapper<k1,v1,k2,v2> 接口;
//k1:表示输入键类型。         v1:表示输入值类型。
//k2:表示输出键类型。         v2:表示输出值类型。
public class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    /*
    Split:程序自动拆分阶段...
        k1   v1
        0   hello
        5   world
        10  hadoop

    Map:用户可以自定义处理阶段...继承Mapper接口,实现map(..);方法
        k2      v2
        hello   1
        hadoop  1
        hello   1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //打印每个键值数据..
        System.out.println("k1:"+key+"\tv1:"+value);
        //获取一行数据 hello,world,hadoop
        String line = value.toString();
        //分析,并根据 ,逗号拆分每个单词...
        //注意拆分时候不要有空格, 可能会影响拆分,进而影响结果..
        String[] split = line.split(",");
        //循环操作生成k2  v2,并写入context中交给下面操作..
        for (String str : split) {
            //写入每个拆分字符k2    每次都是 1
            context.write(new Text(str.trim()),new LongWritable(1));
        }
    }
}
