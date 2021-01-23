package mr;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
//继承Reducer<k2,v2,k3,v3> 接口;
//把新的k2   v2  ----转换----  k3   v3
public class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /*
    k2   v2经过 suffer 分区 排序 规约 分组产生新的k2  v2(迭代器~)
        hello     <1,1,..>
        word      <1,1,..>
    reduce对多个新的k2 v2进行汇合..成为k3 v3
    因为一个大文件可能差分出很多个小文件由多个不同Map执行 (Shuffle可以理解Map过程中的一个小汇总),Reduce是对其最后的汇总操作...
    k3      v3
        hello   2
        word    2
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("k2:" + key + "v2s:" + values);
        //计数器
        long count = 0;
        //循环遍历迭代器,并计数！
        for (LongWritable value : values) {
            count += value.get();
        }
        //生成k3  v3
        context.write(key, new LongWritable(count));
    }
}
