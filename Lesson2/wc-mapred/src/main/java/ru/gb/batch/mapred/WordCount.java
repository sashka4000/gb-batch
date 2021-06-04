package ru.gb.batch.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;

/**
 * Класс запускает MapReduce задачу, которая:
 * 1. читает каждый файл из директории {@link #IN_PATH_PARAM}
 * 2. из всех слов в файле составляет пару вида "слово-1" в {@link TokenizerMapper}
 * 3.1. слова короче 3 символов отправляет на reducer-0, который пишет part-r-00000 в {@link AlphabetPartitioner}
 * 3.2. слова длиннее 2 символов отправляет на reducer-1, который пишет part-r-00001 в {@link AlphabetPartitioner}
 * 4. суммирует все числа напротив каждого слова и записывает в файл в {@link IntSumReducer}
 */
public class WordCount extends Configured implements Tool {

    public static final String IN_PATH_PARAM = "gb.wordcount.input";
    public static final String OUT_PATH_PARAM = "gb.wordcount.output"; 

    /**
     * Реализация маппера, который разбивает входной текст на отдельные слова, которые отдаёт в качестве ключа, а
     * значение всегда устанавливает в 1.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        enum CountersEnum {INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();
        private final HashMap<String, Integer> hmap = new HashMap<String, Integer>();


        public TokenizerMapper () {
          String filePath = "hdfs:/user/hduser/stopwordv1.txt";
          String line;
          try
          {
           Path pt=new Path(filePath);
           FileSystem fs = FileSystem.get(new Configuration());
           BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(pt)));
           while ((line = reader.readLine()) != null)
           {
             hmap.put(line, 0); 
           } 
           reader.close();
          }
              catch (IOException ex)  
          {
          }          
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            line = line.replaceAll ("\\p{Punct}","");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                 String testline = itr.nextToken();
                 testline = testline.trim();
                 if (!hmap.containsKey(testline)){
                  word.set(testline); 
                  context.write(word, one);
                  context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString()).increment(1);
                 }
            }
        }
    }

    /**
     * Реализация редьюсера, который на вход получает слово и итератор с числами. На выходе он суммирует все
     * числа и пишет конечный результат в файл.
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Партиционер, который разбивает все входные слова на 2 редьюсера в зависимости от длины слова.
     */
    public static class AlphabetPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (numPartitions == 1) {
                return 0; // all in one
            }

            return (key.getLength() > 2) ? 1 : 0;
        }
    }

    /**
     * Данный метод инициализирует настройки {@link Job}, в которых задаются:
     * - имя приложения для отображения в ResourceManager
     * - маппер
     * - комбинатор (локальный редьюсер)
     * - редьюсер
     * - тип данных на выходе редьюсера
     * - количество редьюсеров
     * - партиционер
     * - входные и выходные директории
     * <p>
     * После чего запускает расчет, дожидается его окончания, пишет в консоль собранные счётчики и завершает работу.
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);
        job.setPartitionerClass(AlphabetPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(getConf().get(IN_PATH_PARAM)));
        FileOutputFormat.setOutputPath(job, new Path(getConf().get(OUT_PATH_PARAM)));

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(job.getCounters().toString());
        return exitCode;
    }

    /**
     * Входная точка приложения. Если входной аргумент содержит префикс -D, то он распарсится и попадет в
     * {@link Configuration}.
     *
     * @param args ожидается два входных параметра:
     *             -D{@link #IN_PATH_PARAM}=путь_к_директории_на_чтение
     *             -D{@link #OUT_PATH_PARAM}=путь_к_директории_на_запись
     * @throws Exception при проблеме с запуском приложения на кластере
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

}

