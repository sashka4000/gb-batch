package ru.gb.batch.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Класс запускает Spark RDD задачу, которая:
 * 1. читает каждый файл из директории в args[0]
 * 2. разбивает каждую строку на слова разделителем args[3]
 * 3. из всех слов в файле составляет пару вида "слово-1"
 * 4. суммирует все числа у одинаковых слов и записывает результат в файл args[1]
 */
public class WordCount {
 
    /**
     * Входная точка приложения. Считает количество слов во входном файле и пишет результат в выходной.
     */
    public static void main(String[] args) {
        // проверка аргументов
        if (args.length < 1) {
            throw new IllegalArgumentException("Expected arguments: input_dir output_dir [delimiter]");
        }
        final String input = args[0];
        final String output = args[1];
		final String stopwordfile = args[2];
        final String delimiter = args.length > 3 ? args[3] : " ";

        // инициализация контекста Spark
        JavaSparkContext sc = new JavaSparkContext();


        // выполняем broadcast и открываем файл на чтение
        Broadcast<String> broadcastDelimiter = sc.broadcast(delimiter);
        
		JavaRDD<String> rdd = sc.textFile(input);
		
		JavaRDD<String> rddSW = sc.textFile(stopwordfile).map(line -> line.trim());
        
		//Broadcast<String> broadcastSW  =  sc.broadcast (rddSW.toString());
		 	
        // вызываем функцию, которая преобразует данные
        JavaPairRDD<String, Integer> result = countWords(rdd, broadcastDelimiter, rddSW);

        // сохраняем на диск
        result.saveAsTextFile(output);

        // останавливаем спарк контекст
        sc.stop();
    }

    /**
     * Функция получает на вход {@code rdd} со документами, которые разбивает на термы через {@code delimiter},
     * после чего считает количество повторений каждого терма.
     */
    static JavaPairRDD<String, Integer> countWords(JavaRDD<String> rdd, Broadcast<String> delimiter, JavaRDD<String> rddSW) {
        return rdd.flatMap(line -> Arrays.asList(line.toLowerCase().replaceAll ("\\p{Punct}","").split(delimiter.getValue())).iterator())
		        .subtract (rddSW)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);
    }

}
