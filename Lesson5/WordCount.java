package ru.gb.batch.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

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
        final String delimiter = args.length > 2 ? args[3] : " ";

        // инициализация Spark
        SparkSession sqlc = SparkSession.builder().getOrCreate();

        // выполняем broadcast и открываем файл на чтение
        Dataset<Row> df = sqlc.read().option("header", "true").csv(input);

        // вызываем функцию, которая преобразует данные
        Dataset<Row> wc = countWords(df, delimiter);

        // сохраняем на диск
        wc.write().mode(SaveMode.Overwrite).csv(output);

        // завершаем работу
        sqlc.stop();
    }

    /**
     * Функция получает на вход {@code rdd} со документами, которые разбивает на термы через {@code delimiter},
     * после чего считает количество повторений каждого терма.
     */
    static Dataset<Row> countWords(Dataset<Row> df, String delimiter) {
        return df.select(concat_ws(" ", col("class"), col("comment")).as("docs"))
                .select(split(col("docs"), delimiter).as("words"))
                .select(explode(col("words")).as("word"))
                .groupBy(col("word")).count();
    }

}
