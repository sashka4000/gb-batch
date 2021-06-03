package ru.gb.batch.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WordCountTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void simpleTest() throws Exception {
        String input = folder.newFolder().getAbsolutePath();
        String output = new Path(folder.newFolder().getAbsolutePath(), "out").toString();

        // init file system
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // generate input data
        fs.mkdirs(new Path(input));
        try (FSDataOutputStream in = fs.create(new Path(input, "in001"), true)) {
            in.write("Some days some knowledge\nDo not forget it".getBytes(StandardCharsets.UTF_8));
        }
        try (FSDataOutputStream in = fs.create(new Path(input, "in002"), true)) {
            in.write("Forget about me".getBytes(StandardCharsets.UTF_8));
        }

        // run
        WordCount job = new WordCount();
        job.setConf(conf);
        ToolRunner.run(conf, job, new String[]{
                "-D" + WordCount.IN_PATH_PARAM + "=" + input,
                "-D" + WordCount.OUT_PATH_PARAM + "=" + output
        });

        // assert
        assertTrue(fs.exists(new Path(output)));
        List<String> part0 = Files.readAllLines(Paths.get(output, "part-r-00000"));
        List<String> part1 = Files.readAllLines(Paths.get(output, "part-r-00001"));
        assertEquals(part0, Arrays.asList(
                "do\t1",
                "it\t1",
                "me\t1"
        ));
        assertEquals(part1, Arrays.asList(
                "about\t1",
                "days\t1",
                "forget\t2",
                "knowledge\t1",
                "not\t1",
                "some\t2"
        ));
    }

    private Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        return conf;
    }
}
