package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.*;

/**
   This class reads the data from 'N' files concurrently  and generate the wordcount in a shared cache
 */
class MultiThreaded implements Runnable {

    private String fileName;
    private Map<String, Map<String, Integer>> map;
    private CountDownLatch latch;
    private int wordCtr = 0;

    public MultiThreaded(String fileName, Map<String, Map<String, Integer>> map, CountDownLatch latch) {
        this.fileName = fileName;
        this.map = map;
        this.latch = latch;
    }

    public String getFileName(String filePath) {
        File file = new File(fileName);
        return file.getName();
    }

    public void run() {
        try (BufferedReader crunchifyBuffer = new BufferedReader(new FileReader(fileName))) {
            String line;
            String threadName = getFileName(fileName);
            Map<String, Integer> countMap;
            // read each line one by one
            while ((line = crunchifyBuffer.readLine()) != null) {
                // ignore multiple white spaces
                String[] myWords = line.replaceAll("\\s+", " ").split(" ");
                wordCtr += myWords.length;
                if(wordCtr > 500) throw new IllegalArgumentException(fileName + " shouldn't exceed 500 words");
                setCache(threadName, myWords);
            }
            latch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setCache(String threadName, String[] myWords) {
        Map<String, Integer> countMap;
        for (String s : myWords) {
            synchronized (map) {
                countMap = map.getOrDefault(s, null);
                if (countMap != null) {
                    if (countMap.containsKey(threadName)) {//t, (k,v) -> v+1
                        countMap.put(threadName, countMap.get(threadName) + 1);
                    } else {
                        countMap.put(threadName, 1);
                    }
                } else {
                    countMap = new ConcurrentHashMap<>();
                    countMap.put(threadName, 1);
                }
                map.putIfAbsent(s, countMap);
            }
        }
    }
}


/**
 This class triggers the multithreaded code and display the content in the specified format
 */
public class MultiThreadedFilReader {

    private static final void validateCommandLineArgs(String... args) {
        if(args.length < 2) throw new IllegalArgumentException("We need minimum one input file to count the words");
    }

    public static final Integer keyOccurancesFromFile(String fileName, Map<String, Integer> map) {
        Integer value = map.get(new File(fileName).getName());
        if(value == null) return 0;
        else return value;
    }

    public static final void display(Map<String, Map<String, Integer>> map, String... filesList) {
        map.forEach((k, v) -> {
            System.out.print(k);
            int sum = map.get(k).values().stream().mapToInt(i -> i).sum();
            System.out.print(" " + sum);
            for(String fileName : filesList) {
                System.out.print(" " + keyOccurancesFromFile(fileName, v));
            }
            System.out.println();
        });
    }


    public static void main(String... args) throws ExecutionException, InterruptedException {
        validateCommandLineArgs(args);
        Map<String, Map<String, Integer>> map = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(args.length);
        ExecutorService executor = Executors.newFixedThreadPool(args.length);
        for (String fileName : args) {
            Runnable worker = new MultiThreaded(fileName, map, latch);
            executor.execute(worker);
        }
        latch.await();
        executor.shutdown();
        System.out.println("\nFinished all threads");
        display(map, args);
    }
}
