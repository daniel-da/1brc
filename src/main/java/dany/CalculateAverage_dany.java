/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dany;

import static java.util.stream.Collectors.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class CalculateAverage_dany {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void mainOriginal(String[] args) throws IOException {
        long t0 = System.nanoTime();
        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
                });

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                .limit(10)
                .map(l -> new Measurement(l.split(";")))
                .collect(groupingBy(m -> m.station(), collector)));

        long t1 = System.nanoTime();

        System.out.println(measurements);
        System.out.println(((t1 - t0) / 1000000) + "ms");
    }

    private static class DataValue {
    
        int centaines;
        int dizaines;
        int dixiemes;

    }

    private static class DataMerge {
        
        int count;
        int countPositive;
        DataValue sum;
        Index min;
        Index max;

        public DataMerge(Index index) {
            min = max = index;
            count = 1;
            sum = toDataValue(index);
        }

        public DataValue toDataValue(Index index) {
            var dv = new DataValue();
            if (index.negative) {
                countPositive--;
                dv.centaines = -index.db.data[index.startNumber];
                dv.dizaines = -index.db.data[index.startNumber+1];
                dv.dixiemes = -index.db.data[index.startNumber+3];
            } else {
                countPositive++;
                dv.centaines = index.db.data[index.startNumber];
                dv.dizaines = index.db.data[index.startNumber+1];
                dv.dixiemes = index.db.data[index.startNumber+3];
            }
            return dv;
        }

        public void merge(Index index) {
            count++;
            if (index.negative) {
                countPositive--;
                sum.centaines -= index.db.data[index.startNumber];
                sum.dizaines -= index.db.data[index.startNumber+1];
                sum.dixiemes -= index.db.data[index.startNumber+3];
            } else {
                countPositive++;
                sum.centaines += index.db.data[index.startNumber];
                sum.dizaines += index.db.data[index.startNumber+1];
                sum.dixiemes += index.db.data[index.startNumber+3];
            }
            if (compare(min, index) > 0 ) {
                min = index;
            } else if (compare(max, index) < 0 ) {
                max = index;
            }
        }

        public int compare(Index a, Index b) {
            int diff = a.db.data[a.startNumber] - b.db.data[b.startNumber];
            if (diff!=0) {
                return diff;
            }
            diff = a.db.data[a.startNumber+1] - b.db.data[b.startNumber+1];
            if (diff!=0) {
                return diff;
            }
            return a.db.data[a.startNumber+3] - b.db.data[b.startNumber+3];
        }


        public int convertX10(DataValue dv, int count) {
            int delta = '0'*countPositive;
            return (dv.centaines-delta)*100 + (dv.dizaines-delta)*10 + dv.dixiemes-delta;
        }

        @Override
        public String toString() {
            int sumValue = convertX10(sum, countPositive);
            return "DM:"+count+" "+sumValue/(count*10.0) +" min " + min.getNumberStr() + " max " + max.getNumberStr();
        }

    }

    private static class Index {

        public int start;
        public int middle;
        public boolean negative;
        public int startNumber;
        public int end;
        public DataBatch db;
        public DataMerge dm;


        public boolean equals(Index index, int fromIndex) {
            // if (index==null) {
            //     return false;
            // }
            int len = middle-start;
            if (len != index.middle-index.start) {
                return false;
            }
            for (int i = fromIndex; i < len; i++) {
                if (db.data[start+i] != index.db.data[index.start+i]) {
                    return false;
                }
            }
            return true;
        }

        public String getNumberStr() {
            if (negative) {
                return '-'+new String(db.data, startNumber, end-startNumber); // FIXME
            } 
            return new String(db.data, startNumber, end-startNumber);
        }

        public String getNameString() {
            return new String(db.data, start, end-start);
        }

        public void merge(Index index) {
            if (dm==null) {
                dm = new DataMerge(this);
            }
            dm.merge(index);
        }

        @Override
        public String toString() {
            if (dm==null) {
                dm = new DataMerge(this);
            }
            return //new String(db.data, start, end-start) + " " +
                new String(db.data, start, middle-start) + " " +
                // " negative?"+negative+" "+
                // getNumberStr() + " "+
                dm 
                +"\n"
                ;
        }

    }

    private static class Factory {

        public static Index newIndex(DataBatch db) {
            Index index = new Index();
            index.db = db; // TODO count dependencies
            return index;
        }

    }

    private static class DataBatch {
        char[] data;
        int len;

        public DataBatch(int len) {
            this.len = len;
            this.data = new char[len];
        }

        public DataBatch(BufferedReader br, int batchSize) throws IOException {
            data = new char[batchSize];
            len = br.read(data, 0, batchSize);
        }

        public DataBatch(BufferedReader br, int offset, int batchSize) throws IOException {
            data = new char[batchSize];
            len = br.read(data, offset, batchSize-offset) + offset;
        }

        public DataBatch(FileChannel channel, CharBuffer buff) throws IOException {
            channel.read(buff);
            buff.flip();
            data = buff.array();
        }
    }

    private static class MyMap {
        MyMapArray elements = new MyMapArray();

        public MyMap() {
            MyMapElement[] c = elements.content;
            for (int i = 0; i < c.length; i++) {
                c[i] = new MyMapArray();
            }
        }

        public Index put(Index index) {
            int i = index.start - 1;
            MyMapArray mapArray = elements;

            char key = (char) (index.middle-index.start);
            do {
                MyMapElement element = mapArray.get(key);
                if (element == null) {
                    MyMapEntry mae = new MyMapEntry(index); 
                    mapArray.put(key, mae);
                    return mae.entry;
                }
                if (element instanceof MyMapEntry) {
                    if (index.equals(((MyMapEntry)element).entry, Math.max(0,i-index.start))) {
                       return ((MyMapEntry)element).entry;
                    } else {
                        MyMapArray mma = new MyMapArray();
                        Index oldElement = ((MyMapEntry)element).entry;
                        mma.put(oldElement.db.data[oldElement.start+i-index.start+1], element);
                        element = mma;
                        mapArray.put(key, mma);
                    }
                }
                mapArray = (MyMapArray) element;
                i++;
                key = index.db.data[i];
            } while (i<=index.middle);
            // should never happens
            System.out.println("WTF "+ i + " "+index.start +" "+index.middle + " "+new String(index.db.data, index.start, index.middle-index.start));
            return null;
        }

        // public Index get(Index index) {
        //     int i = index.start;
        //     MyMapArray mapArray = elements;
        //     do {
        //         MyMapElement element = mapArray.get(index.db.data[i]);
        //         if (element == null) {
        //             return null;
        //         }
        //         if (element instanceof MyMapEntry ) {
        //             if (index.equals(((MyMapEntry)element).entry)) {
        //                 return ((MyMapEntry)element).entry;
        //             }
        //             return null;
        //         } 
        //         mapArray = (MyMapArray) element;
        //         i++;
        //     } while (i<index.middle);
        //     return null;
        // }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toStringBuilder(sb, elements); 
            return sb.toString();
        }

        public void toStringBuilder(StringBuilder sb, MyMapArray mma) {
            MyMapElement[] content = mma.content;
            for (MyMapElement myMapElement : content) {
                if (myMapElement == null) {
                    // on fait rien
                } else if (myMapElement instanceof MyMapArray) {
                    toStringBuilder(sb, (MyMapArray) myMapElement);
                } else if (myMapElement instanceof MyMapEntry) {
                    sb.append(((MyMapEntry)myMapElement).entry);
                }
            }
        }


    }

    private static interface MyMapElement {
    }

    private static class MyMapArray implements MyMapElement {
        MyMapElement[] content = new MyMapElement[1024];

        public MyMapElement get(char c) {
            return content[c];
        }

        public void put(char c, MyMapElement element) { 
            content[c] = element;
        }
        
    }
    
    private static class MyMapEntry implements MyMapElement {
        Index entry;

        public MyMapEntry(Index index) {
            entry = new Index();
            int len = index.end - index.start; 
            entry.db = new DataBatch(len);
            System.arraycopy(index.db.data, index.start, entry.db.data, 0, len);
            entry.start = 0;
            entry.middle = index.middle - index.start;
            entry.startNumber = index.startNumber - index.start;
            entry.end = index.end - index.start;
            entry.negative = index.negative;
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        long t0 = System.nanoTime();
        
        try (RandomAccessFile reader = new RandomAccessFile(FILE, "r");
             FileChannel channel = reader.getChannel()) {
                 
            int bufferSize = (int) channel.size();
            ByteBuffer buff = ByteBuffer.allocate(bufferSize);
            
       
        // try (BufferedReader br = Files.newBufferedReader(Paths.get(FILE), Charset.forName("UTF-8"))) {
            
            BlockingQueue<DataBatch> batchQueue = new ArrayBlockingQueue<>(1 );
            BlockingQueue<Index> idQueue = new LinkedTransferQueue<>();
            BlockingQueue<MergeData> mergeQueue = new LinkedTransferQueue<>( );

            MyMap mm = new MyMap();

            
            int batchSize = 10*1024*1024; // 1M
            int bi =0;

            // processBatchQueue(batchQueue,idQueue, mm);
            // processIdQueue(idQueue, mergeQueue, mm);
            // processMergeQueue(mergeQueue, mm);

            

            //DataBatch db = new DataBatch(br, batchSize);
            DataBatch db = new DataBatch(channel, buff);
            while (true) {
                int len = db.len;
                if (len<=0) {
                    break;
                }
                //batchQueue.put(db);
               
                long t00 = System.nanoTime();
                processDataBatch(db, idQueue, mm);
                long t01 = System.nanoTime();
                System.out.println("Process batch in "+ ((t01-t00)/1000000));
               
               
                if (db.data[len-1] == '\n') {
                    // rien à ajuster
                    db = new DataBatch(br, batchSize);
                } else {
                    int lastStart = len - 1;
                    while (db.data[lastStart] != '\n') {
                        lastStart--;
                    }
                    lastStart++;
                    bi++;
                    System.out.println("next batch "+bi + " " +len +" "+lastStart +" " +batchQueue.size()+" " +idQueue.size()+" " +mergeQueue.size());
                    DataBatch db2 = new DataBatch(br, len-lastStart, batchSize);
                    System.arraycopy(db.data, lastStart, db2.data, 0, len-lastStart); 
                    db = db2;
                }
            }

            /*
            DataBatch db = new DataBatch(br, batchSize);
            while (true) {
                int len = db.len;
                if (len<=0) {
                    break;
                }
                int lastStart = processDataBatch(db, idQueue);
                //if (true) break;
                if (db.data[len-1] == '\n') {
                    // rien à ajuster
                    db = new DataBatch(br, batchSize);
                } else {
                    bi++;
                    System.out.println("next batch "+bi + " " +len +" "+lastStart +" " +idQueue.size()+" " +mergeQueue.size());
                    DataBatch db2 = new DataBatch(br, len-lastStart, batchSize);
                    System.arraycopy(db.data, lastStart, db2.data, 0, len-lastStart); 
                    db = db2;
                }
            }
            */
            
            while (!batchQueue.isEmpty() || !idQueue.isEmpty() || !mergeQueue.isEmpty()) {
                Thread.sleep(10);
            }
            
            System.out.println(mm.toString());
        }
        
        
        long t1 = System.nanoTime();
        System.out.println(((t1 - t0) / 1000000) + "ms");

    }

    private static void processBatchQueue(BlockingQueue<DataBatch> batchQueue, BlockingQueue<Index> idQueue, MyMap myMap) {
        Thread t = new Thread() {
            public void run() {
                try {
                    while (true) {
                        DataBatch db = batchQueue.take();
                        long t0 = System.nanoTime();
                        processDataBatch(db, idQueue, myMap);
                        long t1 = System.nanoTime();
                        System.out.println("Process batch in "+ ((t1-t0)/1000000));
                    }
                } catch (InterruptedException|RuntimeException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            };
        };
        t.setDaemon(true);
        t.start();
    }

    private static void processIdQueue(BlockingQueue<Index> idQueue, BlockingQueue<MergeData> mergeQueue, MyMap myMap) {
        Thread t = new Thread() {
            public void run() {
                try {
                    List<Index> toProcess = new ArrayList<>();
                    while (true) {
                        //idQueue.drainTo(toProcess);
                        Index index = idQueue.take();
                        //if (true) {
                        //    continue;
                        //}
                        Index old = myMap.put(index);
                        // System.out.println(">"+ index);
                        if (old.start != index.start) {
                            // On aggrège
                            // System.out.println("+"+old);
                             old.merge(index);
                            //mergeQueue.put(MergeData.of(old,index));
                            // System.out.println("="+old);
                        } else {
                            System.out.println("new");
                        }
                    }
                } catch (InterruptedException|RuntimeException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            };
        };
        //t.setPriority(0);
        t.setDaemon(true);
        t.start();
    }

    private static class MergeData {
        Index old;
        Index index;
        
        public static MergeData of(Index old2, Index index2) {
            MergeData md = new MergeData();
            md.old = old2;
            md.index = index2;
            return md;
        }
        
        public void merge() {
            old.merge(index);
        }
    }

    private static void processMergeQueue(BlockingQueue<MergeData> mergeQueue, MyMap myMap) {
        Thread t = new Thread() {
            public void run() {
                while (true) {
                    try {
                        MergeData md = mergeQueue.take();
                        md.merge();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
        };
        t.setDaemon(true);
        t.start();
    }

    private static int processDataBatch(DataBatch db, BlockingQueue<Index> idQueue, MyMap myMap) throws InterruptedException {
        int count =0;
        Index currentIndex = Factory.newIndex(db);
        currentIndex.start = 0;
        for (int i = 1; i < db.data.length; i++) {
            char c = db.data[i];
            switch (c) {
                case ';':
                    currentIndex.middle = i;
                    currentIndex.startNumber = i+1;
                    break;
                case '-':
                    currentIndex.negative = true;
                    currentIndex.startNumber = i+1;
                    break;
                case '\n':
                    currentIndex.end = i;
                    if (currentIndex.end - currentIndex.startNumber < 4) {
                        currentIndex.startNumber--;
                        db.data[currentIndex.startNumber] = '0';
                    }
                    count++;
                    //idQueue.put(currentIndex);

                    Index old = myMap.put(currentIndex);
                    if (old.start != currentIndex.start) {
                         old.merge(currentIndex);
                    }

                    currentIndex = Factory.newIndex(db);
                    currentIndex.start = i+1;
                    break;
                default:
                    break;
            } 
        }
        System.out.println("count index by batch "+count);
        return currentIndex.start;
    }
}
