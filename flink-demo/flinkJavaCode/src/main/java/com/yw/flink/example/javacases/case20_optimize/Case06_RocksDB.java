package com.yw.flink.example.javacases.case20_optimize;

import com.yw.flink.example.StationLog;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;

import java.util.Collection;
import java.util.Random;

/**
 * RocksDB高级参数设置
 */
public class Case06_RocksDB {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置状态后端为RocksDBStateBackend，并指定增量checkpoint
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        //设置高级参数，Flink框架提供
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.DEFAULT);
        //自己实现高级参数
//        rocksDBStateBackend.setRocksDBOptions(new MyOptionsFactory());
        env.setStateBackend(rocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster/rockDBState-dir");

        DataStreamSource<StationLog> source = env.addSource(new RichParallelSourceFunction<StationLog>() {
            Boolean flag = true;

            @Override
            public void run(SourceContext<StationLog> ctx) throws Exception {
                Random random = new Random();
                String[] callTypes = {"fail", "success", "busy", "barring"};
                while (flag) {
                    String sid = "sid_" + random.nextInt(10);
                    String callOut = "1811234" + (random.nextInt(9000) + 1000);
                    String callIn = "1915678" + (random.nextInt(9000) + 1000);
                    String callType = callTypes[random.nextInt(4)];
                    Long callTime = System.currentTimeMillis();
                    Long durations = Long.valueOf(random.nextInt(50) + "");
                    ctx.collect(new StationLog(sid, callOut, callIn, callType, callTime, durations));
                    Thread.sleep(1000);//1s 产生一个事件
                }

            }

            //当取消对应的Flink任务时被调用
            @Override
            public void cancel() {
                flag = false;
            }
        });

        //处理数据
        SingleOutputStreamOperator<String> result =
                source.keyBy(stationLog -> stationLog.sid)
                        .map(new RichMapFunction<StationLog, String>() {

                            private ListState<String> listState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("liststate", String.class);
                                listState = getRuntimeContext().getListState(stateDescriptor);
                            }

                            @Override
                            public String map(StationLog value) throws Exception {
                                //100倍状态存储
                                for (int i = 0; i < 100; i++) {
                                    listState.add(value.toString());
                                }
                                return value.toString();
                            }

                        });

        result.print();

        env.execute();
    }

    //MyOptionsFactory类实现
    private static class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {

        @Override
        public DBOptions createDBOptions(DBOptions currentOptions,
                                         Collection<AutoCloseable> handlesToClose) {
            return currentOptions
                    //数据自动刷盘
                    .setAtomicFlush(true);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions,
                                                       Collection<AutoCloseable> handlesToClose) {
            return currentOptions
                    //设置在内存中允许保留的 memtable 最大个数，默认2
                    .setMaxWriteBufferNumber(2)
                    //设置数据写入RocksDB时在内存中空间占用大小，默认64M
                    .setWriteBufferSize(64 * 1024 * 1024L)
                    //设置内存中writebuffer 进行合并的最小阈值，默认1
                    .setMinWriteBufferNumberToMerge(1)
                    .setTableFormatConfig(
                            new BlockBasedTableConfig()
                                    //设置RocksDB中BlockCache大小的参数，默认8M
                                    .setBlockCache(new LRUCache(8 * 1024 * 1024L))
                                    //设置SST文件底层block大小，默认4kb
                                    .setBlockSize(4 * 1024L)
                    );
        }

        @Override
        public RocksDBOptionsFactory configure(ReadableConfig configuration) {
            //返回配置项
            return this;
        }
    }
}
