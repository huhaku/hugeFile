package info.ggdog.hugeFile;

import info.ggdog.hugeFile.util.BigFileReader;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class Main {
    private static int oldSize = 0; // 没读完文件前堵塞主线程用的
    private static LinkedList<String> lists = new LinkedList<>();
    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        String dataPath ="Y:/data/components_warehouse.txt"; // 文件路径,记得改,Y盘映射在NAS-3上面,要插入的数据表语句写在InSQLThread里面
        BigFileReader.Builder builder = new BigFileReader.Builder(dataPath,
                line -> {
            // 导出的数据表格式 int[5,20]----String[0,32]
            if (Pattern.matches("^\\d{5,20}----[A-Za-z0-9]{0,32}", line)) {
                lists.push(line);
            }
        });
        builder.withTreahdSize(80)
                .withCharset("UTF8")
                .withBufferSize(1024 * 1024);
        BigFileReader bigFileReader = builder.build();
        bigFileReader.start();

        Thread.sleep(10000L); // 文件本身不大,怕网络开销太大,等文件读完进内存再说
        while (true) {
            if (lists.size() != oldSize) {
                oldSize = lists.size();
                System.out.println("文件载入...");
                Thread.sleep(1000L);
            } else {
                break;
            }
        }

        System.out.println("开始写库");

        String type = null;
        LinkedList<String> strings1 = new LinkedList<>();
        ExecutorService exec = Executors.newFixedThreadPool(100);
        while ((type = lists.poll()) != null) {
            strings1.push(type);
            if (strings1.size() == 10000) {
                System.out.println(strings1.size());
                exec.submit(new InSQlThread(strings1));
                strings1 = new LinkedList<>();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("执行时间" + (end - start) + "ms");
    }
}
