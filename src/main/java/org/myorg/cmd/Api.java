package org.myorg.cmd;

import com.sun.istack.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class Api {
    private static final String HOST = "hdfs://localhost:9000";
    private static Configuration configuration;
    private static FileSystem fs;

    public Api() {

    }

    static {
        configuration = new Configuration();
        try {
            fs = FileSystem.get(new URI(Api.HOST), configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * hdfs创建目录
     * @param name
     * @return
     * @throws IOException
     */
    public static boolean mkdir(@NotNull String name)  {
        boolean flag = false;
        try {
            if (!fs.isDirectory(new Path(name))) {
                flag = fs.mkdirs(new Path(name));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return flag;
    }

    public static boolean mkFile(@NotNull String name, @NotNull String content) {
        boolean flag = false;
        try {
            FSDataOutputStream fsDataOutputStream =  null;
            byte[] arg = content.getBytes();
            if (!fs.exists(new Path(name))) {
                fsDataOutputStream = fs.create(new Path(name));
                fsDataOutputStream.write(arg, 0, arg.length);
                fsDataOutputStream.close();
                System.out.println("create file success");

                flag = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return flag;
    }

    public static boolean rm(String name) {
        boolean flag = false;
        try {
            flag =  fs.deleteOnExit(new Path(name));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return flag;
    }

    public static void main(String[] args) {
        Api.rm("a");
    }
}
