package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 
 * XChecksum：所有日志条目的累积校验和，用于快速验证日志完整性
 * Log1...LogN：有效的日志条目序列
 * BadTail：崩溃时可能产生的不完整日志（恢复时需要截断）
 * 
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;  // 校验和计算的种子

    private static final int OF_SIZE = 0;   // Size字段的偏移量
    private static final int OF_CHECKSUM = OF_SIZE + 4;   // Checksum字段的偏移量
    private static final int OF_DATA = OF_CHECKSUM + 4;   // Data字段的偏移量
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;  // 日志文件句柄
    private FileChannel fc;  // 文件通道（NIO）
    private Lock lock;  // 并发控制锁，保护对日志文件的访问

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;  // 校验和，用于快速验证日志完整性

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    // 检查并移除损坏的日志尾部（bad tail）
    private void checkAndRemoveTail() {
        rewind();   // 重置位置指针到文件开头（跳过 XChecksum）

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException); // 校验失败，日志损坏
        }

        // 截断到最后一条有效日志
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();  // 重新定位到开头
    }

    /**
     * 计算日志条目的校验和
     * @param xCheck 累积校验和
     * @param log 日志条目
     * @return 更新后的累积校验和
     */
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b; // SEED = 13331
        }
        return xCheck;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data); // 包装成 [Size][Checksum][Data] 格式
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size()); // 追加到文件末尾
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);    // 更新累积校验和
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 包装日志数据为 [Size][Checksum][Data] 格式
     * @param data 待包装的日志数据
     * @return 包装后的日志数据
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x); // 截断到指定位置 x
        } finally {
            lock.unlock();
        }
    }

    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {
            return null; // 到达文件末尾
        }

        // 1. 读取 Size 和 Checksum
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        // 2. 读取完整日志
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array(); 

        // 3. 校验
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null; // 校验失败
        }
        position += log.length; // 更新位置指针
        return log; // 返回日志数据
    }

    /**
     * 读取下一条日志,用于恢复时读取日志
     * @return 下一条日志的纯数据部分，如果没有更多日志则返回null
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext(); // 读取下一条日志
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length); // 返回纯数据部分
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4; // 跳过开头的 XChecksum，指向第一条日志的位置
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
