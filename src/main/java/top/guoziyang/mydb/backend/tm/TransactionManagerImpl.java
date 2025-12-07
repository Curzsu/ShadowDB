package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

    /**
     * 事务管理器实现类 (TransactionManagerImpl)
     * 核心功能:
     * 1. 事务生命周期管理:
     *    - begin() - 开始一个新事务，返回唯一的事务ID（XID）
     *    - commit(long xid) - 提交指定的事务
     *    - abort(long xid) - 回滚指定的事务
     * 2. 事务状态查询:
     *    - isActive(long xid) - 检查事务是否处于活跃状态
     *    - isCommitted(long xid) - 检查事务是否已提交
     *    - isAborted(long xid) - 检查事务是否已回滚
     * 3. XID文件管理:
     *    - checkXIDCounter() - 检查XID文件完整性，验证文件头与实际长度是否匹配
     *    - getXidPosition(long xid) - 计算事务ID在文件中的存储位置
     *    - updateXID(long xid, byte status) - 更新事务状态到文件，并强制刷盘
     * 4. 事务计数器维护:
     *    - incrXIDCounter() - 递增事务计数器，并持久化到文件头
     *    - 使用 ReentrantLock 保证计数器操作的线程安全
     * 5. 资源管理:
     *    - close() - 关闭文件通道和文件句柄，释放资源
     * 6. 超级事务支持:
     *    - 维护 SUPER_XID（值为0）作为系统级超级事务，永远处于已提交状态
     * 
     * 总结(by Claude Sonnet 4.5)：
     * TM模块是 MYDB 的事务管理器，负责事务的创建、提交、回滚和状态追踪。 
     * 它通过一个 .xid 文件持久化每个事务的状态（活跃/已提交/已回滚），
     * 使用简单的文件映射机制：文件头8字节记录事务计数器，后续每个字节存储一个事务的状态标志。
     * 核心职责:维护事务生命周期，保证即使系统崩溃也能恢复事务状态，为上层的版本控制和并发管理提供基础支持。
     * 简单说：用一个文件记账本管理所有事务的生老病死。
     * 
     * @author Lukesu
     * @version 1.0
     */
public class TransactionManagerImpl implements TransactionManager {


    /*维护一个 .xid 文件。
    文件头：前 8 个字节记录“全局事务计数器”，表示一共发出了多少个事务 ID。
    文件体：从第 9 个字节开始，每一个字节（Byte）代表一个事务的状态。 */

    // XID文件头长度，用于存总事务数
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0; // 活跃状态
	private static final byte FIELD_TRAN_COMMITTED = 1; // 提交状态
	private static final byte FIELD_TRAN_ABORTED  = 2; // 事务被回滚

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0; 

    static final String XID_SUFFIX = ".xid"; // XID文件后缀
    
    private RandomAccessFile file; // XID文件
    private FileChannel fc; // XID文件通道，用于读写XID文件（可理解为对磁盘文件的直接操作句柄）
    private long xidCounter; // XID计数器，用于记录当前分配到哪个事务ID了
    private Lock counterLock; // XID计数器锁，用于保证线程安全，防止多线程并发申请ID时冲突

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock(); // 创建一个锁，用于保证线程安全，防止多线程并发申请ID时冲突
        checkXIDCounter(); // 检查XID文件是否合法
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度，如果长度不一致，则抛出异常
     */
    private void checkXIDCounter() {
        long fileLen = 0; // 文件长度
        try {
            fileLen = file.length(); // 获取文件长度
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException); // 如果文件不存在，则抛出异常
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException); // 如果文件长度小于8字节，则抛出异常
            /*
            因为事务计数器 xidCounter 的类型是 long，在 Java 中 long 占 8 个字节（64位）。
            补充：内存中的 long 是个 8 字节的数值，但文件存储的是字节流，
            因此需要一个序列化/反序列化的过程：
            1. 存文件：long → byte[] → 写入磁盘
            2. 读文件：从磁盘读取 → byte[] → long
            */
        }


        /**
         * 执行逻辑（假设文件头存的是数字 100）：
            文件前8字节: [0, 0, 0, 0, 0, 0, 0, 100]
                ↓ 第61行 fc.read(buf) 读到内存
            ByteBuffer buf: [0, 0, 0, 0, 0, 0, 0, 100]
                ↓ buf.array() 取出字节数组
            byte[]: [0, 0, 0, 0, 0, 0, 0, 100]
                ↓ Parser.parseLong(...) 转换（把 8 个字节转换成一个 long 类型的数字）
            long: 100
                ↓ 赋值
            this.xidCounter = 100
            这里的 this 指当前 TransactionManagerImpl 实例
         */
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH); // 创建一个缓冲区，用于读取文件头
        try {
            fc.position(0); // 定位光标到文件头
            fc.read(buf); // 读取文件头
        } catch (IOException e) {
            Panic.panic(e); // 如果读取文件头失败，则抛出异常
        }
        this.xidCounter = Parser.parseLong(buf.array()); // 解析文件头，获取事务计数器
        long end = getXidPosition(this.xidCounter + 1); // 计算下一个事务ID的位置
        if(end != fileLen) { // 如果计算出的文件长度不等于实际文件长度，则抛出异常
            Panic.panic(Error.BadXIDFileException); // 如果计算出的文件长度不等于实际文件长度，则抛出异常
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        // 计算偏移量：跳过头部，每个 xid 占 1 个字节
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;  //相当于 return 8 + (xid-1)*1
    }

    // 更新xid事务的状态为status（更新文件中的状态）
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset); // 定位光标
            fc.write(buf); // 写入状态
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false); // 强制刷盘（关键！确保断电不丢失）
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 开始一个事务，并返回XID
    public long begin() {
        counterLock.lock(); // 加锁，防止多线程同时申请同一个ID（只有这一步需要加锁，因为要分配唯一的 ID）
        try {
            long xid = xidCounter + 1; // 分配新 ID：下一个事务ID = 当前计数 + 1 
            updateXID(xid, FIELD_TRAN_ACTIVE); // 【关键步骤】在文件中记录：这个新事务 xid 的状态是 ACTIVE (0)
            incrXIDCounter();  // 更新XID计数器，为下一个事务做准备
            return xid; // 返回新分配的XID（返回给上层使用）
        } finally {
            counterLock.unlock(); // 解锁，释放ID
        }
    }

    // 提交XID事务
    public void commit(long xid) {
        // 更新XID事务状态为提交状态，即COMMITTED (1)
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务（将状态改为 ABORTED (2)）
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检查事务状态（比如检查是否已提交）
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED); // 直接去文件里读那个字节，看看是不是 1
    }

    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
