package top.guoziyang.mydb.backend.dm.dataItem;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManagerImpl;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.utils.Types;

public interface DataItem {
    SubArray data();
    
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    /**
     * 将数据项的原始数据包装成数据项格式
     * @param raw 原始数据
     * @return 包装后的数据项格式
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1]; // 默认为0（合法），表示数据项有效
        byte[] size = Parser.short2Byte((short)raw.length); // 将数据大小转换为2字节
        return Bytes.concat(valid, size, raw);  // 将有效标志、大小和原始数据拼接成数据项格式
    }

    /**
     * 从页面的offset处解析处dataitem，生成数据项
     * @param pg 页面
     * @param offset 偏移量
     * @param dm 数据管理器
     * @return 解析得到的数据项
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        // 读取数据大小
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
         // 生成唯一ID：页号 + 偏移量
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /**
     * 通过设置 ValidFlag 实现软删除
     * @param raw 数据项的原始数据
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;   // 设置有效标志为1（标记为无效）
    }
}
