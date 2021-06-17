package com.jsy.work;

import com.github.luben.zstd.Zstd;

import java.nio.charset.Charset;

/**
 * zstd 压缩算法demo
 *
 * 解压时间各种算法差别不大
 * 压缩时间（越小越好）：lz4, zstd < lzo < snappy << gzip-1 < lz4-9 < gzip < gzip-9 < lzo-9
 * 压缩率（越大越好）：zstd-10 > zstd >> lz4-9 > gzip-9 > gzip, lzo-9 >> lz4, gzip-1 > snappy, lzo
 *
 * 结论
 * 对大数据量的文本压缩场景，zstd是综合考虑压缩率和压缩性能最优的选择，其次是lz4。
 * 对小数据量的压缩场景，如果能使用zstd的字典方式，压缩效果更为突出。
 * 综上所述，zstd凭着优异的特性，今后应用将会越来越广，值得及早了解和尝试。
 *
 * 链接：https://www.jianshu.com/p/71eb3071d3e0
 *
 * @Author: jsy
 * @Date: 2021/6/4 21:49
 */
public class CompressedFile {
    public static void main(String[] args) {
        String str = "abcd";
        // 压缩
        byte[] compress = Zstd.compress(str.getBytes(Charset.defaultCharset()));

        // 解压缩
        byte[] compressArray = compress;
        int size = (int) Zstd.decompressedSize(compressArray);
        byte[] array = new byte[size];
        Zstd.decompress(array, compressArray);

        String s = new String(array);
        System.out.println("s = " + s);

    }
}
