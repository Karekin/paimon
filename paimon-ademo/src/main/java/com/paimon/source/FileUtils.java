package com.paimon.source;


import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * @desc: FileUtils
 */
public class FileUtils {
    public static void deleteDir(String directoryPath) {
        // 创建 Path 对象
        Path path = Paths.get(directoryPath);

        try {
            // 检查目录是否存在
            if (Files.exists(path) && Files.isDirectory(path)) {
                // 删除目录及其内容
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
                System.out.println("目录已成功删除: " + directoryPath);
            } else {
                System.out.println("目录不存在或不是一个目录: " + directoryPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
