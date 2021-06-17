package util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtil {

    public static void saveAvScToFile(String data, String filename) throws IOException {
        if (Files.notExists(Constrains.avScDirPath)) Files.createDirectory(Constrains.avScDirPath);

        Path filePath = Paths.get(Constrains.avScDirPath.toString(), filename + ".avsc");
        if (Files.notExists(filePath)) Files.createFile(filePath);

        Files.writeString(filePath, data);
    }
}
