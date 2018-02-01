package Flume;

import org.apache.flume.formatter.output.PathManager;

import java.io.File;

public class WeblukerPathManager extends PathManager{

    private File currentFile;
    private File baseDirectory;


    public File nextFile(String fileName) {
        currentFile = new File(baseDirectory, fileName);

        return currentFile;
    }

    public File getCurrentFile(String fileName) {
        if (currentFile == null) {
            return nextFile(fileName);
        }

        return currentFile;
    }
}
