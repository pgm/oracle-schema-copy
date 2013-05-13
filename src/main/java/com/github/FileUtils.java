package com.github;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/9/13
 * Time: 1:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUtils {

    static com.github.Target createFileTarget(String filename) {
        try {
            return new com.github.OutputStreamTarget(new GZIPOutputStream(new FileOutputStream(filename)));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
