package Loader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

class ZipLoader {
    private ZipFile zipFile;

    ZipLoader(String fileName) {
        try {
            File file = new File(ZipLoader.class.getResource("/" + fileName).getPath());
            this.zipFile = new ZipFile(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    InputStream getInputStreamFromZip(String fileName) {
        try {
            Enumeration<? extends ZipEntry> entries = this.zipFile.entries();

            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.getName().equals(fileName)) {
                    return this.zipFile.getInputStream(entry);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    void close() {
        try {
            this.zipFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
