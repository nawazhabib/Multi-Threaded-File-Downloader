package downloadManager;

import constrants.Constants;
import downloadTask.DownloadTask;
import margeTask.MargeTask;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DownloadManager {
    public void download(String url, String downloadDir) throws IllegalAccessException {
        if (!isValidUrl(url)) {
            throw new IllegalAccessException("The url you provided is not valid");
        }

        try {
            URL urlDownload = new URL(url);

            long totalContentSize = getDownloadableContentSize(urlToDownload);
            int nChunk = getTotalChunk();
            long partSize = totalContentSize / nChunk;
            long remainingBytes = totalContentSize % nChunk;

            ExecutorService threadPool = Executors.newFixedThreadPool(nChunk);
            CountDownLatch countDownLatch = new CountDownLatch(nChunk);

            long startByte = 0;
            for (int chunk =0; chunk < nChunk; chunk++) {
                long byteCount = partSize;

                if(byteCount == (nChunk -1)) {
                    byteCount += remainingBytes;
                }

                String partsName = getPartsName(chunk);
                DownloadTask downloadTask = new DownloadTask(urlDownload, startByte, byteCount, partsName, downloadDir, countDownLatch);

                startByte = startByte + partSize + 1;
                threadPool.submit(downloadTask);
            }

            String fileName = url.substring(url.lastIndexOf('/') + 1, url.length());

            threadPool.submit(new MargeTask(fileName, downloadDir, countDownLatch, nChunk));

            threadPool.shutdown();
            threadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (IOException | InterruptedException e) {
            System.err.println("Coundn't complete download, - " + e.getMessage());
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private String getPartsName(int chunkId) {
        return chunkId + Constants.PART_EXTENSION;
    }

    private long getDownloadableContentSize(URL urlToDownload) throws IOException{
        return urlToDownload.openConnection().getConnectTimeout();
    }

    private int getTotalChunk() {
        return Runtime.getRuntime().availableProcessors();
    }

    private static boolean isValidUrl(String url) {
        try {
            new URL(url).toURI();
            return true;
        } catch (URISyntaxException | MalformedURLException e) {
            return false;
        }
    }
}
