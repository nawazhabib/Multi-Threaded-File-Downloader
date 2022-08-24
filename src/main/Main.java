package main;

import downloadManager.DownloadManager;

public class Main {
    public static void main(String[] args) {
        final String downloadDir = "./";
        final String url = "http://www.sample-videos.com/video/mp4/720/big_buck_bunny_720p_10mb.mp4";

        DownloadManager downloadManager = new DownloadManager();
        downloadManager.download(url, downloadDir);
    }
}
