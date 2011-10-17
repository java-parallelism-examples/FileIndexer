package indexer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import util.Timer;

public class Sequential {

	private static void startIndexing(File[] files) {
		FileFilter filter = new FileFilter() {
			public boolean accept(File file) {
				return true;
			}
		};

		Indexer indexer = new Indexer(files, filter);
		try {
			indexer.compute();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("The index contains: "
				+ indexer.getIndex().get("the").size());
	}

	static class Indexer {
		private final File[] files;
		private Map<String, Set<File>> index;
		private final FileFilter fileFilter;

		public Indexer(File[] files, FileFilter filter) {
			this.files = files;
			this.fileFilter = filter;
		}

		public Map<String, Set<File>> getIndex() {
			return index;
		}

		public void compute() throws InterruptedException {
			index = new HashMap<String, Set<File>>();
			for (File entry : files)
				if (entry.isDirectory())
					crawl(entry);
				else if (!alreadyIndexed(entry))
					indexFile(entry);
		}

		private void crawl(File root) throws InterruptedException {
			File[] entries = root.listFiles(fileFilter);
			if (entries != null) {
				for (File entry : entries)
					if (entry.isDirectory())
						crawl(entry);
					else if (!alreadyIndexed(entry))
						indexFile(entry);
			}
		}

		public void indexFile(File file) {
			System.out.println("Indexing... " + file);
			try {
				Scanner s = new Scanner(file);
				while (s.hasNextLine()) {
					String line = s.nextLine();
					String[] split = line.split(" ");
					for (String token : split) {
						if (!index.containsKey(token)) {
							Set<File> set = Collections
									.newSetFromMap(new HashMap<File, Boolean>());
							index.put(token, set);
						}
						Set<File> set = index.get(token);
						set.add(file);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		private boolean alreadyIndexed(File f) {
			return false;
		}
	}

	public static void main(String[] args) {
		File[] files = new File[1];
		files[0] = new File("data");
		Timer.start();
		startIndexing(files);
		Timer.stop();
		Timer.log("Runtime: ");
	}
}
