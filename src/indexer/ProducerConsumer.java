package indexer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import util.Timer;

/**
 * ProducerConsumer
 * <p/>
 * Producer and consumer tasks in a desktop search application
 * 
 * @author Brian Goetz
 * @author Tim Peierls
 * 
 * @author Cosmin Radoi
 * 
 *         The base for this application is an example from from Thinking in
 *         Java 4th edition. 
 *         - implemented the missing parts of the example
 *         - added a graceful termination mechanism
 */
public class ProducerConsumer {
	static class FileCrawler implements Runnable {
		private final BlockingQueue<File> fileQueue;
		private final FileFilter fileFilter;
		private final File root;

		public FileCrawler(BlockingQueue<File> fileQueue,
				final FileFilter fileFilter, File root) {
			this.fileQueue = fileQueue;
			this.root = root;
			this.fileFilter = new FileFilter() {
				public boolean accept(File f) {
					return f.isDirectory() || fileFilter.accept(f);
				}
			};
		}

		private boolean alreadyIndexed(File f) {
			return false;
		}

		public void run() {
			try {
				crawl(root);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		private void crawl(File root) throws InterruptedException {
			File[] entries = root.listFiles(fileFilter);
			if (entries != null) {
				for (File entry : entries)
					if (entry.isDirectory())
						crawl(entry);
					else if (!alreadyIndexed(entry))
						fileQueue.put(entry);
			}
		}
	}

	static class Indexer implements Runnable {
		private final BlockingQueue<File> queue;
		private final Map<String, Set<File>> index;
		private boolean producersFinished = false;

		public Indexer(BlockingQueue<File> queue, Map<String, Set<File>> index) {
			this.queue = queue;
			this.index = index;
		}

		public void run() {
			try {
				while (!Thread.interrupted()) {
					File file = queue.poll(100, TimeUnit.MILLISECONDS);
					if (file != null)
						indexFile(file);
					else if (producersFinished)
						break;
				}
			} catch (InterruptedException e) {

			}
		}

		public void producersFinished() {
			producersFinished = true;
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
									.newSetFromMap(new ConcurrentHashMap<File, Boolean>());
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
	}

	private static final int BOUND = 10;
	private static final int N_CONSUMERS = Runtime.getRuntime()
			.availableProcessors();

	public static void startIndexing(File[] roots) throws InterruptedException {
		BlockingQueue<File> queue = new LinkedBlockingQueue<File>(BOUND);
		FileFilter filter = new FileFilter() {
			public boolean accept(File file) {
				return true;
			}
		};

		Thread[] producerThreads = new Thread[roots.length];

		int i = 0;
		for (File root : roots) {
			producerThreads[i++] = new Thread(new FileCrawler(queue, filter,
					root));
			producerThreads[i - 1].start();
		}

		Map<String, Set<File>> index = new ConcurrentHashMap<String, Set<File>>();

		Thread consumerThreads[] = new Thread[N_CONSUMERS];
		Indexer[] indexers = new Indexer[N_CONSUMERS];
		for (i = 0; i < N_CONSUMERS; i++) {
			indexers[i] = new Indexer(queue, index);
			consumerThreads[i] = new Thread(indexers[i]);
			consumerThreads[i].start();
		}
		for (i = 0; i < producerThreads.length; i++)
			producerThreads[i].join();

		for (i = 0; i < N_CONSUMERS; i++)
			indexers[i].producersFinished();

		for (i = 0; i < N_CONSUMERS; i++)
			consumerThreads[i].join();

		System.out.println("The check value is: " + index.get("the").size());

	}

	public static void main(String[] args) throws FileNotFoundException,
			InterruptedException {
		File[] files = new File[1];
		files[0] = new File("data");

		Timer.start();
		startIndexing(files);
		Timer.stop();
		Timer.log("Runtime: ");
	}
}
