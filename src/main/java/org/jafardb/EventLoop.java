package org.jafardb;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class EventLoop {

    private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
    private volatile boolean running = false;

    public void submit(Runnable task) {
        try {
            tasks.put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void start() {
        synchronized (this) {
            if (running) {
                throw new IllegalStateException("EventLoop is already running");
            }
            running = true;
        }

        while (running) {
            try {
                Runnable task = tasks.take();
                task.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public void stop() {
        running = false;
    }

    public static void main(String[] args) {
        EventLoop eventLoop = new EventLoop();

        new Thread(eventLoop::start).start();

        eventLoop.submit(() -> System.out.println("Task 1 executed!"));
        eventLoop.submit(() -> System.out.println("Task 2 executed!"));
    }
}
