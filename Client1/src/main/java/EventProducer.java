import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class EventProducer implements Runnable{
    private static final int  TOTAL_REQUESTS = 200000;
    private static final BlockingQueue<SkierLiftEvent> skierEventQueue = new LinkedBlockingQueue<>(TOTAL_REQUESTS);

    @Override
    public void run() {
        int eventCount = 0;
        while (eventCount < TOTAL_REQUESTS) {

            SkierLiftEvent event = SkierLiftProducer.generateSkierLiftEvent();
            try {
                skierEventQueue.put(event);
                eventCount++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
       // System.out.println("Event producer thread completed generating events.");
    }

    public static SkierLiftEvent getEvent() throws InterruptedException {
        return skierEventQueue.take();
    }

}
