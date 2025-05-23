import java.util.Random;

public class SkierLiftProducer {

    private static final Random random = new Random();
    private static int currentDayID = 1; // Default day ID

    public static void setCurrentDayID(int dayID) {
        currentDayID = dayID;
    }

    public static SkierLiftEvent generateSkierLiftEvent() {
        SkierLiftEvent event = new SkierLiftEvent();
        int skierID = random.nextInt(100000) + 1;
        int resortID = 3;
        int liftID = random.nextInt(40) + 1;
        int time = random.nextInt(360) + 1;
        int seasonID = 2025;

        event.setSkierID(skierID);
        event.setResortID(resortID);
        event.setLiftID(liftID);
        event.setTime(time);
        event.setSeasonID(seasonID);
        event.setDayID(currentDayID);
        return event;
    }
}