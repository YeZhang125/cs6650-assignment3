import java.util.Random;

public class SkierLiftProducer {

    private static final Random random = new Random();
    public static SkierLiftEvent generateSkierLiftEvent() {

        SkierLiftEvent event = new SkierLiftEvent();
        int skierID = random.nextInt(100000) + 1;
        int resortID = random.nextInt(10) + 1;
        int liftID = random.nextInt(40) + 1;
        int time = random.nextInt(360) + 1;
        int seasonID = 2025;
        int dayID = 1;

        event.setSkierID(skierID);
        event.setResortID(resortID);
        event.setLiftID(liftID);
        event.setTime(time);
        event.setSeasonID(seasonID);
        event.setDayID(dayID);
        return event;
    }


}