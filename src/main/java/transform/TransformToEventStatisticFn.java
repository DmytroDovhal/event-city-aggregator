package transform;

import entity.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TransformToEventStatisticFn extends DoFn<KV<String, Iterable<Event>>, KV<String, EventStatistic>> {

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Event>> event, OutputReceiver<KV<String, EventStatistic>> receiver) {
        //grouping events by event subject
        Map<EventSubject, List<Event>> groupedEventsBySubject = StreamSupport
                .stream(Objects.requireNonNull(event.getValue()).spliterator(), false)
                .collect(Collectors.groupingBy(Event::getEventSubject));

        //creating statistic by subjects
        List<EventStatisticSubject> subjects = groupedEventsBySubject.keySet().stream()
                .map(subject -> {
                    //getting events related to current subject
                    List<Event> eventWithCurrentSubjectList = groupedEventsBySubject.get(subject);

                    //grouping events by event type
                    Map<String, List<Event>> eventActivityByEventType = eventWithCurrentSubjectList.stream()
                            .collect(Collectors.groupingBy(Event::getEventType));

                    //crating statistic by event type
                    List<EventStatisticSubjectActivities> activities = eventActivityByEventType.entrySet().stream()
                            .map(events -> {
                                String eventType = events.getKey();

                                //creating map with user activity statistic to identify unique events
                                Map<String, Long> userActivityStatistic = getUserActivityStatistic(eventActivityByEventType.get(eventType));

                                int past7daysCount = (int) events.getValue().stream()
                                        .filter(evt -> evt.getTimestamp().toInstant().isBefore(Instant.now().plus(7, ChronoUnit.DAYS)))
                                        .count();
                                int past7daysUniqueCount = (int) events.getValue().stream()
                                        .filter(evt -> evt.getTimestamp().toInstant().isBefore(Instant.now().plus(7, ChronoUnit.DAYS)))
                                        .filter(evt -> userActivityStatistic.get(evt.getUserId()) == 1)
                                        .count();
                                int past30daysCount = (int) events.getValue().stream()
                                        .filter(evt -> evt.getTimestamp().toInstant().isBefore(Instant.now().plus(30, ChronoUnit.DAYS)))
                                        .count();
                                int past30daysUniqueCount = (int) events.getValue().stream()
                                        .filter(evt -> evt.getTimestamp().toInstant().isBefore(Instant.now().plus(30, ChronoUnit.DAYS)))
                                        .filter(evt -> userActivityStatistic.get(evt.getUserId()) == 1)
                                        .count();

                                return new EventStatisticSubjectActivities(eventType, past7daysCount,
                                        past7daysUniqueCount, past30daysCount, past30daysUniqueCount);
                            }).collect(Collectors.toList());

                    return new EventStatisticSubject(subject.getId(), subject.getType(), activities);
                })
                .collect(Collectors.toList());

        String cityName = event.getKey();
        EventStatistic eventStatistic = new EventStatistic(subjects);
        receiver.output(KV.of(cityName, eventStatistic));
    }

    private Map<String, Long> getUserActivityStatistic(List<Event> events) {
        Map<String, Long> map = new HashMap<>();
        Long initialValue = 1L;

        for (Event event : events) {
            if (!map.containsKey(event.getUserId())) {
                map.put(event.getUserId(), initialValue);
            } else {
                map.put(event.getUserId(), map.get(event.getUserId()) + 1L);
            }
        }
        return map;
    }
}