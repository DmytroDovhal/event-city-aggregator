import entity.Event;
import entity.EventStatistic;
import options.CityAnalyzerOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import transform.TransformToEventStatisticFn;

public class CityAnalyzerPipeline {
    public static void main(String... args) {
        CityAnalyzerOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CityAnalyzerOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        //reading jsonl files
        PCollection<String> lines = pipeline.apply(TextIO.read().from(options.getInputFiles()));

        //parsing lines into Event objects
        PCollection<Event> events = lines
                .apply(ParseJsons.of(Event.class))
                .setCoder(SerializableCoder.of(Event.class));

        //creating map city -> event
        PCollection<KV<String, Event>> eventsMap = events.apply(WithKeys.of(Event::getCity))
                .setCoder(KvCoder.of(StringDelegateCoder.of(String.class), SerializableCoder.of(Event.class)));

        //grouping by city, creating map city -> List<Event>
        PCollection<KV<String, Iterable<Event>>> groupedByCityMap = eventsMap.apply(GroupByKey.create())
                .setCoder(KvCoder.of(StringDelegateCoder.of(String.class), IterableCoder.of(SerializableCoder.of(Event.class))));

        //processing city map and creating final statistic map
        PCollection<KV<String, EventStatistic>> statistic = groupedByCityMap.apply(ParDo.of(new TransformToEventStatisticFn()));

        //serializing into .avro file
        statistic.apply("Write Analyzing results", FileIO.<String, KV<String, EventStatistic>>writeDynamic()
                .withDestinationCoder(StringDelegateCoder.of(String.class))
                .by(KV::getKey)
                .via(Contextful.fn((SerializableFunction<KV<String, EventStatistic>, EventStatistic>) KV::getValue), AvroIO.sink(EventStatistic.class))
                .to(options.getOutput())
                .withNaming((SerializableFunction<String, FileIO.Write.FileNaming>) key -> FileIO.Write.defaultNaming(key, ".avro"))
        );

        pipeline.run().waitUntilFinish();

    }
}
