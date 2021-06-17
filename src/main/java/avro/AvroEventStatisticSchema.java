package avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import util.FileUtil;

import java.io.IOException;

public class AvroEventStatisticSchema {
    public static void main(String[] args) throws IOException {
        Schema eventStatisticSubjectActivities = SchemaBuilder.record("EventStatisticSubjectActivities")
                .namespace("entity")
                .fields()
                .requiredString("type")
                .requiredInt("past7daysCount")
                .requiredInt("past7daysUniqueCount")
                .requiredInt("past30daysCount")
                .requiredInt("past30daysUniqueCount")
                .endRecord();

        Schema eventStatisticSubject = SchemaBuilder.record("EventStatisticSubject")
                .namespace("entity")
                .fields()
                .requiredLong("id")
                .requiredString("type")
                .name("activities")
                .type().array().items().type(eventStatisticSubjectActivities).noDefault()
                .endRecord();

        Schema eventStatistic = SchemaBuilder.record("EventStatistic")
                .namespace("entity")
                .fields()
                .name("subjects")
                .type().array().items().type(eventStatisticSubject).noDefault()
                .endRecord();

        FileUtil.saveAvScToFile(eventStatistic.toString(), "EventStatistic");
    }
}
