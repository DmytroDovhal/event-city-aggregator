package entity;

import lombok.Data;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Event implements Serializable {
    private String id;
    private String userId;
    private String city;
    private String eventType;
    private Timestamp timestamp;
    private EventSubject eventSubject;
}
