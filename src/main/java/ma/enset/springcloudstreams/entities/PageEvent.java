package ma.enset.springcloudstreams.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data @NoArgsConstructor @AllArgsConstructor
public class PageEvent {
    private String name;
    private String user;
    private Date date;
    private long duration;
}
