package ai.dataeng.sqml.source.util;

import ai.dataeng.sqml.source.SourceRecord;
import ai.dataeng.sqml.time.Time;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.nio.file.attribute.FileTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class SourceRecordCreator {

    public static SourceRecord from(Map<String,Object> data, FileTime sourceTime) {
        return new SourceRecord(data, Time.convert(sourceTime));
    }

    public static Map<String,Object> dataFrom(String[] header, String[] content) {
        Preconditions.checkArgument(header!=null && header.length>0);
        Preconditions.checkNotNull(content!=null && content.length== header.length);
        HashMap<String,Object> map = new HashMap<>(header.length);
        for (int i = 0; i < header.length; i++) {
            map.put(header[i],content[i]);
        }
        return map;
    }

}
