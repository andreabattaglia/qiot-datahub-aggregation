package org.qiot.covid19.datahub.aggregation.domain.station;
//package org.qiot.covid19.datahub.aggregation.domain.station;
//
//import org.bson.codecs.pojo.annotations.BsonId;
//import org.bson.codecs.pojo.annotations.BsonIgnore;
//
//import com.mongodb.client.model.geojson.Point;
//
//import io.quarkus.runtime.annotations.RegisterForReflection;
//
//@RegisterForReflection
//public class MeasurementStation {
//    @BsonId
//    public int id;
//    public String serial;
//    public Point location;
//    @BsonIgnore
//    private double longitude;
//    @BsonIgnore
//    private double latitude;
//    public boolean active;
//
//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + ((serial == null) ? 0 : serial.hashCode());
//        return result;
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (this == obj)
//            return true;
//        if (obj == null)
//            return false;
//        if (getClass() != obj.getClass())
//            return false;
//        MeasurementStation other = (MeasurementStation) obj;
//        if (serial == null) {
//            if (other.serial != null)
//                return false;
//        } else if (!serial.equals(other.serial))
//            return false;
//        return true;
//    }
//
//    @Override
//    public String toString() {
//        return "MeasurementStation [id=" + id + ", serial=" + serial
//                + ", coordinates=" + location + ", active=" + active + "]";
//    }
//
//}
