package inputformat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SensorMapper extends Mapper<SensorKey, SensorReading, Text, Text> {
        
          protected void map(SensorKey key, SensorReading value, Context context)
              throws java.io.IOException, InterruptedException {
        	  
            String sensor = key.getSensorType().toString();
            
            if(sensor.toLowerCase().equals("a")){
            	context.write(value.getValue1(),value.getValue2());
            }
            		
          }  
}