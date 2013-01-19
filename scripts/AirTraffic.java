
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class AirTraffic {

 private static List<String> colList = new ArrayList<String>();

 public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private FloatWritable value1 = new FloatWritable();
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
    
        //Get configurations
        String Col1 = conf.get("Col1"); 
        String Col2 = conf.get("Col2"); 
        String Col3 = conf.get("Col3"); 
        String Col4 = conf.get("Col4"); 

        System.out.println("Col1:"+Col1+" Col2:"+Col2+" Col3:"+Col3+" Col4:"+Col4);
        String Val1="ZZZ",Val2="ZZZ",Val3="ZZZ",Val4="ZZZ";
        if(!Col1.equals("ZZZ")) Val1=conf.get("Val1");
        if(!Col2.equals("ZZZ")) Val2=conf.get("Val2");
        if(!Col3.equals("ZZZ")) Val3=conf.get("Val3");
        if(!Col4.equals("ZZZ")) Val4=conf.get("Val4");
        System.out.println("Val1:"+Val1+" Val2:"+Val2+" Val3:"+Val3+" Val4:"+Val4);

        int Pos1=-1,Pos2=-1,Pos3=-1,Pos4=-1,outputPos=-1; 
        boolean cond1=false,cond2=false,cond3=false,cond4=false;
        if(!Val1.equals("ZZZ")){ Pos1=Integer.parseInt(conf.get("Pos1")); cond1=true;}
        if(!Val2.equals("ZZZ")){ Pos2=Integer.parseInt(conf.get("Pos2")); cond2=true;}
        if(!Val3.equals("ZZZ")){ Pos3=Integer.parseInt(conf.get("Pos3")); cond3=true;}
        if(!Val4.equals("ZZZ")){ Pos4=Integer.parseInt(conf.get("Pos4")); cond4=true;}
        outputPos=Integer.parseInt(conf.get("outputColPos"));
        System.out.println("Pos1:"+Pos1+" Pos2:"+Pos2+" Pos3:"+Pos3+" Pos4:"+Pos4+" OutputColPos:"+outputPos);
        System.out.println("cond1:"+cond1+" cond2:"+cond2+" cond3:"+cond3+" cond4:"+cond4);

        String line = value.toString();
	String[] str = line.split(",");

        if(cond1) cond1 = cond1 && str[Pos1].trim().replaceAll("^\"|\"$", "").equals(Val1); else cond1=true;
        if(cond2) cond2 = cond2 && str[Pos2].trim().replaceAll("^\"|\"$", "").equals(Val2); else cond2=true;
        if(cond3) cond3 = cond3 && str[Pos3].trim().replaceAll("^\"|\"$", "").equals(Val3); else cond3=true;
        if(cond4) cond4 = cond4 && str[Pos4].trim().replaceAll("^\"|\"$", "").equals(Val4); else cond4=true;
        System.out.println("cond1:"+cond1+" cond2:"+cond2+" cond3:"+cond3+" cond4:"+cond4);

        if(Col1.equals("Carrier") && Val1.equals("All")){word.set(str[Pos1].trim());cond1=true;}
        else if(Col2.equals("Carrier") && Val2.equals("All")){word.set(str[Pos2].trim());cond2=true;}
        else if(Col3.equals("Carrier") && Val3.equals("All")){word.set(str[Pos3].trim());cond3=true;}
        else if(Col4.equals("Carrier") && Val4.equals("All")){word.set(str[Pos4].trim());cond4=true;}
        else word.set("AverageDelay");

        if(cond1 && cond2 && cond3 && cond4)
        {
            String recValue = str[outputPos];
            if((recValue != null) && (recValue.length() > 0)){
                context.write(word,new FloatWritable(Float.parseFloat(recValue)));
            }else {
                context.write(word,new FloatWritable(Float.parseFloat("0.00")));
            }
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
      throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        //System.out.println("In reducer");
        for (FloatWritable val : values) {
            System.out.println("Count:"+count+" Value:"+val.get());
            sum += val.get();
            count++;
        }
        context.write(key, new FloatWritable(sum/count));
    }
 }
        
 public static void main(String[] args) throws Exception {

    String inputword;
    String inputpath;
    String outputpath;
        
    System.out.println("Input Arguments");
    for(int i=0;i<args.length;i++){
       System.out.println("Argument "+i+":"+args[i]);
    }

    if(args.length == 3){
       inputword = args[0];
       inputpath = args[1];
       outputpath = args[2];

    } else {
       System.out.println("Number of input params not equal 3");
       return;
    }

    System.out.println("Search word:"+inputword);
    System.out.println("Input Path:"+inputpath);
    System.out.println("Output Path:"+outputpath);
  
    Configuration conf = addConfigs(inputword);
    Job job = new Job(conf, "airtrafficdelay");
    FileInputFormat.addInputPath(job, new Path(inputpath));
    FileOutputFormat.setOutputPath(job, new Path(outputpath));

    job.setJarByClass(AirTraffic.class); 

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
 
    job.waitForCompletion(true);
 }

public static Configuration addConfigs(String str){

        Configuration conf = new Configuration();

        String colNames = "Year,Quarter,Month,DayofMonth,DayOfWeek,FlightDate,UniqueCarrier,AirlineID,Carrier,TailNum,FlightNum,OriginAirportID,OriginAirportSeqID,OriginCityMarketID,Origin,OriginCityName,OriginState,OriginStateFips,OriginStateName,OriginWac,DestAirportID,DestAirportSeqID,DestCityMarketID,Dest,DestCityName,DestState,DestStateFips,DestStateName,DestWac,CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,DepartureDelayGroups,DepTimeBlk,TaxiOut,WheelsOff,WheelsOn,TaxiIn,CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,ArrivalDelayGroups,ArrTimeBlk,Cancelled,CancellationCode,Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Flights,Distance,DistanceGroup,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,FirstDepTime,TotalAddGTime,LongestAddGTime,DivAirportLandings,DivReachedDest,DivActualElapsedTime,DivArrDelay,DivDistance,Div1Airport,Div1AirportID,Div1AirportSeqID,Div1WheelsOn,Div1TotalGTime,Div1LongestGTime,Div1WheelsOff,Div1TailNum,Div2Airport,Div2AirportID,Div2AirportSeqID,Div2WheelsOn,Div2TotalGTime,Div2LongestGTime,Div2WheelsOff,Div2TailNum,Div3Airport,Div3AirportID,Div3AirportSeqID,Div3WheelsOn,Div3TotalGTime,Div3LongestGTime,Div3WheelsOff,Div3TailNum,Div4Airport,Div4AirportID,Div4AirportSeqID,Div4WheelsOn,Div4TotalGTime,Div4LongestGTime,Div4WheelsOff,Div4TailNum,Div5Airport,Div5AirportID,Div5AirportSeqID,Div5WheelsOn,Div5TotalGTime,Div5LongestGTime,Div5WheelsOff,Div5TailNum";

	String searchAttr = str;
	System.out.println("String:"+searchAttr);
	String splitStr[] = searchAttr.split(":");
	String outputCol = splitStr[0];
	String inputCols[] = splitStr[1].split(",");
	String inputCol1 = "ZZZ", inputCol2 = "ZZZ", inputCol3 = "ZZZ",inputCol4="ZZZ";
	String inputVal1="ZZZ",inputVal2="ZZZ",inputVal3 = "ZZZ",inputVal4="ZZZ";
	int inputPos1 = -1,inputPos2=-1,inputPos3=-1,outputColPos=-1,inputPos4=-1;

        if(outputCol.contains("Depart")) outputCol = "DepDelayMinutes";
        else if(outputCol.contains("Arrival")) outputCol = "ArrDelayMinutes";

	int ilen = inputCols.length;

        if(ilen > 3) {
                String input[] = inputCols[0].split("=");
                inputCol1 = input[0]; inputVal1 = input[1];

                input = inputCols[1].split("=");
                inputCol2 = input[0]; inputVal2 = input[1];

                input = inputCols[2].split("=");
                inputCol3 = input[0]; inputVal3 = input[1];

                input = inputCols[3].split("=");
                inputCol4 = input[0]; inputVal4 = input[1];
        }else if(ilen > 2) {
		String input[] = inputCols[0].split("=");
		inputCol1 = input[0]; inputVal1 = input[1];

		input = inputCols[1].split("=");
		inputCol2 = input[0]; inputVal2 = input[1];

		input = inputCols[2].split("=");
		inputCol3 = input[0]; inputVal3 = input[1];
	} else if (ilen > 1) {
		String input[] = inputCols[0].split("=");
		inputCol1 = input[0]; inputVal1 = input[1];

		input = inputCols[1].split("=");
		inputCol2 = input[0]; inputVal2 = input[1];
	} else if(ilen > 0) {
		String input[] = inputCols[0].split("=");
		inputCol1 = input[0]; inputVal1 = input[1];
	}

	StringTokenizer tokenizer = new StringTokenizer(colNames,",");
        int i = 0;
        while (tokenizer.hasMoreTokens()) {
		String token = tokenizer.nextToken();
            	if(inputCol1.equals(token))
            		inputPos1 = i;
  	        else if(inputCol2.equals(token))
            		inputPos2 = i;
            	else if(inputCol3.equals(token))
            		inputPos3 = i;
            	else if(inputCol4.equals(token))
            		inputPos4 = i;
                else if(outputCol.equals(token))
			outputColPos = i;
            	i++;
        }

	System.out.println("Output Col:"+outputCol+" outputColPos:"+outputColPos);
	System.out.println("Col1:"+inputCol1+" Val1:"+inputVal1+" Pos1:"+inputPos1);
	System.out.println("Col2:"+inputCol2+" Val2:"+inputVal2+" Pos2:"+inputPos2);
	System.out.println("Col3:"+inputCol3+" Val3:"+inputVal3+" Pos3:"+inputPos3);
	System.out.println("Col4:"+inputCol4+" Val4:"+inputVal4+" Pos4:"+inputPos4);
        conf.set("Col1",inputCol1);conf.set("Col2",inputCol2);conf.set("Col3",inputCol3);conf.set("Col4",inputCol4);
        conf.set("Val1",inputVal1);conf.set("Val2",inputVal2);conf.set("Val3",inputVal3);conf.set("Val4",inputVal4);
        conf.set("Pos1",Integer.toString(inputPos1)); conf.set("Pos2",Integer.toString(inputPos2)); 
        conf.set("Pos3",Integer.toString(inputPos3)); conf.set("Pos4",Integer.toString(inputPos4));
        conf.set("outputColPos",Integer.toString(outputColPos));

        return conf;

} //End of adConfigs method

} //End of class
