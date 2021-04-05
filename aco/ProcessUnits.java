package hadoop; 

import java.util.*; 
import java.io.*;

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import Ant2;




public class ProcessUnits {
   //Mapper class 


   public static class E_EMapper extends  
   Mapper<LongWritable ,/*Input key Type */ 
   Text,                /*Input value Type*/ 
   Text,                /*Output key Type*/ 
   Text>        /*Output value Type*/ 
   {
      ArrayList<ArrayList<ArrayList<Double>>> cost;
      ArrayList<ArrayList<ArrayList<Double>>> pher;
      ArrayList<ArrayList<Double>> datasetArray;
      ArrayList<Double> wtList;
      ArrayList<String> aggList;
      
      int taskNum;
      int rowsPerTask;
      int itemsPerRow = 5;

      protected void setup(Mapper.Context context)
        throws IOException, InterruptedException {
         cost = new ArrayList<ArrayList<ArrayList<Double>>>(); 
         pher = new ArrayList<ArrayList<ArrayList<Double>>>(); 
         datasetArray = new ArrayList<ArrayList<Double>>();
         wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
         aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
         StringTokenizer s;
         taskNum = Integer.parseInt(context.getConfiguration().get("taskNum"));
         int w = 2500/taskNum;
         rowsPerTask = w;

         String inputCost = context.getConfiguration().get("cost");
         String inputPher = context.getConfiguration().get("pher");
         String inputDataset = context.getConfiguration().get("cost");
         
         s = new StringTokenizer(inputCost," ");
         
         ArrayList<ArrayList<Double>> tempMatrix = new ArrayList<ArrayList<Double>>();
         ArrayList<Double> tempRow = new ArrayList<Double>();
         for(int i = 0; i < w; i++)
            tempRow.add(Double.parseDouble(s.nextToken()));
         tempMatrix.add(tempRow);
         cost.add(tempMatrix);
         for(int i = 0;i < taskNum-1;i++){
            tempMatrix = new ArrayList<ArrayList<Double>>();
            for(int j = 0; j < w; j++){
               tempRow = new ArrayList<Double>();
               for(int k = 0; k < w; k++){
                  tempRow.add(Double.parseDouble(s.nextToken()));
               }
               tempMatrix.add(tempRow);
            }
            cost.add(tempMatrix);
         }

         s = new StringTokenizer(inputPher," ");

         tempRow = new ArrayList<Double>();
         tempMatrix = new ArrayList<ArrayList<Double>>();

         for(int i = 0; i < w; i++)
            tempRow.add(Double.parseDouble(s.nextToken()));
         tempMatrix.add(tempRow);
         pher.add(tempMatrix);
         for(int i = 0;i < taskNum-1;i++){
            tempMatrix = new ArrayList<ArrayList<Double>>();
            for(int j = 0; j < w; j++){
               tempRow = new ArrayList<Double>();
               for(int k = 0; k < w; k++){
                  tempRow.add(Double.parseDouble(s.nextToken()));
               }
               tempMatrix.add(tempRow);
            }
            pher.add(tempMatrix);
         }

         s = new StringTokenizer(inputDataset," ");
         for(int i = 0;i < 2500;i++){
            tempRow = new ArrayList<Double>();
            for(int j = 0;j < 5; j++){
               tempRow.add(Double.parseDouble(s.nextToken()));
            }
            datasetArray.add(tempRow);
         }


      }


      //Map function 
      public void map(LongWritable key, Text value, 
      Context context) throws IOException,InterruptedException { 

         String antid = value.toString();
         
         

         Ant2 ant = new Ant2(taskNum,cost,pher,wtList,aggList,datasetArray);
         ant.move();
         StringBuilder result = new StringBuilder();
            result.append(String.valueOf(ant.getFitness()) + " ") ;
         for(int j = 0; j < taskNum; j++)
            result.append(String.valueOf(ant.trail.get(j)) + " ");
         context.write(new Text(antid),new Text(result.toString()));
      }

   }
   
   //Reducer class 
   public static class E_EReduce extends Reducer< Text, Text, Text,Text > {
   
      //Reduce function 
      public void reduce( Text key, Iterable <Text> values, 
      Context context) throws IOException,InterruptedException { 
         for(Text i : values)
            context.write(key,i);
      } 
   }
   public static Dataset dataset;
   public static ArrayList<ArrayList<ArrayList<Double>>> cost;
   public static ArrayList<ArrayList<ArrayList<Double>>> pher;
   public static ArrayList<Double> wtList;
   public static ArrayList<String> aggList;
   public static int taskNum;
   public static int itemsPerRow ;
   public static int rowsPerTask ;
   public static double evap;

   //Main function 

   public static void main(String args[])throws Exception { 
      // System.out.println("Time Taken: " + (endTime-startTime) + "ms");
      // long endTime = System.currentTimeMillis();
      
      taskNum = 80;
      evap = 0.5;
      dataset = new Dataset(taskNum);
      itemsPerRow = 5;
      rowsPerTask = 2500/taskNum;
      Double bestValSofar = 0.0;
      wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
      aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
      cost = new ArrayList<ArrayList<ArrayList<Double>>>();
      pher = new ArrayList<ArrayList<ArrayList<Double>>>();
        
      PrintWriter outlog = new PrintWriter("output_log.txt");
      PrintWriter inputWriter = new PrintWriter("inputfile.txt");
      for(int i =0; i < taskNum; i++)
         inputWriter.write(String.valueOf(i) + "\n");
      inputWriter.flush();
      StringBuilder qwsData = new StringBuilder();
      BufferedReader br = new BufferedReader(new FileReader("qws2_csv_normalised_4.csv"));
      String nextline = br.readLine();   
      StringTokenizer tokenizer;

      while((nextline = br.readLine()) != null){
         tokenizer = new StringTokenizer(nextline,",");
         ArrayList<String> vals = new ArrayList<String>();
         while(tokenizer.hasMoreTokens())
               vals.add(tokenizer.nextToken());
         
         qwsData = qwsData.append(vals.get(6) + " ");   
         qwsData = qwsData.append(vals.get(2) + " ");   
         qwsData = qwsData.append(vals.get(1) + " ");   
         qwsData = qwsData.append(vals.get(4) + " ");   
         qwsData = qwsData.append(vals.get(0) + " ");

        }
         generateMatrices();

         StringBuilder pw1 = new StringBuilder();
         writeMatrix(pw1,cost);

         StringBuilder pw2 = new StringBuilder();
         writeMatrix(pw2,pher);



      Configuration config = new Configuration();
      FileSystem fs = FileSystem.get(config);
      fs.copyFromLocalFile(true,true,new Path("./inputfile.txt"), new Path(args[0]));
      config.set("cost",pw1.toString());
      config.set("pher",pw2.toString());
      config.set("dataset",qwsData.toString());
      config.set("taskNum",String.valueOf(taskNum));
      // config.set("mapred.min.split.size","2");
      // config.set("mapred.max.split.size","2");
      int itr = 2;
      ArrayList<Double> bestValList = new ArrayList<Double>();
      for(int i = 0;i < itr; i++){
         double bestValSofarForCurrItr = 0;
         Job job = Job.getInstance(config, "testing_stuff");  
         if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(String.valueOf(args[1])), true);
         }    
         job.setJarByClass(ProcessUnits.class);
         job.setMapperClass(E_EMapper.class); 
         job.setCombinerClass(E_EReduce.class); 
         job.setReducerClass(E_EReduce.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.waitForCompletion(true);
         evaporate();
         InputStream is = fs.open(new Path("output_dir/part-r-00000"));
         try {
            Properties props = new Properties();
            props.load(new InputStreamReader(is, "UTF8"));
            for (Map.Entry prop : props.entrySet()) {
              String name = (String)prop.getKey();
              String value = (String)prop.getValue();
            //   System.out.println("Value: " + value);
               double fitnessVal = updateTrail(value);
               bestValSofarForCurrItr = Math.max(bestValSofarForCurrItr,fitnessVal);
            }
          } 
          catch(Exception e){
            e.printStackTrace();
          }
          finally {
              is.close();
          }
          pw2 = new StringBuilder();
          writeMatrix(pw2,pher);
          config.set("pher",pw2.toString());
          outlog.write("Iteration  " + i + ": BestFitness: " +bestValSofarForCurrItr);
          bestValSofar = Math.max(bestValSofar,bestValSofarForCurrItr);
          bestValList.add(bestValSofar);
          outlog.write(" BestFitnessSoFar: " + bestValSofar);
          outlog.write("\n");
      }  
      outlog.flush();
   } 
   private static void writeMatrix(StringBuilder pw,ArrayList<ArrayList<ArrayList<Double>>> arr){
      for(int i = 0;i < arr.size();i++){
          for(int j = 0;j < arr.get(i).size(); j++){
              for(int k = 0; k < arr.get(i).get(j).size(); k++){
                  pw.append(String.valueOf( arr.get(i).get(j).get(k) ) + " ");
              }
          }
      }
  }
  private static void generateMatrices(){
   cost.add(generateCostMatrix(-1,1,dataset.getRowsPerTask()));
   for(int i = 0; i < taskNum-1; i++)
       cost.add(generateCostMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
   
   pher.add(generatePherMatrix(-1,1,dataset.getRowsPerTask()));
   for(int i = 0; i < taskNum-1; i++)
       pher.add(generatePherMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
   }
   private static ArrayList<ArrayList<Double>> generateCostMatrix(int task,int r,int c){
      ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();
      ArrayList<Double> tempRow;
      for(int i =0; i < r; i++){
          tempRow = new ArrayList<Double>();
          for(int j = 0; j < c;j++){
              tempRow.add(getDistance(task,i,j));
          }
          matrix.add(tempRow);
      }
      return matrix;
  }
  private static ArrayList<ArrayList<Double>> generatePherMatrix(int task,int r,int c){
   ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();
   ArrayList<Double> tempRow;
   for(int i =0; i < r; i++){
       tempRow = new ArrayList<Double>();
       for(int j = 0; j < c;j++){
           tempRow.add(Math.random());
       } 
       matrix.add(tempRow);
   }
   return matrix;
   }
   private static double getDistance(int task,int i, int j){
      double dist = 0;
      if(task == -1)
          for(int attr = 0; attr < dataset.getItemsPerRow(); attr++)
              dist += wtList.get(attr)*dataset.getItem(0,j,attr);
      else
          for(int attr = 0; attr < dataset.getItemsPerRow(); attr++){
              switch(aggList.get(attr)){
                  case "sum": dist += wtList.get(attr)*(dataset.getItem(task,i,attr)+dataset.getItem(task+1,j,attr));break;
                  case "mul": dist += wtList.get(attr)*(dataset.getItem(task,i,attr)*dataset.getItem(task+1,j,attr));break;
                  case "min": dist += wtList.get(attr)*Math.min(dataset.getItem(task,i,attr),dataset.getItem(task+1,j,attr));break;
                  case "max": dist += wtList.get(attr)*Math.max(dataset.getItem(task,i,attr),dataset.getItem(task+1,j,attr));break;
              }
          }
      return dist;
  }
   public static double updateTrail(String value){
         // System.out.println("UpdateTrailCalled");
         StringTokenizer s= new StringTokenizer(value," ");
         double contrib = Double.parseDouble(s.nextToken());
         ArrayList<Integer> trail = new ArrayList<Integer>();
         for(int i = 0;i < taskNum;i++)
            trail.add(Integer.parseInt(s.nextToken()));
         pher.get(0).get(0).set(trail.get(0),pher.get(0).get(0).get(trail.get(0))+contrib);
            for(int i = 1; i < taskNum; i++){
                  pher.get(i).get(trail.get(i-1))
                     .set(trail.get(i),
                     pher.get(i).get(trail.get(i-1)).get(trail.get(i))+contrib);
            }
         return contrib;
      }
      public static void evaporate(){
         for(int i =0 ;i < pher.size();i++)
             for(int j = 0;j < pher.get(i).size();j++)
                 for(int k = 0; k < pher.get(i).get(j).size();k++)
                 pher.get(i).get(j).set(k,pher.get(i).get(j).get(k)*evap);
 
     }

}