package hadoop; 

import java.util.*; 
import java.io.*;

import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ABC {
    public static class EMapper extends  Mapper<Text, Text, Text, Text>        
    {
      
        public void map(Text key, Text value, Context context) throws IOException,InterruptedException { 
            System.out.println("Key "+key.toString() + " Value: " + value.toString());
            context.write(key ,value);
        }
    }
    public static class EReduce extends Reducer< Text, Text, Text,Text > {
        ArrayList<ArrayList<Double>> dataset;
        int limit = 1;
        int taskNum;
        int l_b = 0;
        int u_b = 0;
        int rowsPerTask;
        int itemsPerRow;
        public void setup(Reducer.Context context){    
            String inputDataset;
            taskNum = Integer.parseInt(context.getConfiguration().get("taskNum"));
            rowsPerTask = Integer.parseInt(context.getConfiguration().get("rowsPerTask"));
            itemsPerRow = Integer.parseInt(context.getConfiguration().get("itemsPerRow"));
            limit = Integer.parseInt(context.getConfiguration().get("limit"));
            l_b = Integer.parseInt(context.getConfiguration().get("l_b"));
            u_b = Integer.parseInt(context.getConfiguration().get("u_b"));
            inputDataset= context.getConfiguration().get("inputDataset");
            dataset = new ArrayList<ArrayList<Double>>();
            StringTokenizer s = new StringTokenizer(inputDataset," ");
            ArrayList<Double> tempRow;
            for(int i = 0;i < 2500;i++){
                tempRow = new ArrayList<Double>();
                for(int j = 0;j < itemsPerRow; j++){
                    tempRow.add(Double.parseDouble(s.nextToken()));
                }
                dataset.add(tempRow);
            }
        }
        public double getFitness(ArrayList<Integer> t){
            ArrayList<ArrayList<Double>> path = new ArrayList<ArrayList<Double>>();
            for(int i =0; i < taskNum; i++){
                path.add(dataset.get(i*rowsPerTask + t.get(i)));
            }
            return (new Fitness(path)).calculate();
        }
        public void reduce( Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException { 
            ArrayList<ArrayList<Integer>> foods = new ArrayList<ArrayList<Integer>>();  
            ArrayList<Integer> trail = new ArrayList<Integer>();
            System.out.println("Dataset Size: " + dataset.size());
            for(Text i : values){
                StringTokenizer st = new StringTokenizer(i.toString()," ");
                ArrayList<Integer> temp = new ArrayList<Integer>();
                for(int itr = 0;itr < taskNum;itr++)
                    temp.add(Integer.parseInt(st.nextToken()));
                trail.add(Integer.parseInt(st.nextToken()));
                foods.add(temp);
            }     
            //Objective Score array
            ArrayList<Double> obj = new ArrayList<Double>();
            //Fitness score array
            ArrayList<Double> fit = new ArrayList<Double>();
            for(int i = 0; i < foods.size(); i++)
                fit.add(getFitness(foods.get(i)));  
            System.out.println("Going to Emp Phase");
            //Emp phase
            for(int  i =0; i < foods.size();i++){
                int partner;
                while((partner = (int) (foods.size()*Math.random())) != i);
                int gene = (int)( foods.get(0).size()*Math.random() );
                double  phi = Math.random()*2 -1;
                int newVal = (int)( foods.get(i).get(gene) + phi* (foods.get(i).get(gene) - foods.get(partner).get(gene)));
                if(newVal < l_b) newVal = 0;
                if(newVal > u_b) newVal = u_b;
                int oldVal = foods.get(i).get(gene);
                foods.get(i).set(gene,newVal);
                double fitnessVal = getFitness(foods.get(i));
                if(fit.get(i) > fitnessVal){ //new solution is better, lower the fitness better the solution, thus gene gets replaced
                    trail.set(i,0);
                    fit.set(i,fitnessVal);
                } 
                else{
                    trail.set(i,trail.get(i)+1);
                    foods.get(i).set(gene,oldVal); //gene value restored to oldValue
                }

            }
            System.out.println("Emp Phase Completed");
            System.out.println("Onlooker Started");
            //Onlooker Phase
            ArrayList<Double> prob = new ArrayList<Double>();
            double fitnessSum = 0;
            for( double i : fit) fitnessSum += i; 
            for(int i =0 ; i < fit.size(); i++)
                prob.add(fit.get(i)/fitnessSum);
            
            int index =0 ;
            for(int i =0 ;i < foods.size(); i++){
                double r = Math.random();

                System.out.println("The while loop start");
                double _temp_var = Math.random();
                System.out.println("_temp_var " + _temp_var);
                System.out.println("prob val " + prob.get(index));
                while(_temp_var < prob.get(index)){    
                    System.out.println("_temp_var " + _temp_var);
                    System.out.println("prob val " + prob.get(index));
                    index = (index+1)%foods.size();
                    _temp_var = Math.random();
                }
                System.out.println("The while loop end");
                int partner;
                while((partner = (int) (foods.size()*Math.random())) != index);
                int gene = (int)( foods.get(0).size()*Math.random() );
                double  phi = Math.random()*2 -1;
                int newVal = (int) (foods.get(index).get(gene) + phi* (foods.get(index).get(gene) - foods.get(partner).get(gene)));
                int oldVal = foods.get(index).get(gene);
                if(newVal < l_b) newVal = 0;
                if(newVal > u_b) newVal = u_b;
                foods.get(index).set(gene,newVal);
                double fitnessVal = getFitness(foods.get(index));
                if(fit.get(i) > fitnessVal){ //new solution is better, lower the fitness better the solution, thus gene gets replaced
                    trail.set(index,0);
                    fit.set(index,fitnessVal);
                } 
                else{
                    trail.set(index,trail.get(index)+1);
                    foods.get(index).set(gene,oldVal); //gene value restored to oldValue
                }
            }
            double maxFitness = 0;
            double maxFitnessIndex =0;
            for(int i = 0; i < fit.size(); i++)
                if(maxFitness < fit.get(i)){
                    maxFitness = fit.get(i);
                    maxFitnessIndex = i;
                }
            System.out.println("Onlooker Phase completed");
            //Scout Phase
            ArrayList<Integer> reset = new ArrayList<Integer>();
            for(int i =0 ; i < trail.size();i++){
                if(trail.get(i) > limit) 
                    reset.add(i);
            }
            if(reset.size() > 0){
                int ind = (int) (Math.random()*reset.size());
                int partner;
                while((partner = (int) (foods.size()*Math.random())) != ind);
                int gene = (int)( foods.get(0).size()*Math.random() );
                double phi = Math.random()*2 -1;
                int newVal = (int) ( foods.get(ind).get(gene) + phi* (foods.get(ind).get(gene) - foods.get(partner).get(gene)));
                if(newVal < l_b) newVal = 0;
                if(newVal > u_b) newVal = u_b;
                foods.get(ind).set(gene,newVal);
                fit.set(ind,getFitness(foods.get(ind)));
            }

            System.out.println("Scout Phase completed");

            for(int i =0 ;i < foods.size(); i++){
                StringBuilder out = new StringBuilder();
                for(int t : foods.get(i))
                    out = out.append(String.valueOf(t) +" ");
                context.write(new Text(String.valueOf(fit.get(i))), new Text(out.toString() + " " + trail.get(i)));
            }
            System.out.println("R Completed");
        }
    }
    public static class EPartitioner extends Partitioner<Text,Text>{
        public int getPartition(Text key, Text val, int num){
            return Integer.parseInt(key.toString()) % num;
        }
    }

    public static void main(String... args) throws IOException, InterruptedException,ClassNotFoundException{

        int taskNum = 10;
        int swarmSize = 100;
        Dataset dataset = new Dataset(taskNum);
        int rowsPerTask = dataset.getRowsPerTask();
        int itemsPerRow = dataset.getItemsPerRow();
        System.out.println("ItemsPerRow: " + itemsPerRow);
        int partitions = 10;
        int limit = 5;
        PrintWriter pw = new PrintWriter("inputfile.txt");
        for(int i = 0;i < swarmSize; i++){
            pw.write(String.valueOf(i%partitions) +"\t");
            for(int j =0 ;j < taskNum; j++){
                int index = (int) ( Math.random()*dataset.getRowsPerTask());
                pw.write(String.valueOf(index) + " ");
            }
            pw.write("0 \n"); //this is trail value of a food source
        }
        pw.flush();
        pw.close();

        ArrayList<Integer> temparr = new ArrayList<Integer>();
        for(int i = 0;i < 10; i++)
            temparr.add(i);
        Configuration config = new Configuration();
        //write dataset to config

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


        config.set("inputDataset",qwsData.toString());
        config.set("taskNum",String.valueOf(taskNum));
        config.set("rowsPerTask",String.valueOf(rowsPerTask));
        config.set("itemsPerRow",String.valueOf(itemsPerRow));
        config.set("limit",String.valueOf(limit));
        config.set("l_b",String.valueOf(0));
        config.set("u_b",String.valueOf(rowsPerTask-1));

        FileSystem fs = FileSystem.get(config);
        if (fs.exists(new Path(args[1])))
            fs.delete(new Path(String.valueOf(args[1])), true);
        fs.copyFromLocalFile(true,true,new Path("./inputfile.txt"),new Path(args[0]));
        // FSDataOutputStream outs = fs.create(new Path("thisiscreated.txt"),true);
        // outs.write("Thi is is created".getBytes());
        // outs.hflush();
        // outs.close();
        
        // FileOutputStream fos = new FileOutputStream("object.dat");
        // ObjectOutputStream oos = new ObjectOutputStream(fos);
        // oos.writeObject(temparr);
        // oos.close();  
        int numOfItrs = 10;
        for(int _itr_ = 0; _itr_ < numOfItrs; _itr_++){

            Job job = Job.getInstance(config, "ABC");  
            job.setJarByClass(ABC.class);
            job.setMapperClass(EMapper.class); 
            job.setPartitionerClass(EPartitioner.class);
            job.setCombinerClass(EReduce.class); 
            job.setReducerClass(EReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
            System.out.println("MR-Over");
            double maxFitnessSoFar = 0;
            int indexOfBestSol = 0;
            ArrayList<ArrayList<Integer>> reducerOutput = new  ArrayList<ArrayList<Integer>>();
            InputStream is = fs.open(new Path("output_dir/part-r-00000"));
            try {
                Properties props = new Properties();
                props.load(new InputStreamReader(is, "UTF8"));
                int itr = 0;
                for (Map.Entry prop : props.entrySet()) {
                    String name = (String)prop.getKey();
                    String value = (String)prop.getValue();
                    // System.out.println("Driver: key " + name + " value " + value);
                    double fitnessVal = Double.parseDouble(name);
                    if(maxFitnessSoFar < fitnessVal){
                        maxFitnessSoFar = Math.max(maxFitnessSoFar,fitnessVal);
                        indexOfBestSol = itr; 
                    }
                    StringTokenizer st = new StringTokenizer(value.toString()," ");
                    ArrayList<Integer> tempSol = new ArrayList<Integer>();
                    while(st.hasMoreTokens()){
                        int val = Integer.parseInt(st.nextToken());
                        tempSol.add(val);
                        // newTotalList_hs.get(itr).add(val);
                    }
                    reducerOutput.add(tempSol);
                    itr++;
                }
            } 
            catch(Exception e){
                e.printStackTrace();
            }
            finally {
                is.close();
            }
            System.out.println("MaxFitnessSoFar: " + maxFitnessSoFar);
        }
    }
}