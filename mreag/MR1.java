package hadoop; 

import java.util.*; 
import java.io.*;


import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class MR1{
   //Mapper class
   //input -> partition \t genome(csid qos) 
   //output -> partition , genome(csid qos fitness)
    public static class customComp implements Comparator<ArrayList<Double>>{
        public int compare(ArrayList<Double> a, ArrayList<Double> b){
            return a.get(a.size()-1).compareTo(b.get(b.size()-1));
        }
    }
    public static class EMapper extends Mapper<Text,Text,Text,Text>{
        int taskNum;
        int rowsPerTask;
        int itemsPerRow = 5;
        
        protected void setup(Mapper.Context context){
            taskNum = Integer.parseInt(context.getConfiguration().get("taskNum"));
            rowsPerTask = Integer.parseInt(context.getConfiguration().get("rowsPerTask"));
            itemsPerRow = Integer.parseInt(context.getConfiguration().get("itemsPerRow"));
        }
        public void map(Text key, Text val,Context context) throws IOException, InterruptedException{
            ArrayList<ArrayList<Double>> sol = new ArrayList<ArrayList<Double>>(); 
            StringTokenizer st = new StringTokenizer(val.toString()," ");
            StringBuilder csids = new StringBuilder();
            for(int i = 0;i < taskNum; i++){
                ArrayList<Double> gene = new ArrayList<Double>();
                csids = csids.append(st.nextToken() + " ");
                for(int j =0 ;j < itemsPerRow;j++)
                    gene.add(Double.parseDouble(st.nextToken()));
                sol.add(gene);
            }
            
            System.out.println("map output: key: " + key.toString() +" value: " + csids.toString() + " fitness: " + (new Fitness(sol)).calculate());
            context.write(key,new Text(csids.toString() + " " + (new Fitness(sol)).calculate()));
        }
    }
    //Reducer
    //input -> partition, genome(csid fitness)
    //output -> fitness, genome(csid)
    public static class EReducer extends Reducer<Text,Text,Text,Text>{
        int taskNum;
        int rowsPerTask;
        int itemsPerRow = 5;
        
        protected void setup(Reducer.Context context){
            taskNum = Integer.parseInt(context.getConfiguration().get("taskNum"));
            rowsPerTask = Integer.parseInt(context.getConfiguration().get("rowsPerTask"));
            itemsPerRow = Integer.parseInt(context.getConfiguration().get("itemsPerRow"));
        }
        public void reduce(Text key, Iterable<Text> val,Context context) throws IOException, InterruptedException{
                //select parent solutions
                ArrayList<ArrayList<Double>> subPop = new ArrayList<ArrayList<Double>>();
                ArrayList<Double> sol;
                for(Text i : val){
                    sol = new ArrayList<Double>();
                    StringTokenizer st = new StringTokenizer(i.toString()," ");
                    while(st.hasMoreTokens())
                        sol.add(Double.parseDouble(st.nextToken()));
                    subPop.add(sol);
                }
                String t = new String();
                for(double i : subPop.get(0))
                    t = t + String.valueOf(i) + " ";
                System.out.println("reducer: Subpop 0: " + t);
                
                Collections.sort(subPop,new customComp());
                int lastindex = subPop.get(0).size()-1;
                for(int i = 0; i <= subPop.size()/4; i++){
                    double fitness = subPop.get(i).get(lastindex);
                    StringBuilder out = new StringBuilder();
                    for(int itr =0 ; itr < taskNum; itr++)
                        out = out.append(String.valueOf((int)subPop.get(i).get(itr).doubleValue()) + " ");
                    System.out.println("reducer output: key: " + String.valueOf(fitness) +" value: " + out.toString());
                    context.write(new Text(String.valueOf(fitness)),new Text(out.toString()));
                }
            }   
    }
    public static class EPartitioner extends Partitioner<Text,Text>{
        public int getPartition(Text key, Text val, int num){
            return Integer.parseInt(key.toString()) % num;
        }
    }

    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{
        int taskNum = 80;
        //Get Data
        Dataset dataset = new Dataset(taskNum);
        int rowsPerTask = dataset.getRowsPerTask();
        int itemsPerRow = dataset.getItemsPerRow();
        System.out.println("ItemsPerRow: " + itemsPerRow);
        int partitions = 20;
        PrintWriter pw = new PrintWriter("inputfile.txt");


        //Skyline
        ArrayList<ArrayList<ArrayList<Double>>> totalList = new ArrayList<ArrayList<ArrayList<Double>>>(); 
        for(int task = 0 ; task < taskNum; task++){
            ArrayList<ArrayList<Double>> domList = new ArrayList<ArrayList<Double>>();
            for(int i = 0;i < dataset.getRowsPerTask();i++){
                boolean canBeAdded = true;
                for(int j = 0; j < dataset.getRowsPerTask();j++){
                    int e = 0;
                    int g = 0;
                    int l = 0;
                    if(i == j) continue;
                    for(int k = 0; k < 5; k++){
                        if(dataset.getItem(task,i,k) < dataset.getItem(task,j,k))
                            l += 1;
                        if(dataset.getItem(task,i,k) == dataset.getItem(task,j,k))
                            e += 1;
                        if(dataset.getItem(task,i,k) > dataset.getItem(task,j,k))
                            g += 1;
                    }
                            
                    if(g==0 && e !=5){
                        canBeAdded = false;
                        break;
                    }
                }
                if(canBeAdded == true){
                    // pw.write(task + " ");
                    // for(int val : dataset.getRow(task,i))
                    //     pw.write(val + " ");
                    // pw.write("\n")
                    domList.add(dataset.getRow(task,i));
                }
            }
            totalList.add(domList);
        }

        //Generate Initial Probs
        ArrayList<ArrayList<Double>> prob = new ArrayList<ArrayList<Double>>();
        for(int i =0;i < taskNum; i++){
            ArrayList<Double> temp = new ArrayList<Double>();
            for(int j =0 ;j < totalList.get(i).size(); j++)
                temp.add(1.0/totalList.get(i).size());
            prob.add(temp);
        }
         

        //generate initial solutions
        //solution(genome) format -> part \t genome(csid qos)
        int numOfSols = 1000;
        for(int i = 0;i < numOfSols; i++){
            pw.write(String.valueOf(i%partitions) +"\t");
            for(int j =0 ;j < taskNum; j++){
                int index = (int) ( Math.random()*totalList.get(j).size() );
                pw.write(String.valueOf(index) + " ");
                for(double val : totalList.get(j).get(index))
                    pw.write(String.valueOf(val) + " ");
            }
            pw.write("\n");
        }
        pw.flush();
        pw.close();

        
        //Map-Reduce Phase
        Configuration config = new Configuration();
        config.set("taskNum",String.valueOf(taskNum));
        config.set("rowsPerTask",String.valueOf(rowsPerTask));
        config.set("itemsPerRow",String.valueOf(itemsPerRow));
        int noOfItrs = 2;
        double maxFitness = 0;
        ArrayList<Double> fitnessData = new ArrayList<Double>();
        for(int itrs = 0; itrs < noOfItrs;itrs++ ){            
            System.out.println("Iteration: " + itrs);
            Job job = Job.getInstance(config,"test MR1");
            FileSystem fs = FileSystem.get(config);
            fs.copyFromLocalFile(true,true,new Path("./inputfile.txt"),new Path(args[0]));
            if (fs.exists(new Path(args[1]))) 
                fs.delete(new Path(String.valueOf(args[1])), true);
            job.setJarByClass(MR1.class);
            job.setMapperClass(EMapper.class); 
            job.setReducerClass(EReducer.class);
            job.setPartitionerClass(EPartitioner.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            //Get Reducer output (set of genomes w/fitness)
            ArrayList<ArrayList<Integer>> reducerOutput = new  ArrayList<ArrayList<Integer>>();
            //keep track of the max fitness of the reducer outputs
            double maxFitnessSoFar = 0;
            //new totalList wrt reducer Output
            ArrayList<ArrayList<Integer>> newTotalList = new ArrayList<ArrayList<Integer>>();
            ArrayList<HashSet<Integer>> newTotalList_hs = new ArrayList<HashSet<Integer>>();
            for(int i = 0; i < taskNum; i++)
                newTotalList_hs.add(new HashSet<Integer>());
            int indexOfBestSol = 0;
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
                    int itr2 = 0;
                    while(st.hasMoreTokens()){
                        int val = Integer.parseInt(st.nextToken());
                        tempSol.add(val);
                        newTotalList_hs.get(itr2).add(val);
                        itr2++;
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
            fitnessData.add(maxFitnessSoFar);
            System.out.println("MaxFitnessSoFar: " + maxFitnessSoFar);
            maxFitness = Math.max(maxFitness,maxFitnessSoFar);
            for(HashSet<Integer> hs: newTotalList_hs){
                ArrayList<Integer> tempList = new ArrayList<Integer>();
                Iterator<Integer> itr = hs.iterator();
                while(itr.hasNext())
                    tempList.add(itr.next());
                newTotalList.add(tempList);
            }
            // System.out.println("maxFitnessSoFar: " + maxFitnessSoFar);

            //initialise cnts
            ArrayList<ArrayList<Integer>> cnts = new ArrayList<ArrayList<Integer>>();  
            for(int i =0 ;i < taskNum; i++){
                ArrayList<Integer> cnt = new ArrayList<Integer>();
                for(int j = 0; j < totalList.get(i).size();j++)
                    cnt.add(0);
                cnts.add(cnt);
            }
            //update cnts
            for(ArrayList<Integer> i : reducerOutput){
                for(int j = 0; j < i.size(); j++){
                    cnts.get(j).set( i.get(j) , cnts.get(j).get( i.get(j) ) + 1);
                }
            }
            System.out.println("Updated Cnts");
            //update probabilities using Alg-2
            double lambda = 0.34;
            for(int i = 0;i < taskNum; i++){
                for(int j =0; j < totalList.get(i).size(); j++){
                    prob.get(i).set(j , (1-lambda)*prob.get(i).get(j) + ( lambda*4*cnts.get(i).get(j) / numOfSols ) );
                }
            }

            System.out.println("Updated Probs");

            ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>(); 
            
            //Generate New Population using Alg-3
            //Guided Mutation
            double beta = 0.035;
            double th_prob = 0.55;
            for(int i = 0;i < taskNum; i++){
                ArrayList<Integer> temp = new ArrayList<Integer>();
                for(int j = 0; j < totalList.get(i).size(); j++){
                    double r = Math.random();
                    if( r < beta){
                        if(prob.get(i).get(j) < th_prob){
                            newTotalList.get(i).add(j);
                        }
                    }
                }
            }
            System.out.println("Guided Mutation Completed");
           
            //Repair Phase - Alg-4
            for(int i =0 ;i < taskNum; i++){
                if(newTotalList.get(i).size() < 0.3*totalList.get(i).size()){
                    for(int j = 0; j < (0.3*totalList.get(i).size() - newTotalList.get(i).size()) ;j++)
                        newTotalList.get(i).add((int)(Math.random()*totalList.get(i).size()));
                }
            }
            //generate new solutions
            
            pw = new PrintWriter("inputfile.txt");
            for(int i =0 ;i < numOfSols-1; i++){
                StringBuilder out = new StringBuilder();
                out = out.append(String.valueOf((int)i%partitions) +"\t");
                for(int j = 0; j < taskNum; j++){
                    int csid = (int)newTotalList.get(j).get((int)(Math.random()*newTotalList.get(j).size()));
                    out = out.append(String.valueOf(csid) + " ");
                    for(double k : totalList.get(j).get(csid))
                        out = out.append(String.valueOf(k) + " ");
                }
                pw.write(out.toString());
                pw.write("\n");
            }
            StringBuilder tempSB = new StringBuilder();
            tempSB = tempSB.append(String.valueOf((numOfSols-1)%partitions) + "\t");
            for(int i =0 ; i < taskNum; i++){
                int csid = reducerOutput.get(indexOfBestSol).get(i);
                tempSB = tempSB.append(String.valueOf(csid) + " ");
                for(double k : totalList.get(i).get(csid))
                    tempSB = tempSB.append(String.valueOf(k) + " ");
            }
            pw.write(tempSB.toString());
            pw.write("\n");
            pw.flush();
            pw.close();
            System.out.println("New Solutions generated");
        }
        PrintWriter outputWriter = new PrintWriter("output_log.txt");
        for(double i : fitnessData)
            outputWriter.write(String.valueOf(i) + "\n");
        outputWriter.flush();
        System.out.println("Max Fitness: " + maxFitness);
    }
}