package hadoop;
import java.util.*;

public class Ant2{
    ArrayList<Integer> trail;
    ArrayList<ArrayList<Double>> path;
    private int state;
    // Dataset dataset;
    ArrayList<ArrayList<ArrayList<Double>>> cost;
    ArrayList<ArrayList<ArrayList<Double>>> pher;
    ArrayList<ArrayList<Double>> datasetArray;
    Fitness fitness;
    ArrayList<Double> wtList;
    ArrayList<String> aggList;
    int taskNum;
    private double alpha;
    private double beta;
    private double evap;
    private double Q;
    private double antFactor;
    private double randFactor;
    private int rowsPerTask;
    private int itemsPerRow;
    public Ant2(int _taskNum){
        taskNum = _taskNum;
        alpha = 1.0;
        beta = 0.5;
        evap = 0.5;
        Q = 500;
        antFactor = 0.8;
        randFactor = 0.1;
        trail = new ArrayList<Integer>();
        path = new ArrayList<ArrayList<Double>>();
        for(int i = 0;i < taskNum; i++){
            trail.add(0);
            path.add(new ArrayList<Double>());
        }
        state = 0;
    }

    public Ant2(int _taskNum,ArrayList<ArrayList<ArrayList<Double>>> _cost,
            ArrayList<ArrayList<ArrayList<Double>>> _pher,
            ArrayList<Double> _wtList,
            ArrayList<String> _aggList,
            ArrayList<ArrayList<Double>> _datasetArray
            ){

        datasetArray = _datasetArray;
        taskNum = _taskNum;
        cost = _cost;
        pher = _pher;
        wtList = _wtList;
        aggList = _aggList;
        rowsPerTask = 2500/taskNum;
        itemsPerRow = 5;
        // dataset = _dataset;
        trail = new ArrayList<Integer>();
        path = new ArrayList<ArrayList<Double>>();
        for(int i = 0;i < taskNum; i++){
            trail.add(0);
            path.add(new ArrayList<Double>());
        }
        state = 0;
    }



    public void visit(int task, int value) {
        // System.out.println(task + " " + trail.size());
        trail.set(task, value);
        path.set(task,datasetArray.get(task*rowsPerTask + value));
        state = value;
    }

    public int getState(){return state;}
    public void setState(int _state){state = _state;}

    public void setCost(ArrayList<ArrayList<ArrayList<Double>>> _cost){cost = _cost;}
    public void setPher(ArrayList<ArrayList<ArrayList<Double>>> _pher){pher = _pher;}
    // public void setDataset(Dataset _dataset){dataset = _dataset;}
    public void setAggList(ArrayList<String> _aggList){aggList = _aggList;}
    public void setwtList(ArrayList<Double> _wtList){wtList = _wtList;}

    public ArrayList<Integer> getTrail(){return trail;}

    public void move(){
        for(int ind = 0; ind < taskNum; ind++){
            int selection = makeSelection(ind,getState());
            visit(ind,selection);
        }
        setState(0);
    }

    public int makeSelection(int task,int state){

        // randomly choose if we should make a random decision
        if(Math.random() < randFactor){
            return (int)(Math.random()*(double)rowsPerTask);
        }

        //Calculate Probablities
        ArrayList<Double> prob = new ArrayList<Double>();
        double pheromone = 0.0;
        for(int ch =0 ;ch < rowsPerTask;ch++){
            pheromone +=    (
                            Math.pow(pher.get(task).get(state).get(ch),alpha)
                            * Math.pow(cost.get(task).get(state).get(ch),beta)
                            );
        }

        for(int ch =0 ;ch < rowsPerTask;ch++){
            double numerator = Math.pow(pher.get(task).get(state).get(ch),alpha)
            * Math.pow(cost.get(task).get(state).get(ch),beta);
            prob.add(numerator/pheromone);
        }

        //making selection
        double total = 0;
        double randVal = Math.random();
        for(int i = 0; i < prob.size(); i++){
            total += prob.get(i);
            if (total >= randVal)
                return i;
        }
        return 0;
    }

    public double getFitness(){
        fitness = new Fitness(path,aggList,wtList);
        return fitness.calculate();
    }

    // public void updateTrail(){
    //     double contrib = getFitness();
    //     pher.get(0).get(0).set(trail.get(0),pher.get(0).get(0).get(trail.get(0))+contrib);
    //         for(int i = 1; i < taskNum; i++){
    //             pher.get(i).get(trail.get(i-1))
    //                 .set(trail.get(i),
    //                 pher.get(i).get(trail.get(i-1)).get(trail.get(i))+contrib);
    //         }
    // }

}