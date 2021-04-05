package hadoop;


import java.util.*;


public class ACO2{
    ArrayList<ArrayList<ArrayList<Double>>> cost;
    ArrayList<ArrayList<ArrayList<Double>>> pher;
    ArrayList<Double> wtList;
    ArrayList<String> aggList;
    Dataset dataset;
    Fitness fitness;
    // ArrayList<Ant> ants;
    ArrayList<Integer> bestFitnessValTrail;
    private double bestFitnessVal;
    private double c;
    private double alpha;
    private double beta;
    private double evap;
    private double Q;
    private double antFactor;
    private double randFactor;
    private double numOfAnts;
    private int numOfItr;

    public int taskNum;
    public ACO2(){
        taskNum = 10;
        init();
    }
    public ACO2(int _taskNum){
        taskNum = _taskNum;
        init();
    }
    private void init(){
        numOfItr = 3;
        cost = new ArrayList<ArrayList<ArrayList<Double>>>(); 
        pher = new ArrayList<ArrayList<ArrayList<Double>>>(); 
        wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
        aggList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
        // ants = new ArrayList<Ant>();

        c = 1.0;
        alpha = 1.0;
        beta = 0.5;
        evap = 0.5;
        Q = 500;
        antFactor = 0.8;
        randFactor = 0.1;
        numOfAnts = 10;
        dataset = new Dataset(taskNum);
        bestFitnessVal = -Double.MAX_VALUE;
        generateMatrices();
        
    }

    public void setTaskNum(int _taskNum){taskNum = _taskNum;}
    public void setNumOfItr(int _numOfItr){numOfItr = _numOfItr;}
    public void setNumOfAnts(int _numOfAnts){
        numOfAnts = _numOfAnts;
    }

    // public void setAnts(){
    //     ants.clear();
    //     for(int i = 0;i < numOfAnts;i++)
    //         ants.add(new Ant(taskNum));
    // }

    private void generateMatrices(){
        cost.add(generateCostMatrix(-1,1,dataset.getRowsPerTask()));
        for(int i = 0; i < taskNum-1; i++)
            cost.add(generateCostMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
        
        pher.add(generatePherMatrix(-1,1,dataset.getRowsPerTask()));
        for(int i = 0; i < taskNum-1; i++)
            pher.add(generatePherMatrix(i,dataset.getRowsPerTask(),dataset.getRowsPerTask()));
    }



    private ArrayList<ArrayList<Double>> generateCostMatrix(int task,int r,int c){
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


    private ArrayList<ArrayList<Double>> generatePherMatrix(int task,int r,int c){
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


    private double getDistance(int task,int i, int j){
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


    // public void run(){
    //     for(int i = 0; i < numOfItr; i++){
    //         moveAnts();
    //         updateTrails();
    //         System.out.println("Iteration "+i+" Best Fitness So far: " + bestFitnessVal);
    //     }
    // }

    // private void moveAnts(){
    //     for(int ind = 0;ind < taskNum; ind++){
    //         for(Ant ant : ants){
    //             // int selection = makeSelection(arr.get(ind).get(ant.getState()));
    //             int selection = makeSelection(ind,ant.getState());
    //             ant.visit(ind,selection);
    //         }
    //     }
    //     for(Ant ant : ants) ant.setState(0);

    // }

    //Evaporation
    public void evaporate(){
        for(int i =0 ;i < pher.size();i++)
            for(int j = 0;j < pher.get(i).size();j++)
                for(int k = 0; k < pher.get(i).get(j).size();k++)
                pher.get(i).get(j).set(k,pher.get(i).get(j).get(k)*evap);

    }

    // private void updateTrails(){
    //     for(Ant ant : ants){
    //         ArrayList<ArrayList<Double>> path = new ArrayList<ArrayList<Double>>();
    //         ArrayList<Integer> trail = ant.getTrail();
    //         for(int ind =0; ind < taskNum;ind++)
    //             path.add(dataset.getRow(ind,trail.get(ind)));
    //         fitness = new Fitness(path,aggList,wtList);
    //         double fitnessVal = fitness.calculate(); 
    //         // System.out.println("Current Fitness: "+ fitnessVal);
    //         updateBest(trail,fitnessVal);
    //         double contrib = fitnessVal;
    //         pher.get(0).get(0).set(trail.get(0),pher.get(0).get(0).get(trail.get(0))+contrib);
    //         for(int i = 1; i < taskNum; i++){
    //             pher.get(i).get(trail.get(i-1))
    //                 .set(trail.get(i),
    //                 pher.get(i).get(trail.get(i-1)).get(trail.get(i))+contrib);
    //         }
    //     }
    // }

    private void updateBest(ArrayList<Integer> trail,double val){
        if(bestFitnessVal < val){
            bestFitnessVal = val;
            bestFitnessValTrail = new ArrayList<Integer>(trail);
        }
    }

    // private int makeSelection(int task,int state){

    //     // randomly choose if we should make a random decision
    //     if(Math.random() < randFactor){
    //         return (int)(Math.random()*(double)dataset.getItemsPerRow());
    //     }

    //     //Calculate Probablities
    //     ArrayList<Double> prob = new ArrayList<Double>();
    //     double pheromone = 0.0;
    //     for(int ch =0 ;ch < dataset.getRowsPerTask();ch++){
    //         pheromone +=    (
    //                         Math.pow(pher.get(task).get(state).get(ch),alpha)
    //                         * Math.pow(cost.get(task).get(state).get(ch),beta)
    //                         );
    //     }

    //     for(int ch =0 ;ch < dataset.getRowsPerTask();ch++){
    //         double numerator = Math.pow(pher.get(task).get(state).get(ch),alpha)
    //         * Math.pow(cost.get(task).get(state).get(ch),beta);
    //         prob.add(numerator/pheromone);
    //     }

    //     //making selection
    //     double total = 0;
    //     double randVal = Math.random();
    //     for(int i = 0; i < prob.size(); i++){
    //         total += prob.get(i);
    //         if (total >= randVal)
    //             return i;
    //     }
    //     return 0;
    // }

    public void printMetrics(){
        System.out.println("Best Fitness: " + bestFitnessVal);
        System.out.println(bestFitnessValTrail);
    }
}