package hadoop;
import java.util.*;


public class Fitness{
    private ArrayList<ArrayList<Double>> arr;
    private ArrayList<String>  aggFnList;
    private ArrayList<Double>  wtList;

    public Fitness(){arr=null;}
    public Fitness( 
                    ArrayList<ArrayList<Double>> _Arr,
                    ArrayList<String> _aggFnList,
                    ArrayList<Double>  _wtList
                )
    {
        arr = _Arr;
        aggFnList = _aggFnList;
        wtList = _wtList;
    }
    public Fitness(ArrayList<ArrayList<Double>> _Arr){
        arr = _Arr;
        wtList = new ArrayList<Double>(Arrays.asList(0.1417,0.1373,0.3481,0.964,0.325));
        aggFnList = new ArrayList<String>(Arrays.asList("sum","min","mul","mul","sum"));
    }

    private double getSum(int index){
        double sum = 0;
        for(int i = 0; i < arr.size(); i++)
            sum += arr.get(i).get(index);
        return sum;
    }

    private double getMul(int index){
        double mul = 1;
        for(int i = 0; i < arr.size(); i++)
            mul *= arr.get(i).get(index);
        return mul;
    }
    private double getMin(int index){
        double _min = Double.MAX_VALUE;
        for(int i = 0; i < arr.size(); i++)
            _min =  Math.min(arr.get(i).get(index),_min);
        return _min;
    }
    private double getMax(int index){
        double _max = Double.MIN_VALUE;
        for(int i = 0; i < arr.size(); i++)
            _max =  Math.max(arr.get(i).get(index),_max);
        return _max;
    }

    public double calculate(){
        double fitness = 0;
        for(int i = 0; i < arr.get(0).size(); i++){
            switch (aggFnList.get(i)) {
                case "sum": fitness += wtList.get(i)*getSum(i);
                    break;
                case "mul": fitness += wtList.get(i)*getMul(i);
                    break;
                case "min": fitness += wtList.get(i)*getMin(i);
                    break;
                case "max": fitness += wtList.get(i)*getMax(i);
                    break;
                default:
                    break;
            }
        }
        return fitness;
    }
}