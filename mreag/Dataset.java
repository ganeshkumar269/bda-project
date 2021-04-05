package hadoop;
import com.opencsv.CSVReader;
import java.util.*;
import java.io.*;

// DataFormat: Price, throughput, availabity, reliablity, response time 
 
public class Dataset{
    private ArrayList<ArrayList<Double>> _Data;
    // private String[] head;
    private int taskNum;
    public Dataset(){
        taskNum = 10;
        readData();
    }
    public Dataset(int _taskNum){
        taskNum = _taskNum;
        readData();
    }

    private void readData(){
        try{
            CSVReader reader = new CSVReader(new FileReader("dataset_final.csv"));
            _Data = new ArrayList<ArrayList<Double>>();

            String[] nextline = reader.readNext();
            
            while((nextline = reader.readNext()) != null){
                ArrayList<Double> temp = new ArrayList<Double>();
                temp.add(Double.parseDouble(nextline[6]));   
                temp.add(Double.parseDouble(nextline[2]));   
                temp.add(Double.parseDouble(nextline[1]));   
                temp.add(Double.parseDouble(nextline[4]));   
                temp.add(Double.parseDouble(nextline[0]));  

                _Data.add(temp);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }   

    public double getItem(int task,int index,int attr){
        return _Data.get(task*getRowsPerTask()+index).get(attr);
    }

    public ArrayList<Double> getRow(int task,int index){
        return _Data.get(task*getRowsPerTask()+index);
    }

    public int getRowsPerTask(){
        return _Data.size()/taskNum;
    }

    public void setTaskNum(int _taskNum){taskNum = _taskNum;}

    public int getItemsPerRow(){
        return _Data.get(0).size();
    }
}