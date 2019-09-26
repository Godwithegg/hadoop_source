import java.util.*;

public class Main{
    public static void main(String[] args){

        Scanner in = new Scanner(System.in);
        int n= in.nextInt();
        People[] people = new People[n];
        Map<String,Integer> map = new TreeMap<>();
        for(int i=0;i<n;i++){
            people[i]=new People(in.next(), in.next());
        }
        Set<String> set = new HashSet<>();
        for(int i=0;i<n;i++){
            set.add(people[i].toString());
        }
        people = new People[set.size()];
        int index=0;
        for (String s : set) {
            String[] s1 = s.split(" ");
            people[index++]=new People(s1[0],s1[1] );
        }
        Arrays.sort(people,0 ,index );
        for(int i=0;i<index;i++){
            for(int j=0;j<index;j++){
                if(i==j)continue;
                if(people[i].value.equals(people[j].value)){
                    if(people[i].key.equals(people[j].key))continue;
                    String line = people[i].key+" "+people[j].key;
                    //System.out.println(line);
                    if(map.containsKey(line)){
                        map.put(line,map.get(line)+1 );
                    }else{
                        map.put(line,1 );
                    }
                }
            }
        }
        for (String s : map.keySet()) {
            if(map.get(s)>2){
                System.out.println(s+" "+map.get(s));
            }
        }



    }
}
class People implements Comparable<People>{
    String key;
    String value;
    public People(String key,String value){
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key +" "+ value;
    }

    @Override
    public int compareTo(People o) {
        int result=0;
        if(this.key.compareTo(o.key)>0){
            result=1;
        }else if(this.key.compareTo(o.key)<0){
            result=-1;
        }else{
            if(this.value.compareTo(o.value)>0){
                result=1;
            }else if(this.value.compareTo(o.value)<0){
                result=-1;
            }
        }
        return result;
    }
}