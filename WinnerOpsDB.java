package winneropsdb;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WinnerOpsDB {
    
    /*---------------------Auxilary method for creating objects of winners----*/
    
    public static WinnerClass auxWinner(String[] str){
         
            return new WinnerClass(Integer.parseInt(str[1]),Integer.parseInt(str[2]),str[3],str[4]);
        }
     
    /*--------------------------Loading the database--------------------------*/
    
    public static Stream<Winner> loadData(String[] paths) throws IOException{
        Stream<Winner> maleWinners = null;
        Stream<Winner> femaleWinners= null;
        try{
            
          maleWinners = Files.lines(Paths.get(paths[0])).skip(1)
                  .map(s -> s.split(","))
                  .map(ss -> auxWinner(ss));
          femaleWinners = Files.lines(Paths.get(paths[1])).skip(1)
                  .map(s -> s.split(","))
                  .map(ss -> auxWinner(ss));
                  
        }catch(IOException e){}
        
      return Stream.concat(maleWinners, femaleWinners);
    }
    
    /*----------------------youngerWinners() method---------------------------*/
    
    public static Stream<Winner> youngWinners(Stream<Winner> winners){
       Stream<Winner> youngWinners = winners.filter(w -> w.getWinnerAge() < 35)
              .sorted((ww1,ww2) -> ww1.getWinnerName().compareTo(ww2.getWinnerName()));
               
     return youngWinners ;
    }
    
    /*--------------------------extreamWinners() method ----------------------*/
    
   public static Stream<Winner> extreamWinners(Stream<Winner> winners){
           
       List<Winner> allWinner =  winners.sorted((w1,w2)-> Integer.compare(w1.getWinnerAge(),w2.getWinnerAge())).collect(Collectors.toList());
       Stream<Winner> extreamWinners = Stream.of(allWinner.get(0),allWinner.get(allWinner.size()-1));
         
     return extreamWinners;
    }
   
   /*-----------------multiAwardedPerson() method-----------------------------*/
   
   public static Stream<String> multiAwardedPerson(Stream<Winner> winners)throws IOException{
        Map<String , List<Winner>> li = winners.collect(Collectors.groupingBy(Winner :: getWinnerName));
        Stream<String> multiAwardedPerson = li.values().stream().filter(s -> s.size() >= 2)
                .map(s -> s.get(0))
                .sorted((s1,s2) -> s1.getWinnerName().compareTo(s2.getWinnerName()))
                .map(ss -> ss.getWinnerName());
       
    return multiAwardedPerson;
    }
   
   /* ---------------------multiAwardedFilm() method -------------------------*/
     
   public static Stream<String> multiAwardedFilm(Stream<Winner> winners){
   
       Map<String , List<Winner>> li = winners.collect(Collectors.groupingBy(Winner :: getFilmTitle));
       Stream<String> multiAwardedFilm  = li.values().stream().filter(s -> s.size() == 2)
              .map(s -> s.get(0))
              .sorted((s1,s2) -> Integer.compare(s1.getYear(), s2.getYear()))
               .map(ss -> ss.getFilmTitle());
       
       
   return multiAwardedFilm;
   }
   
   /*------------------------measure() method---------------------------------*/
   
   public static long measure(Function<Stream<Winner>, Stream<String>> f , Stream<Winner> s1){
     
       Stream<String> s2 = f.apply(s1);
       long t1 = System.nanoTime();
       s2.collect(Collectors.toList());
       long t2 = System.nanoTime();
   
    return t2 - t1 ;   
    } 
   
   
   /*-------------------------youngWinnersParallel() method ------------------*/
   
   public static Stream<Winner> youngWinnersParallel(Stream<Winner> winners){
       Stream<Winner> youngWinners = winners.parallel().filter(w -> w.getWinnerAge() < 35)
              .sorted((ww1,ww2) -> ww1.getWinnerName().compareTo(ww2.getWinnerName()));
               
     return youngWinners ;
   }
   
   /*----------------------extreamWinnersParallel() method--------------------*/
   
   public static Stream<Winner> extreamWinnersParallel(Stream<Winner> winners){
       List<Winner> allWinner =  winners.parallel().sorted((w1,w2)-> Integer.compare(w1.getWinnerAge(),w2.getWinnerAge())).collect(Collectors.toList());
       Stream<Winner> extreamWinners = Stream.of(allWinner.get(0),allWinner.get(allWinner.size()-1));
     return extreamWinners;
   }
   
  /*---------------------------multiAwardedPersonParallel() method -----------*/
   
   public static Stream<String> multiAwardedPersonParallel(Stream<Winner> winners){
       Map<String , List<Winner>> li = winners.parallel().collect(Collectors.groupingBy(Winner :: getWinnerName));
        Stream<String> multiAwardedPerson = li.values().parallelStream().filter(s -> s.size() >= 2)
                .map(s -> s.get(0))
                .sorted((s1,s2) -> s1.getWinnerName().compareTo(s2.getWinnerName()))
                .map(ss -> ss.getWinnerName());
       
    return multiAwardedPerson;
   }
   
   /*--------------------multiAwardedFilmParallel() method--------------------*/
   
    public static Stream<String> multiAwardedFilmParallel(Stream<Winner> winners){
   
       Map<String , List<Winner>> li = winners.parallel().collect(Collectors.groupingBy(Winner :: getFilmTitle));
       Stream<String> multiAwardedFilm  = li.values().parallelStream().filter(s -> s.size() == 2)
              .map(s -> s.get(0))
              .sorted((s1,s2) -> Integer.compare(s1.getYear(), s2.getYear()))
               .map(ss -> ss.getFilmTitle());
      
   return multiAwardedFilm;
   }
    
    /*-------------------comparison() method----------------------------------*/
    
    public static long[] comparison(Stream<Winner> winners) throws IOException{
       String[] addr = {"C:\\Users\\HP x360\\Desktop\\oscar_age_male.csv" ,"C:\\Users\\HP x360\\Desktop\\oscar_age_female.csv" }; 
       long[] executionTime;
        executionTime = new long[11];
       long y1 = System.nanoTime();
       youngWinners(loadData(addr));
       long y2 = System.nanoTime();
       executionTime[2] = y2 - y1 ;
       
       long e1 = System.nanoTime();
       extreamWinners(loadData(addr));
       long e2 = System.nanoTime();
       executionTime[3] = e2 -e1 ;
       
       long a1 = System.nanoTime();
       multiAwardedPerson(loadData(addr));
       long a2 = System.nanoTime();
       executionTime[4] = a2 - a1 ;
       
       long f1 = System.nanoTime();
       multiAwardedFilm(loadData(addr));
       long f2 = System.nanoTime();
       executionTime[5] = f2 -f1 ;
       
       long yp = System.nanoTime();
       youngWinnersParallel(loadData(addr));
       long yp1 = System.nanoTime();
       executionTime[7] = yp1 -yp;
       
       long ep = System.nanoTime();
       extreamWinnersParallel(loadData(addr));
       long ep1 = System.nanoTime();
       executionTime[8] = ep1 - ep ;
       
       long pp = System.nanoTime();
       multiAwardedPersonParallel(loadData(addr));
       long pp1 = System.nanoTime();
       executionTime[9] = pp1 - pp ;
       
       long fp = System.nanoTime();
       multiAwardedFilmParallel(loadData(addr));
       long fp1 = System.nanoTime();
       executionTime[10] = fp1 - fp ;
       
    return executionTime;
    }
   
   
    
   public static void main(String[] args) throws IOException {
       
        String[] addr = {"C:\\Users\\HP x360\\Desktop\\oscar_age_male.csv" ,"C:\\Users\\HP x360\\Desktop\\oscar_age_female.csv" };
       
        try{
           System.out.println("-------------All the Winners------------------------");
           loadData(addr).forEach(s ->  System.out.println(s.getWinnerName()));
           
           System.out.println("---------------Young Winners----------------------");
           youngWinners(loadData(addr)).forEach(s -> System.out.println(s.getWinnerName()+" "+ s.getWinnerAge()));
           
           System.out.println("-------------Extream Winners ------------------------");
           extreamWinners(loadData(addr)).forEach(s -> System.out.println(s.getWinnerName()+" "+ s.getWinnerAge()));
           
           System.out.println("---------------Multi Awarded Persons----------------------");
           multiAwardedPerson(loadData(addr)).forEach(s -> System.out.println(s));
           
           System.out.println("--------------Multi Awarded Films-----------------------");
           multiAwardedFilm(loadData(addr)).forEach(s -> System.out.println(s ));
           
           System.out.println("---------------measure() method----------------------");
            Function<Stream<Winner>,Stream<String>> func= null;
           
            func = (Stream<Winner> winners) -> {
                return winners.map(m -> m.getFilmTitle());
            };
           
           //System.out.println(measure( multiAwardedFilm(loadData(addr)) , loadData(addr)));
           
           
           
           System.out.println("---------------Comparison of methods----------------------");
              for(long i : comparison(loadData(addr))){
          
              System.out.println("\t" +i );
          } 

          
        }catch(NullPointerException e) {}
    }

   
}
