
package winneropsdb;


public class WinnerClass implements Winner {
   // private int index;
    private int year;
    private int winnerAge;
    private String winnerName;
    private String movieTitle;
    public WinnerClass(int year, int winnerAge, String winnerName,String movieTitle){
       // this.index = index;
        this.year = year;
        this.winnerAge = winnerAge;
        this.winnerName = winnerName;
        this.movieTitle = movieTitle;
    
    }
   /* public int getIndex(){
        return this.index;
    }*/
    public int getYear(){
        return this.year;
    }
    public int getWinnerAge(){
        return this.winnerAge;
    }
    public String getWinnerName(){
        return this.winnerName;
    }
    public String getFilmTitle(){
        return this.movieTitle;
    }

}    
