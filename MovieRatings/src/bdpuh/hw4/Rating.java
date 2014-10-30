/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw4;

/**
 *
 * @author hdadmin
 */
public class Rating {
    
    private int uid;
    private int rating;
    
    public Rating()
    {
        
    }
    
    public Rating(int uid, int rating) {
        this.uid = uid;
        this.rating = rating;
    }
    
    public int getUid() {
        return this.uid;
    }
    
    public int getRating() {
        return this.rating;
    }
    
    public void setUid(int uid) {
        this.uid = uid;
    }
    
    public void setRating(int rating) {
        this.rating = rating;
    }
    
}
