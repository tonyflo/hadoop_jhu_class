/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hw4;

import java.util.Iterator;
import java.util.Vector;

/**
 *
 * @author hdadmin
 */
public class RatingCollection implements Iterable<Rating>{
    
    private Vector<Rating> ratings;
    
    public RatingCollection() {
        ratings = new Vector<Rating>();
    }
    
    public void add(Rating rating) {
        ratings.add(rating);
    }
    
    @Override
    public Iterator<Rating> iterator() {
        return ratings.iterator();
    }
    
}
