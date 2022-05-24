package simpledb;

import java.util.*;
import java.io.*;

/*
 * I am implementing a simple cache here
 */
public class Cache {
    int capacity;
    HashMap<PageId, Page> hashMap;
    ArrayList<PageId> queue;

    public Cache(int capacity) {
        this.capacity = capacity;
        this.hashMap = new HashMap<PageId, Page> ();
        this.queue = new ArrayList<PageId> ();
    }

    public Page get(PageId key) {
        // I don't update the queue here
        return this.hashMap.get(key);
    }

    public void put(PageId key, Page value) {
        if(!this.hashMap.containsKey(key)) {
            if(this.hashMap.size() >= this.capacity) {
                this.evict();
            }
            this.queue.add(key);
            this.hashMap.put(key, value);
        } else {
            // this is just updating the value in the key;
            this.hashMap.put(key, value);
        }
    }

    public boolean containsKey(PageId key) {
        return this.hashMap.containsKey(key);
    }

    public int size() {
        return this.hashMap.size();
    }

    public PageId getEvictingKey() {
        return this.queue.get(0);
    }
    public void evict() {
        PageId key = this.queue.get(0);
        this.queue.remove(0);
        this.hashMap.remove(key);
    }

    public void remove(PageId key) {
        this.hashMap.remove(key);
        for(int i = 0; i < this.queue.size(); i++) {
            if(this.queue.get(i) == key) {
                this.queue.remove(i);
            }
        }
    }
}
