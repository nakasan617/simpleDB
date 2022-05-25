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
        PageId pid;
        for(int i = 0; i < this.queue.size(); i++) {
            pid = this.queue.get(i);
            if(this.hashMap.get(pid).isDirty() == null) {
                return pid;
            }
        }
        return null;
    }
    public void evict() {
        PageId pid = null;
        int i = 0;
        boolean found = false;
        for(; i < this.queue.size(); i++) {
            pid = this.queue.get(i);
            if(this.hashMap.get(pid).isDirty() == null) {
                found = true;
                break;
            }
        }
        if(found) {
            this.queue.remove(i);
            this.hashMap.remove(pid);
        }
    }

    public void remove(PageId key) {
        this.hashMap.remove(key);
        for(int i = 0; i < this.queue.size(); i++) {
            if(this.queue.get(i) == key) {
                this.queue.remove(i);
            }
        }
    }

    public void removePids(Set<PageId> pids) {
        // iterate through the queue and remove
        PageId pid;
        int size = this.queue.size();
        for(int i = size - 1; i >= 0; i--) {
            pid = this.queue.get(i);
            if(pids.contains(pid)) {
                this.queue.remove(i);
            }
        }

        // iterate through the hashMap and remove
        Iterator<PageId> it = pids.iterator();
        while(it.hasNext()) {
            pid = it.next();
            this.hashMap.remove(pid);
        }

    }
}
