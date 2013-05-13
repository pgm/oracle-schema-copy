package com.github;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 3/20/13
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class MultiMap<K, V> {
    Map<K, Set<V>> map = new HashMap();

    public void add(K key, V value) {
        Set<V> set = map.get(key);
        if (set == null) {
            set = new HashSet<V>();
            map.put(key, set);
        }
        set.add(value);
    }

    public Collection<V> get(K key) {
        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            return Collections.EMPTY_SET;
        }
    }

    public void remove(K key, V value) {
        if (map.containsKey(key)) {
            Set<V> set = map.get(key);
            set.remove(value);

            if (set.size() == 0) {
                map.remove(key);
            }
        }
    }

    public boolean contains(K key, V value) {
        return get(key).contains(value);
    }

    public int size() {
        return map.size();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public String toString() {
        return map.toString();
    }
}
