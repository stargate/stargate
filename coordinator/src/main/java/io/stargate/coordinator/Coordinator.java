package io.stargate.coordinator;

import io.stargate.db.Persistence;
import io.stargate.filterchain.FilterChain;

public class Coordinator {
    private Persistence persistence;
    private FilterChain readFilterChain;
    private FilterChain writeFilterChain;

    public Persistence getPersistence() {
        return persistence;
    }

    public void setPersistence(Persistence persistence) {
        this.persistence = persistence;
    }

    public FilterChain getReadFilterChain() {
        return readFilterChain;
    }

    public void setReadFilterChain(FilterChain readFilterChain) {
        this.readFilterChain = readFilterChain;
    }

    public FilterChain getWriteFilterChain() {
        return writeFilterChain;
    }

    public void setWriteFilterChain(FilterChain writeFilterChain) {
        this.writeFilterChain = writeFilterChain;
    }

    public String read(String query) {
        System.out.println("Performing read in Coordinator");

        getReadFilterChain().doFilter();

        return "";
    }

    public String write() {
        System.out.println("Performing write in Coordinator");

        getWriteFilterChain().doFilter();

        return "";
    }
}
