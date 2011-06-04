package com.liveprofile.hashring;

public class HashRingException extends Exception {
    public HashRingException(String msg) {
        super(msg);
    }
    
    public HashRingException(String msg, Throwable cause) {
        super(msg, cause);
    }
    
    public HashRingException(Throwable cause) {
        super(cause);
    }
}