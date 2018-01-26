package org.cx.topic2gp.rmi;

import java.io.FileNotFoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RPC
 * Created by 冯曦 on 2017/12/19.
 */
public interface ISinkFile extends Remote{

    public void sinkFile(String json) throws RemoteException, FileNotFoundException;

    public void sinkFile(String json, int threadCount) throws RemoteException, FileNotFoundException;

    public String hello(String name) throws RemoteException;
}
