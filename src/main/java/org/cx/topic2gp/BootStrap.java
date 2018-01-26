package org.cx.topic2gp;

import org.cx.topic2gp.rmi.ISinkFile;
import org.cx.topic2gp.rmi.SinkFile;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 * 程序入口
 * Created by 冯曦 on 2017/12/20.
 */
public class BootStrap {
    public static void main(String[] args) throws RemoteException, AlreadyBoundException, MalformedURLException {
        ISinkFile iSinkFile = new SinkFile();
        LocateRegistry.createRegistry(9998);
        Naming.bind("rmi://localhost:9998/iSinkFile", iSinkFile);
        System.out.println("Server started.Port:" + 9998);
    }
}
