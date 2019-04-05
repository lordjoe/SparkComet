package com.lordjoe.ssh;

import com.jcabi.ssh.Shell;
import com.jcabi.ssh.Ssh;
import com.lordjoe.utilities.FileUtilities;

import java.io.IOException;

public class RemoteExecutor {

    public static final String LIST_HPC = "login.hpc.private.list.lu";
    public static final String KEY_FILE = "~/.ssh/lewis.ppk";
    public static final String USER = "lewis";
    public static final String PASS_PHRASE_FILE = "~/.ssh/lewis.phrase";

    public static Shell login()
    {
        try {
            String privatekey = FileUtilities.readInFile(KEY_FILE);
            String passphrase = FileUtilities.readInFile(PASS_PHRASE_FILE);

            Shell shell = new Ssh(LIST_HPC, 22, "username", privatekey,passphrase);
            String stdout = new Shell.Plain(shell).exec("echo 'Hello, world!'");
            System.out.println(stdout);
            return shell;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        Shell shell = login();

    }
}
