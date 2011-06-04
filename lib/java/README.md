# libhashring for java

## Building

    gradle jar
    
This will create a file named **libhashring-VERSION.jar** in the *build/libs* directory. When using this jar in your application make sure that the library is installed and that *jna.library.path* system property points to the proper directory. i.e */usr/local/lib*.

You will also need *jna.jar* on your classpath. You can download it from  <http://java.net/projects/jna/downloads/download/3.2.7/jna.jar>.