/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
//import com.yahoo.ycsb.StringByteIterator;

//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.Protocol;

import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import java.io.*;
import java.net.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class MydbClient extends DB {

  //private Jedis jedis;

  public static final String HOST_PROPERTY = "mydb.host";
  public static final String PORT_PROPERTY = "mydb.port";
  public static final String PASSWORD_PROPERTY = "redis.password";

  public static final String INDEX_KEY = "_indices";
  
  private Socket socket;
  private int connectionTimeout = 2000;
  private int soTimeout = 2000;
  private int count=0;
  private PrintWriter writer;
  
  public void init() throws DBException {
    System.out.println("----init");
    Properties props = getProperties();
    int port;
    try{   
      writer= new PrintWriter("myworkload", "UTF-8");
    }catch (IOException ex) {
      //broken = true;
      //throw new JedisConnectionException(ex);
      System.out.println("*****IO exp");
    }    
    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      //port = Protocol.DEFAULT_PORT;
      port=1111;
    }
    String host = props.getProperty(HOST_PROPERTY);
    try {
      socket = new Socket();
      socket = new Socket();
      socket.setReuseAddress(true);
      socket.setKeepAlive(true); // Will monitor the TCP connection is
      socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
      socket.setSoLinger(true, 0); // Control calls close () method,
      socket.connect(new InetSocketAddress(host, port), connectionTimeout);
      socket.setSoTimeout(soTimeout);
    }catch (IOException ex) {
      //broken = true;
      //throw new JedisConnectionException(ex);
      System.out.println("*****IO exp"+ex.toString());
    }

    //jedis = new Jedis(host, port);
    //jedis.connect();

    // String password = props.getProperty(PASSWORD_PROPERTY);
    // if (password != null) {
      // jedis.auth(password);
    // }
  }

  public void cleanup() throws DBException {
    System.out.println("----clean up");
    try{
      socket.close();
    }catch(IOException ex){
      System.out.println("*****close error");
    }
    writer.close();
    //jedis.disconnect();
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    //System.out.println("rrrr"+key);
    String command="r";
    //System.out.println(key);
  
    writer.println(key);   
    count++;
    if(count%1000000==0){
      System.out.println(count);
    }
    // DataOutputStream outToServer;
    // BufferedReader inFromServer;
    // try{
      // outToServer = new DataOutputStream(socket.getOutputStream());
      // inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      // //send command
      // outToServer.writeBytes(command);
      // //send key length
      // outToServer.writeInt(key.length());
      // //send key
      // outToServer.writeBytes(key);
      // outToServer.flush();
      // //wait for the response
      // String reply=inFromServer.readLine();
      // //System.out.println("reply from the server:"+reply);
    // }catch(IOException ex){
      // System.out.println("***** new DataOutputStream error, in read:"+ex.toString());
    // }

    return Status.OK;
    // if (fields == null) {
      // StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    // } else {
      // String[] fieldArray =
          // (String[]) fields.toArray(new String[fields.size()]);
      // List<String> values = jedis.hmget(key, fieldArray);

      // Iterator<String> fieldIterator = fields.iterator();
      // Iterator<String> valueIterator = values.iterator();

      // while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        // result.put(fieldIterator.next(),
            // new StringByteIterator(valueIterator.next()));
      // }
      // assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    // }
    // return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    String command="i";
    DataOutputStream outToServer;
    BufferedReader inFromServer;
    System.out.println("sfd");
    // try{
      // // outToServer = new DataOutputStream(socket.getOutputStream());
      // // inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      // // //send command
      // // outToServer.writeBytes(command);
      // // //send key length
      // // outToServer.writeInt(key.length());
      // // //send key
      // // outToServer.writeBytes(key);
      // // outToServer.flush();
      // // //wait for the response
      // // String reply=inFromServer.readLine();
      // // System.out.println("reply from the server:"+reply);
    // }catch(IOException ex){
      // System.out.println("***** new DataOutputStream error,in insert:"+ex.toString());
    // }

    
    //System.out.println("hhhhh"+key);
    return Status.OK;
    // if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        // .equals("OK")) {
      // jedis.zadd(INDEX_KEY, hash(key), key);
      // return Status.OK;
    // }
    // return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String command="d";
    DataOutputStream outToServer;
    BufferedReader inFromServer;
    try{
      outToServer = new DataOutputStream(socket.getOutputStream());
      inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //send command
      outToServer.writeBytes(command);
      //send key length
      outToServer.writeInt(key.length());
      //send key
      outToServer.writeBytes(key);
      outToServer.flush();
      //wait for the response
      String reply=inFromServer.readLine();
      //System.out.println("reply from the server:"+reply);
    }catch(IOException ex){
      System.out.println("***** new DataOutputStream error,in insert:"+ex.toString());
    }
    return Status.OK;
    //return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
    //    : Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    //System.out.println("uuuuu"+key);
    String command="u";
    DataOutputStream outToServer;
    BufferedReader inFromServer;
    try{
      outToServer = new DataOutputStream(socket.getOutputStream());
      inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //send command
      outToServer.writeBytes(command);
      //send key length
      outToServer.writeInt(key.length());
      //send key
      outToServer.writeBytes(key);
      outToServer.flush();
      //wait for the response
      String reply=inFromServer.readLine();
      //System.out.println("reply from the server:"+reply);
    }catch(IOException ex){
      System.out.println("***** new DataOutputStream error, in read");
    }

    return Status.OK;
    // return jedis.hmset(key, StringByteIterator.getStringMap(values))
        // .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String key, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String command="s";
    DataOutputStream outToServer;
    BufferedReader inFromServer;
    try{
      outToServer = new DataOutputStream(socket.getOutputStream());
      inFromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      //send command
      outToServer.writeBytes(command);
      //send key length
      outToServer.writeInt(key.length());
      //send key
      outToServer.writeBytes(key);
      outToServer.flush();
      //wait for the response
      String reply=inFromServer.readLine();
      //System.out.println("reply from the server:"+reply);
    }catch(IOException ex){
      System.out.println("***** new DataOutputStream error,in insert:"+ex.toString());
    }
    // Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        // Double.POSITIVE_INFINITY, 0, recordcount);

    // HashMap<String, ByteIterator> values;
    // for (String key : keys) {
      // values = new HashMap<String, ByteIterator>();
      // read(table, key, fields, values);
      // result.add(values);
    // }

    return Status.OK;
  }

}
