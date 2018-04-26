package com.sc.eni.hbasethrift;

import org.apache.hadoop.hbase.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class HbaseThriftClient {

    private static final byte[] TABLE = Bytes.toBytes("bulkload");

    public static void main (String[] arg) throws Exception{

        Configuration conf = HBaseConfiguration.create();

        TTransport transport = new TSocket("localhost", 9090, 20000);
        TProtocol protocol = new TBinaryProtocol(transport, true, true);
        Hbase.Client client = new Hbase.Client(protocol);
        transport.open();

        //Scan table

        TScan scan = new TScan();
        int scannerId = client.scannerOpenWithScan(
                ByteBuffer.wrap(TABLE),
                scan,
                null);
        // Map(Bytes.toBytes(1),Bytes.toBytes(2)) );


        for (TRowResult result : client.scannerGet(scannerId)) {
            System.out.println("No. columns: " + result.getColumnsSize());

            for (Map.Entry<ByteBuffer, TCell> column :
                    result.getColumns().entrySet()) {

                System.out.println("Column name: " + Bytes.toString(
                        column.getKey().array()));
                System.out.println("Column value: " + Bytes.toString(
                        column.getValue().getValue()));
            }
        }
        client.scannerClose(scannerId);


        System.out.println("Done.");
        transport.close(); // co ThriftExample-7-Close Close the connection after everything is done.

    }



}
