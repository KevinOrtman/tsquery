/*
 * Copyright 2011 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * added jasonp support
 * removed gnuplot logic
 * added TSDB dependency
 *
 * @author kevin ortman
 *
 */

package net.tsquery;

import net.tsquery.data.hbase.HBaseConnection;
import net.opentsdb.core.TSDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Properties;

public class TsdbServlet extends HttpServlet {

    protected static Logger logger = Logger.getLogger("com.facebook.tsdb.services");

    private static final long serialVersionUID = 1L;
    private static final String TSDB_TABLE = "tsdb";
    private static final String TSDB_UID_TABLE = "tsdb-uid";
    private static final short FLUSH_INTERVAL = 1000;

    private static boolean s_returnStackTrace = false;

    public static final String PROPERTIES_FILE = "/etc/tsquery/tsquery.properties";
    public static final String LOG4J_PROPERTIES_FILE = "/etc/tsquery/log4j.properties";

    protected static TSDB _tsdb;

    private static void loadConfiguration() {
        Properties tsdbConf = new Properties();
        org.hbase.async.HBaseClient client = null;

        try {
            PropertyConfigurator.configure(LOG4J_PROPERTIES_FILE);
            tsdbConf.load(new FileInputStream(PROPERTIES_FILE));
            HBaseConnection.configure(tsdbConf);

            String quorum = tsdbConf.getProperty("hbase.zookeeper.quorum","localhost");

            String returnStackTrace = tsdbConf.getProperty("hbase.zookeeper.quorum","localhost");
            if(returnStackTrace != null && returnStackTrace.equalsIgnoreCase("true"))
                s_returnStackTrace = true;

            client = new org.hbase.async.HBaseClient(quorum);

            client.ensureTableExists(TSDB_TABLE).joinUninterruptibly();
            client.ensureTableExists(TSDB_UID_TABLE).joinUninterruptibly();

            client.setFlushInterval(FLUSH_INTERVAL);
            _tsdb = new TSDB(client, TSDB_TABLE, TSDB_UID_TABLE);
        } catch (FileNotFoundException e) {
            System.err.println("Cannot find "  + PROPERTIES_FILE);
        } catch (IOException e) {
            System.err.println("Cannot find "  + PROPERTIES_FILE);
        } catch (Throwable e) {
            try {
                if(client != null)
                    client.shutdown().joinUninterruptibly();
            } catch (Exception e2) {
                logger.error("Failed to shutdown HBase client", e2);
            }
        }
    }

    static {
        loadConfiguration();
    }

    protected long getRequiredTimeStamp(JSONObject obj, String key)   {
        if(!obj.containsKey(key))
            throw new IllegalArgumentException("Required timestamp '" + key + "' missing.  Please add it to the query string.");

        long val;

        try {
            Object o = obj.get(key);

            if(o instanceof Long) {
                val = (Long)o;
            }
            else {
                val = ISO8601DateParser.parse(o.toString()).getTime();
            }

        }
        catch (Exception e) {
            throw new IllegalArgumentException("Required timestamp '" + key + "' must be either an epoch timestamp or an ISO 8601 formatted date.");
        }

        if(val > 1000000000000L)
            val = val / 1000L;

        return val;
    }

    @SuppressWarnings("unchecked")
    protected String getErrorResponse(Throwable e) {
        JSONObject errObj = new JSONObject();
        errObj.put("error", e.getMessage());

        if(s_returnStackTrace) {
            StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));
            errObj.put("stacktrace", stackTrace.toString());
        }

        return errObj.toJSONString();
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
    }

    protected void doSendResponse(HttpServletRequest request, PrintWriter out, String jsonString) {
        // jsonp support
        String jsonCallback = request.getParameter("jsoncallback");
        if((jsonCallback != null) && (!jsonCallback.isEmpty())) {
            out.print(jsonCallback + "('");
            out.print(jsonString);
            out.println("');");
        }
        else {
            out.println(jsonString);
        }
    }

}
