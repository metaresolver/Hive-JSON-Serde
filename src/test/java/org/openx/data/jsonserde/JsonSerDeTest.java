/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/

package org.openx.data.jsonserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author rcongiu
 */
public class JsonSerDeTest {
    
    public JsonSerDeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        initialize();
    }
    
    @After
    public void tearDown() {
    }

    static JsonSerDe instance;
 
    static public void initialize() throws Exception {
        System.out.println("initialize");
        instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string");
        
        instance.initialize(conf, tbl);
    }

    /**
     * Test of deserialize method, of class JsonSerDe.
     */
    @Test
    public void testDeserialize() throws Exception {
        System.out.println("deserialize");
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",\"orange\"],\"two\":19.5,\"four\":\"poop\"}");
        JsonNode result = (JsonNode) instance.deserialize(w);
        assertEquals(result.get("four").getTextValue(), "poop");

        assertTrue(result.get("three").isArray());
        
        assertTrue(result.get("three").get(0).isTextual());
        assertEquals(result.get("three").get(0).getTextValue(), "red");
    }

 //   {"one":true,"three":["red","yellow",["blue","azure","cobalt","teal"],"orange"],"two":19.5,"four":"poop"}

    @Test
    public void testDeserialize2() throws Exception {
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",[\"blue\",\"azure\",\"cobalt\",\"teal\"],\"orange\"],\"two\":19.5,\"four\":\"poop\"}");
        JsonNode result = (JsonNode) instance.deserialize(w);
        assertEquals(result.get("four").getTextValue(), "poop");

        assertTrue(result.get("three").isArray());

        assertTrue(result.get("three").get(0).isTextual());
        assertEquals(result.get("three").get(0).getTextValue(), "red");
    }

    /**
     * Test of getSerializedClass method, of class JsonSerDe.
     */
    @Test
    public void testGetSerializedClass() {
        System.out.println("getSerializedClass");
        Class expResult = Text.class;
        Class result = instance.getSerializedClass();
        assertEquals(expResult, result);
       
    }

    /**
     * Test of serialize method, of class JsonSerDe.
     */
/*    @Test
    public void testSerialize() throws Exception {
        System.out.println("serialize");
        Object o = null;
        ObjectInspector oi = null;
        JsonSerDe instance = new JsonSerDe();
        Writable expResult = null;
        Writable result = instance.serialize(o, oi);
        assertEquals(expResult, result);
    }
     *  
     */
    
    
   // @Test
    public void testSerialize() throws Exception {
        System.out.println("serialize");
        ArrayList row = new ArrayList(5);
        
        List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
        List<String> fieldNames = new LinkedList<String>();
        
        row.add("HELLO");
        fieldNames.add("atext");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        row.add(10);
        fieldNames.add("anumber");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        List<String> array = new LinkedList<String>();
        array.add("String1");
        array.add("String2");
        
        row.add(array);
        fieldNames.add("alist");
        lOi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));
        
        Map<String,String> m = new HashMap<String,String>();
        m.put("k1","v1");
        m.put("k2","v2");
        
        row.add(m);
        fieldNames.add("amap");
        lOi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));
        
        
        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);
        
        Object result = instance.serialize(row, soi);

        JsonNode res = new ObjectMapper().readTree(result.toString());
        assertEquals(res.get("atext").getTextValue(), row.get(0));
        
        assertEquals(res.get("anumber").getNumberValue(), row.get(1));
        
        // after serialization the internal contents of JSONObject are destroyed (overwritten by their string representation
       // (for map and arrays) 
      
       
        System.out.println("Serialized to " + result.toString());
        
    }
    
 }
