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
package org.openx.data.jsonserde.objectinspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;
import static org.junit.Assert.*;

/**
 *
 * @author joey
 */
public class JsonStructObjectInspectorTest {

    public JsonStructObjectInspectorTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of getStructFieldData method, of class JsonStructObjectInspector.
     */
    @Test
    public void testGetStructFieldData() throws JSONException {
        System.out.println("getStructFieldData");
        String json = "{\"time\":\"2011-08-17 11:16:21\",\"response\":{\"response_code\":200,\"content_length\":13795,\"redirected_to\":null,\"layout\":null},\"request\":{\"params\":{\"ver\":1337,\"hdnv\":\"true\",\"action\":\"geo\",\"do_not_redirect\":\"1\",\"controller\":\"cities\",\"ref\":\"fld\",\"was_geo\":\"true\",\"gwo_id\":\"1897392579\"},\"host\":\"www.site.com\",\"remote_addr\":\"192.168.0.100\",\"method\":\"GET\",\"request_uri\":\"/cities/geo?do_not_redirect=1&gwo_id=1897392579&hdnv=true&ref=fld&ver=1337&was_geo=true\",\"referrer\":null},\"analytics_id\":\"bae898bad7ff4a929859bfbdb51d1a0c\",\"cookies\":{\"_session\":\"8\",\"ref_code\":\"f\",\"preferred_city\":\"1\",\"site_fb_connected\":\"true\"}}";
        List<String> fieldNames = Arrays.asList(new String[]{"time",
                    "analytics_id", "response_response_code", "request_params_action",
                    "request_host", "cookies_site_fb_connected"});
        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fieldInspectors.add(new JsonStringJavaObjectInspector());
        }
        Object data = new JSONObject(json);
        JsonStructObjectInspector instance = new JsonStructObjectInspector(fieldNames, fieldInspectors);
        Object expResult = null;
        Object result = null;

        expResult = "2011-08-17 11:16:21";
        result = instance.getStructFieldData(data, instance.getStructFieldRef("time")).toString();
        assertEquals(expResult, result);

        expResult = "bae898bad7ff4a929859bfbdb51d1a0c";
        result = instance.getStructFieldData(data, instance.getStructFieldRef("analytics_id")).toString();
        assertEquals(expResult, result);

        expResult = "200";
        result = instance.getStructFieldData(data, instance.getStructFieldRef("response_response_code")).toString();
        assertEquals(expResult, result);

        expResult = "geo";
        result = instance.getStructFieldData(data, instance.getStructFieldRef("request_params_action")).toString();
        assertEquals(expResult, result);

        expResult = "www.site.com";
        result = instance.getStructFieldData(data, instance.getStructFieldRef("request_host")).toString();
        assertEquals(expResult, result);

        expResult = "true";
        result = instance.getStructFieldData(data, instance.getStructFieldRef("cookies_site_fb_connected")).toString();
        assertEquals(expResult, result);
    }

    /**
     * Test complete record key
     */
    @Test
    public void testCompleteRecordKey() throws JSONException {
        System.out.println("completeRecordKey");
        String json = "{\"time\":\"2011-08-17 11:16:21\",\"response\":{\"response_code\":200,\"content_length\":13795,\"redirected_to\":null,\"layout\":null},\"request\":{\"params\":{\"ver\":1337,\"hdnv\":\"true\",\"action\":\"geo\",\"do_not_redirect\":\"1\",\"controller\":\"cities\",\"ref\":\"fld\",\"was_geo\":\"true\",\"gwo_id\":\"1897392579\"},\"host\":\"www.site.com\",\"remote_addr\":\"192.168.0.100\",\"method\":\"GET\",\"request_uri\":\"/cities/geo?do_not_redirect=1&gwo_id=1897392579&hdnv=true&ref=fld&ver=1337&was_geo=true\",\"referrer\":null},\"analytics_id\":\"bae898bad7ff4a929859bfbdb51d1a0c\",\"cookies\":{\"_session\":\"8\",\"ref_code\":\"f\",\"preferred_city\":\"1\",\"site_fb_connected\":\"true\"}}";
        List<String> fieldNames = Arrays.asList(new String[]{"time",
                    "analytics_id", "response_response_code", "request_params_action",
                    "request_host", "cookies_site_fb_connected", "json"});
        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fieldInspectors.add(new JsonStringJavaObjectInspector());
        }
        Object data = new JSONObject(json);
        JsonStructObjectInspector instance = new JsonStructObjectInspector(fieldNames, fieldInspectors);
        instance.setCompleteRecordKey("json");
        Object expResult = null;
        Object result = null;

        expResult = new JSONObject(json);
        result = new JSONObject(instance.getStructFieldData(data, instance.getStructFieldRef("json")).toString());
        assertEquals(expResult, result);
    }

    /**
     * Test of getStructFieldsDataAsList method, of class JsonStructObjectInspector.
     */
    @Ignore
    @Test
    public void testGetStructFieldsDataAsList() {
        System.out.println("getStructFieldsDataAsList");
        Object o = null;
        JsonStructObjectInspector instance = null;
        List expResult = null;
        List result = instance.getStructFieldsDataAsList(o);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}
