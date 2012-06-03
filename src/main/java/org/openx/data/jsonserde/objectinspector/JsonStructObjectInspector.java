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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.codehaus.jackson.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * This Object Inspector is used to look into an ObjectNode.
 * We couldn't use StandardStructObjectInspector since that expects
 * something that can be cast to an Array<Object>.
 * @author rcongiu
 */
public class JsonStructObjectInspector extends StandardStructObjectInspector {

    public JsonStructObjectInspector(List<String> structFieldNames,
									 List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        if (data == null) {
            return null;
        }
        ObjectNode obj = (ObjectNode) data;
        MyField f = (MyField) fieldRef;
        return obj.get(f.getFieldName());
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object o) {
        ObjectNode obj = (ObjectNode) o;
        List<Object> values = new ArrayList<Object>();
        for (MyField field : fields) {
            values.add(obj.get(field.getFieldName()));
        }
        return values;
    }
}
