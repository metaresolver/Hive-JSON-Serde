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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.io.Text;

import org.codehaus.jackson.JsonNode;

/**
 * This String ObjectInspector was built to tolerate non-string values
 * and treat them as strings.
 * @author rcongiu
 */
public class JsonStringJavaObjectInspector
	extends AbstractPrimitiveJavaObjectInspector
	implements SettableStringObjectInspector {

	JsonStringJavaObjectInspector() {
		super(PrimitiveObjectInspectorUtils.stringTypeEntry);
	}

	@Override
	public Text getPrimitiveWritableObject(Object o) {
		if (o == null)
			return null;
		else
			return new Text(getPrimitiveJavaObject(o));
	}

	@Override
	public String getPrimitiveJavaObject(Object o) {
		JsonNode node = (JsonNode) o;
		if (node == null)
			return null;
		else if (node.isValueNode())
			return node.getValueAsText(); // outputs unquoted Strings
		else
			return node.toString();
	}

	@Override
	public Object create(Text value) {
		return value == null ? null : value.toString();
	}

	@Override
	public Object set(Object o, Text value) {
		return value == null ? null : value.toString();
	}

	@Override
	public Object create(String value) {
		return value;
	}

	@Override
	public Object set(Object o, String value) {
		return value;
	}

}
