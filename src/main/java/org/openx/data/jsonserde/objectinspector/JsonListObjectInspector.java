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
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rcongiu
 */
public class JsonListObjectInspector extends StandardListObjectInspector {

    JsonListObjectInspector(ObjectInspector listElementObjectInspector) {
        super(listElementObjectInspector);
    }

	@Override
	public List<?> getList(Object data) {
		if (data == null) {
			return null;
		}
		List<JsonNode> list = new ArrayList<JsonNode>();
		for (JsonNode node : (ArrayNode) data) {
			list.add(node);
		}
		return list;
	}

	@Override
	public Object getListElement(Object data, int index) {
		if (data == null) {
			return null;
		}
		return ((ArrayNode) data).get(index);
	}

	@Override
	public int getListLength(Object data) {
		if (data == null) {
			return -1;
		}
		return ((ArrayNode) data).size();
	}

}
