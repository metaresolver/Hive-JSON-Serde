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
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Map;

/**
 *
 * @author rcongiu
 */
public class JsonMapObjectInspector extends StandardMapObjectInspector {

    public JsonMapObjectInspector(ObjectInspector mapKeyObjectInspector,
								  ObjectInspector mapValueObjectInspector) {
        super(mapKeyObjectInspector, mapValueObjectInspector);
    }


	@Override
	public Map<?, ?> getMap(Object data) {
		if (data == null) {
			return null;
		}
		final ObjectNode node = (ObjectNode) data;

		return new ObjectNodeMapAdapter(node);
	}

	@Override
	public int getMapSize(Object data) {
		if (data == null) {
			return -1;
		}
		return ((ObjectNode) data).size();
	}

	@Override
	public Object getMapValueElement(Object data, Object key) {
		if (data == null) {
			return -1;
		}
		return ((ObjectNode) data).get(key.toString());
	}
}
